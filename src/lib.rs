//! Query routing. More docs to come...

#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::default_trait_access)]

use id_derive::Id;
use rand::distributions::Uniform;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaChaRng;
use rand_distr::Normal;
use serde::{Deserialize, Serialize};
use std::cmp::{Ord, Ordering, PartialOrd, Reverse};
use std::collections::BinaryHeap;
use std::rc::Rc;
use std::time::Duration;

mod query;
pub use query::{Query, QueryStatus};

mod process;
pub use process::Runnable;

mod query_generator;
pub use query_generator::{QueryGenerator, QueryGeneratorStage};

mod queue;
pub use queue::{PopResult, PushResult, Queue};

mod broker;
pub use broker::{Broker, BrokerStage};

mod node;
pub use node::{Node, NodeStage};

pub mod replica_selector;
pub mod routing_matrix;
use routing_matrix::RoutingMatrixBuilder;
mod shard_selector;

pub mod config;
pub use config::Config;
use config::TimeUnit;

mod status;

mod im_status;
pub use im_status::Status;

/// Shard ID.
#[derive(Id, Debug, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize, Copy, Clone, Hash)]
pub struct ShardId(usize);

/// Node ID.
#[derive(Id, Debug, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize, Copy, Clone, Hash)]
pub struct NodeId(usize);

/// Query ID.
#[derive(Id, Debug, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize, Copy, Clone, Hash)]
pub struct QueryId(usize);

/// Query request ID.
#[derive(Id, Debug, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize, Copy, Clone, Hash)]
pub struct RequestId(usize);

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize, Copy, Clone)]
/// Replica is identified by a node and a shard.
pub struct ReplicaId {
    /// Node ID.
    pub node: NodeId,
    /// Shard ID.
    pub shard: ShardId,
}

/// Represents a process being part of the simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Process {
    /// See [`QueryGenerator`](struct.QueryGenerator.html).
    QueryGenerator(QueryGeneratorStage),
    /// See [`Broker`](struct.Broker.html).
    Broker(BrokerStage),
    /// See [`Node`](struct.Node.html).
    Node {
        /// Node ID.
        id: NodeId,
        /// Stage to enter.
        stage: NodeStage,
    },
}

/// An event is simply a process to run and a time to run it on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Event {
    time: Duration,
    process: Process,
}

impl Event {
    fn new(time: Duration, process: Process) -> Self {
        Event { time, process }
    }
}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.time.partial_cmp(&other.time)
    }
}

impl Ord for Event {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time.cmp(&other.time)
    }
}

/// Request to route a query to a certain node to process a certain shard.
#[derive(Debug, Eq, PartialEq)]
pub struct NodeRequest {
    id: NodeId,
    shard: ShardId,
}

struct NodeEntry<'a> {
    node: Node<'a>,
    entry_queue: Queue<'a, (Query, ShardId), Process>,
}

/// This is what a process returns when yields to the simulation.
/// It is intentionally not very generic for the sake of testing the entire
/// thing step by step. It is entirely possible that it will stay this way
/// and the generic case will be handled by, say, procedural macro attributes
/// on the simulation structure members.
pub enum Effect<'a> {
    /// Simply schedules the event. A process that returns this effect will not be
    /// continued until someone else explicitly schedules it.
    Schedule(Event),
    /// Same as `Schedule` but schedules many events at once.
    ScheduleMany(Vec<Event>),
    /// Attempts to insert a query to the incoming queue, and schedules a given process
    /// if succeeded; otherwise, process must wait for empty slot in the queue until it
    /// is scheduled.
    QueryQueuePut(Query, Process),
    /// Attempts to pop a query from an incoming queue. If succeeded, the query is passed
    /// to the callback and the resulting process is scheduled. Otherwise, the callback
    /// must wait until a query appears in the query to be popped.
    QueryQueueGet(queue::ProcessCallback<'a, Query, Process>),
    /// This effect does two things:
    /// 1. Schedules `query` to be sent to replica `nodes` after `timeout`.
    /// 2. Schedules the broker's `GetQuery` stage.
    /// **Note**: Eventually, when multiple brokers are supported, this will also need
    ///           to receive the broker's ID.
    Route {
        /// Time to wait before sending requests to nodes.
        timeout: Duration,
        /// Query that is being routed.
        query: Query,
        /// Description of nodes, see [`NodeRequest`](struct.NodeRequest.html).
        nodes: Vec<NodeRequest>,
    },
    /// Similar to `QueryQueueGet` but from a node entry queue.
    NodeQueryGet {
        /// ID of the node.
        node: NodeId,
        /// Callback to call on query.
        callback: queue::ProcessCallback<'a, (Query, ShardId), Process>,
    },
    /// Aggregate partial results of a query, coming from a shard node.
    Aggregate {
        /// Time to wait before scheduling aggregator.
        timeout: Duration,
        /// Queryt to aggregate.
        query: Query,
        /// Node where partial results are coming from.
        node: NodeId,
    },
}

macro_rules! push_query {
    ($s:expr, $queue:expr, $query:expr, $process:expr) => {
        if let PushResult::Pushed { resumed } = $queue.push($query, $process) {
            $s.schedule_now($process);
            if let Some(resumed) = resumed {
                $s.schedule_now(resumed);
            }
        }
    };
    ($s:expr, $queue:expr, $query:expr) => {
        if let PushResult::Pushed { resumed } = $queue.push_or_drop($query) {
            if let Some(resumed) = resumed {
                $s.schedule_now(resumed);
            }
            true
        } else {
            false
        }
    };
}

macro_rules! pop_query {
    ($s:expr, $queue:expr, $callback:expr) => {
        if let PopResult::Popped { process, resumed } = $queue.pop($callback) {
            $s.schedule_now(process);
            if let Some(resumed) = resumed {
                $s.schedule_now(resumed);
            }
        }
    };
}

/// This is the main structure and the entry point of the simulation experiment.
/// It is supposed to be the only entity that holds state and modifies it.
/// Other processes are pure functions that receive information and return an effect,
/// such as "push query to a queue", or "wait for X ms", or "request CPU".
pub struct QueryRoutingSimulation<'a> {
    /// Queue of the future events that will be processed in order of time.
    scheduled_events: BinaryHeap<Reverse<Event>>,

    incoming_queries: Queue<'a, Query, Process>,

    /// Nodes. Each contains a number of shard replicas.
    nodes: Vec<NodeEntry<'a>>,

    /// Current simulation time.
    time: Duration,

    query_generator: QueryGenerator<'a, Normal<f32>, Uniform<usize>, Box<dyn Fn(u64) -> Duration>>,
    broker: Broker<'a>,
    query_data: Rc<Vec<config::Query>>,
    time_unit: TimeUnit,
    history: Vec<Status>,
    next_steps: Vec<Status>,
}

fn duration_constructor(unit: TimeUnit) -> Box<dyn Fn(u64) -> Duration> {
    Box::new(match unit {
        TimeUnit::Micro => Duration::from_micros,
        TimeUnit::Milli => Duration::from_millis,
        TimeUnit::Nano => Duration::from_nanos,
        TimeUnit::Second => Duration::from_secs,
    })
}

impl<'a> QueryRoutingSimulation<'a> {
    /// Constructs a new routing simulation from a config and input queries.
    pub fn from_config(config: Config, queries: Vec<config::Query>) -> Self {
        let query_selection_dist = Uniform::from(0..queries.len());
        let mut seeder = config
            .seed
            .map(|seed| ChaChaRng::seed_from_u64(seed))
            .unwrap_or_else(|| ChaChaRng::from_entropy());
        let mut sim = Self {
            incoming_queries: Default::default(),
            scheduled_events: BinaryHeap::new(),
            nodes: vec![],
            time: Duration::from_secs(0),
            query_generator: QueryGenerator::new(
                (0..queries.len()).map(QueryId::from).collect(),
                ChaChaRng::seed_from_u64(seeder.next_u64()),
                Normal::new(f32::from(config.query_interval), 1.0).unwrap(),
                query_selection_dist,
                duration_constructor(config.time_unit),
            ),
            broker: Broker::new(
                shard_selector::ExhaustiveSelector::new(config.assignment.num_shards()),
                RoutingMatrixBuilder::new(config.assignment.weights_by_shard())
                    .rng(ChaChaRng::seed_from_u64(seeder.next_u64()))
                    .build(),
            ),
            query_data: Rc::from(queries),
            time_unit: config.time_unit,
            history: vec![Status::default()],
            next_steps: Vec::new(),
        };
        let nodes = sim.set_up_nodes(&config);
        for NodeEntry {
            node,
            entry_queue: _,
        } in &nodes
        {
            for _ in 0..config.cpus_per_node {
                sim.schedule_now(Process::Node {
                    id: node.id,
                    stage: NodeStage::GetQuery,
                });
            }
        }
        sim.schedule_now(Process::QueryGenerator(QueryGeneratorStage::Generate));
        sim.schedule_now(Process::Broker(BrokerStage::GetQuery));
        sim.nodes = nodes;
        sim
    }

    fn set_up_nodes(&self, config: &Config) -> Vec<NodeEntry<'a>> {
        (0..config.assignment.num_nodes())
            .map(|id| NodeEntry {
                node: Node {
                    id: NodeId(id),
                    query_data: Rc::clone(&self.query_data),
                    duration_from_u64: duration_constructor(self.time_unit),
                },
                entry_queue: Queue::new(),
            })
            .collect()
    }

    /// Schedule a process to run after `time` counting from the current simulation time.
    pub fn schedule(&mut self, time: Duration, process: Process) {
        self.scheduled_events
            .push(Reverse(Event::new(self.time + time, process)));
    }

    fn schedule_now(&mut self, process: Process) {
        self.schedule(Duration::from_secs(0), process)
    }

    fn status(&self) -> &Status {
        self.history.iter().last().unwrap()
    }

    /// Goes back in history. If no past steps exist, it returns an error.
    pub fn step_back(&mut self) -> Result<(), ()> {
        match self.history.pop() {
            Some(status) => {
                self.next_steps.push(status);
                Ok(())
            }
            None => Err(()),
        }
    }

    /// Advance the simulation
    pub fn advance(&mut self) {
        use Effect::*;
        if let Some(next) = self.next_steps.pop() {
            self.history.push(next);
            return;
        }
        if let Some(Reverse(Event { time, process })) = self.scheduled_events.pop() {
            if self.time < time {
                log::info!("Elapsed {:?}", time);
            }
            self.time = time;
            let effect = match process {
                Process::QueryGenerator(stage) => self.query_generator.run(stage),
                Process::Broker(stage) => {
                    if let &BrokerStage::Route(query) = &stage {
                        self.history.push(self.status().pick_up_query(query, time));
                    }
                    self.broker.run(stage)
                }
                Process::Node { id, stage } => self
                    .nodes
                    .get(usize::from(id))
                    .expect("Node ID out of bounds")
                    .node
                    .run(stage),
            };
            match effect {
                Schedule(event) => {
                    self.schedule(event.time, event.process);
                }
                ScheduleMany(events) => {
                    for event in events {
                        self.schedule(event.time, event.process);
                    }
                }
                QueryQueuePut(query, process) => {
                    self.history.push(self.status().enter_query(query, time));
                    push_query!(self, self.incoming_queries, query, process);
                }
                QueryQueueGet(callback) => {
                    pop_query!(self, self.incoming_queries, callback);
                }
                Route {
                    timeout,
                    query,
                    nodes,
                } => {
                    self.schedule(timeout, Process::Broker(BrokerStage::GetQuery));
                    self.history
                        .push(self.status().dispatch_query(query, time, nodes.len()));
                    for NodeRequest { id, shard } in nodes {
                        let queue = &mut self
                            .nodes
                            .get_mut(usize::from(id))
                            .expect("Node ID out of bounds")
                            .entry_queue;
                        if !push_query!(self, queue, (query, shard)) {
                            //
                        }
                    }
                }
                NodeQueryGet { node, callback } => {
                    pop_query!(
                        self,
                        self.nodes
                            .get_mut(usize::from(node))
                            .expect("Node ID out of bounds")
                            .entry_queue,
                        callback
                    );
                }
                Aggregate {
                    timeout,
                    query,
                    node,
                } => {
                    self.history.push(self.status().finish_shard(query, time));
                    self.schedule(
                        timeout,
                        Process::Node {
                            id: node,
                            stage: NodeStage::GetQuery,
                        },
                    );
                }
            }
        }
    }

    /// Run until no events are found.
    pub fn run(&mut self) {
        while !self.scheduled_events.is_empty() {
            self.advance();
        }
    }

    /// Run until certain time.
    pub fn run_until(&mut self, time: Duration) {
        while !self.scheduled_events.is_empty() && self.time < time {
            self.advance();
        }
    }

    /// Run until certain time.
    pub fn run_steps(&mut self, steps: usize) {
        for _ in 0..steps {
            if self.scheduled_events.is_empty() {
                break;
            }
            self.advance();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::distributions::Distribution;
    use std::io::Cursor;

    pub(crate) struct WrappingEchoDistribution<T> {
        size: T,
    }

    impl<T> WrappingEchoDistribution<T> {
        pub(crate) fn new(size: T) -> Self {
            Self { size }
        }
    }

    impl<T> Distribution<T> for WrappingEchoDistribution<T>
    where
        T: std::convert::TryFrom<u32> + std::ops::Rem<T, Output = T> + Copy,
        <T as std::convert::TryFrom<u32>>::Error: std::fmt::Debug,
    {
        fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> T {
            T::try_from(rng.next_u32()).unwrap() % self.size
        }
    }

    pub(crate) struct ToFloatDistribution<D>(pub(crate) D);

    impl<D> Distribution<f32> for ToFloatDistribution<D>
    where
        D: Distribution<u64>,
    {
        fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> f32 {
            self.0.sample(rng) as f32
        }
    }

    #[test]
    fn test_event_order() {
        let proc = Process::Broker(BrokerStage::GetQuery);
        assert_eq!(
            Event::new(
                Duration::from_secs(1),
                Process::Broker(BrokerStage::GetQuery),
            ),
            Event::new(
                Duration::from_secs(1),
                Process::Broker(BrokerStage::GetQuery),
            )
        );
        assert!(
            Event::new(Duration::from_secs(1), proc).cmp(&Event::new(Duration::from_secs(1), proc))
                == Ordering::Equal
        );
        assert!(
            Event::new(Duration::new(1, 1), proc).cmp(&Event::new(Duration::from_secs(1), proc))
                == Ordering::Greater
        );
        assert!(
            Event::new(Duration::new(1, 1), proc).cmp(&Event::new(Duration::new(1, 2), proc))
                == Ordering::Less
        );
        assert!(
            Event::new(Duration::from_secs(1), proc)
                .partial_cmp(&Event::new(Duration::from_secs(1), proc))
                == Some(Ordering::Equal)
        );
        assert!(
            Event::new(Duration::new(1, 1), proc)
                .partial_cmp(&Event::new(Duration::from_secs(1), proc))
                == Some(Ordering::Greater)
        );
        assert!(
            Event::new(Duration::new(1, 1), proc)
                .partial_cmp(&Event::new(Duration::new(1, 2), proc))
                == Some(Ordering::Less)
        );
    }

    #[test]
    fn test_create_sim_from_config() {
        let config = r#"
brokers: 2
cpus_per_node: 2
query_interval: 10
seed: 17
routing: static
time_unit: micro
assignment:
    - [0, 1, 0]
    - [1, 1, 0]
    - [0, 1, 1]"#;
        let sim = QueryRoutingSimulation::from_config(
            Config::from_yaml(Cursor::new(config)).unwrap(),
            vec![config::Query {
                selected_shards: None,
                selection_time: 0,
                retrieval_times: vec![],
            }],
        );
        assert_eq!(sim.scheduled_events.len(), 8);
        assert!(sim.incoming_queries.is_empty());
        // TODO
    }
}
