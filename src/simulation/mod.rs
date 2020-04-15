//! Query routing simulation components.
//!
//! # Constructing Simulation
//!
//! A simulation is constructed from a [`Config`] structure and a vector of queries.
//!
//! The configuration can be constructed programmatically (see related documentation) but typically
//! it is more convenient to load it from a YAML file.
//!
//! Similarly, [`config::Query`] can be constructed directly, or parsed from JSON.
//! A vector of queries can be also directly loaded from a file.
//!
//! ```
//! # use fsim::simulation::*;
//! # use fsim::simulation::config::*;
//! # fn main() -> anyhow::Result<()> {
//! use anyhow::Context;
//!
//! let config = r#"
//! time_unit: micro
//! brokers: 2
//! cpus_per_node: 2
//! query_distribution:
//!     mean: 10
//!     std: 1
//! assignment:
//!     - [0, 1, 0]
//!     - [1, 1, 0]
//!     - [0, 1, 1]"#;
//! let queries = r#"
//! {"retrieval_times":[913,979,1022,994]}
//! {"retrieval_times":[56,66,58,62]}
//! {"retrieval_times":[2197,1893,1910,2031]}"#;
//!
//! let config = Config::from_yaml(std::io::Cursor::new(config))?;
//! let queries: anyhow::Result<Vec<config::Query>> =
//!     serde_json::Deserializer::from_reader(std::io::Cursor::new(queries))
//!         .into_iter()
//!         .map(|elem| elem.context("Failed to parse query"))
//!         .collect();
//! let simulation = Simulation::<ReversibleProgression>::new(config, queries?);
//! # Ok(())
//! # }
//! ```
//! [`Config`]: config/struct.Config.html
//! [`config::Query`]: config/struct.Query.html

use id_derive::Id;
use rand::distributions::Uniform;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaChaRng;
use rand_distr::Normal;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::cmp::{Ord, Ordering, PartialOrd};
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
pub use queue::{PopResult, ProcessCallback, PushResult, Queue};

mod broker;
pub use broker::{Broker, BrokerStage};

mod node;
pub use node::{Node, NodeStage};

pub mod replica_selector;

pub mod routing_matrix;
use routing_matrix::RoutingMatrixBuilder;

mod shard_selector;
use shard_selector::ExhaustiveSelector;

pub mod config;
pub use config::{Config, TimeUnit};

mod im_status;
pub use im_status::Status as ImStatus;

pub mod logger;

mod progression;
pub use progression::{Progression, ReversibleProgression};

/// A snapshot of a simulation at a specific time.
pub trait Status {
    /// Current time.
    fn time(&self) -> Duration;

    /// Returns how many queries have entered the main incoming queue.
    fn queries_entered(&self) -> usize;

    /// Returns how many queries have finished processing.
    fn queries_finished(&self) -> usize;

    /// Active queries are these that have entered the system but have yet to finish, and have not
    /// been dropped.
    fn queries_active(&self) -> usize;

    /// Number of queries that have been finished but some shard requests have been dropped.
    fn queries_incomplete(&self) -> usize;

    /// Iterates over active queries.
    fn active<'a>(&'a self) -> Box<dyn Iterator<Item = &'a (Query, QueryStatus)> + 'a>;

    /// Iterates over finished queries.
    fn finished<'a>(&'a self) -> Box<dyn Iterator<Item = &'a (Query, QueryStatus)> + 'a>;

    /// Get active query identified by `query`.
    fn active_query(&self, query: &Query) -> &QueryStatus;

    /// Logs a new query that entered into the system at `time`.
    fn enter_query(&mut self, query: Query, time: Duration);

    /// Changes the status of `query` to picked up at `time`.
    fn pick_up_query(&mut self, query: Query, time: Duration);

    /// Changes the status of `query` to dispatched to `num_shards` shards at `time`.
    fn dispatch_query(&mut self, query: Query, time: Duration, num_shards: usize);

    /// Records that one node has finished processing.
    fn finish_shard(&mut self, query: Query, time: Duration);

    /// Records a dropped shard request.
    fn drop_shard(&mut self, query: Query, time: Duration);

    /// Iterates over log events.
    fn logs<'a>(&'a self) -> Box<dyn DoubleEndedIterator<Item = &'a String> + 'a>;

    /// Add logged events.
    fn log_events<E>(&mut self, events: E)
    where
        E: Iterator<Item = String>;
}

const EPSILON: Duration = Duration::from_nanos(1);

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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
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
    QueryQueueGet(ProcessCallback<'a, Query, Process>),
    /// TODO
    Route {
        /// Time to wait before sending requests to nodes.
        timeout: Duration,
        /// Query that is being routed.
        query: Query,
        /// Description of nodes, see [`NodeRequest`](struct.NodeRequest.html).
        nodes: Vec<NodeRequest>,
    },
    /// TODO
    Dispatch {
        /// Nodes where the query is being dispatched to.
        nodes: Vec<NodeRequest>,
        /// Query that is being dispatched.
        query: Query,
    },
    /// Similar to `QueryQueueGet` but from a node entry queue.
    NodeQueryGet {
        /// ID of the node.
        node: NodeId,
        /// Callback to call on query.
        callback: ProcessCallback<'a, (Query, ShardId), Process>,
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
    ($s:expr, $time:expr, $queue:expr, $query:expr, $process:expr) => {
        if let PushResult::Pushed { resumed } = $queue.push($query, $process) {
            for process in resumed {
                $s.schedule($time, process);
            }
        }
    };
    ($s:expr, $time:expr, $queue:expr, $query:expr) => {
        if let PushResult::Pushed { resumed } = $queue.push_or_drop($query) {
            for process in resumed {
                $s.schedule($time, process);
            }
            true
        } else {
            false
        }
    };
}

macro_rules! pop_query {
    ($s:expr, $time:expr, $queue:expr, $callback:expr) => {
        if let PopResult::Popped { process, resumed } = $queue.pop($callback) {
            $s.schedule($time, process);
            if let Some(resumed) = resumed {
                $s.schedule($time, resumed);
            }
        }
    };
}

fn duration_constructor(unit: TimeUnit) -> Box<dyn Fn(u64) -> Duration> {
    Box::new(match unit {
        TimeUnit::Micro => Duration::from_micros,
        TimeUnit::Milli => Duration::from_millis,
        TimeUnit::Nano => Duration::from_nanos,
        TimeUnit::Second => Duration::from_secs,
    })
}

type DefaultQueryGenerator<'a> =
    QueryGenerator<'a, Normal<f32>, Uniform<usize>, Box<dyn Fn(u64) -> Duration>>;

/// The simulation.
pub struct Simulation<'a, P> {
    /// Queue of the future events that will be processed in order of time.
    scheduled_events: BinaryHeap<Reverse<Event>>,

    incoming_queries: Queue<'a, Query, Process>,

    /// Nodes. Each contains a number of shard replicas.
    nodes: Vec<NodeEntry<'a>>,

    query_generator: DefaultQueryGenerator<'a>,
    broker: Broker<'a>,
    query_data: Rc<Vec<config::Query>>,
    time_unit: TimeUnit,
    progression: P,
}

/// The result of running simulation for a number of steps or a given interval of time.
pub enum RunResult {
    /// The requested execution has finished.
    Complete,
    /// The simulation finished prematurely because no more events to run are available.
    Incomplete,
}

impl<'a, P: Progression> Simulation<'a, P> {
    /// Constructs a new routing simulation from a config and input queries.
    #[must_use]
    pub fn new(config: &Config, queries: Vec<config::Query>) -> Self {
        let query_selection_dist = Uniform::from(0..queries.len());
        let mut seeder = config
            .seed
            .map_or_else(ChaChaRng::from_entropy, ChaChaRng::seed_from_u64);
        let mut sim = Self {
            incoming_queries: Default::default(),
            scheduled_events: BinaryHeap::new(),
            nodes: vec![],
            query_generator: QueryGenerator::new(
                (0..queries.len()).map(QueryId::from).collect(),
                ChaChaRng::seed_from_u64(seeder.next_u64()),
                Normal::new(
                    config.query_distribution.mean,
                    config.query_distribution.std,
                )
                .unwrap(),
                query_selection_dist,
                duration_constructor(config.time_unit),
            ),
            broker: Broker::new(
                ExhaustiveSelector::new(config.assignment.num_shards()),
                RoutingMatrixBuilder::new(config.assignment.weights_by_shard())
                    .rng(ChaChaRng::seed_from_u64(seeder.next_u64()))
                    .build(),
            ),
            query_data: Rc::from(queries),
            time_unit: config.time_unit,
            progression: P::default(),
        };
        let nodes = sim.set_up_nodes(&config);
        let time = Duration::from_nanos(0);
        for NodeEntry {
            node,
            entry_queue: _,
        } in &nodes
        {
            for _ in 0..config.cpus_per_node {
                sim.schedule(
                    time,
                    Process::Node {
                        id: node.id,
                        stage: NodeStage::GetQuery,
                    },
                );
            }
        }
        sim.schedule(time, Process::QueryGenerator(QueryGeneratorStage::Generate));
        sim.schedule(time, Process::Broker(BrokerStage::RequestQuery));
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

    pub(crate) fn log_event(time: Duration, process: &Process) {
        match process {
            Process::QueryGenerator(stage) => match stage {
                QueryGeneratorStage::Generate => {
                    log::debug!("[{:?}] Generating query", time);
                }
                QueryGeneratorStage::Timeout => {
                    log::debug!("[{:?}] Waiting to generate another query", time);
                }
            },
            Process::Broker(stage) => match stage {
                BrokerStage::RequestQuery => {
                    log::debug!("[{:?}] Broker waiting for incoming query", time);
                }
                BrokerStage::Select(query) => {
                    log::debug!(
                        "[{:?}] [{}] Broker selecting shard and replicas",
                        time,
                        query,
                    );
                }
                BrokerStage::Dispatch { nodes, query } => {
                    for node in nodes {
                        log::debug!(
                            "[{:?}] [{}] Dispatching to node {} for shard {}",
                            time,
                            query,
                            node.id,
                            node.shard,
                        );
                    }
                }
            },
            Process::Node { id, stage } => match stage {
                NodeStage::Retrieval(query, shard) => {
                    log::debug!(
                        "[{:?}] [{}] Picked up by node {:?} for shard {:?}",
                        time,
                        query,
                        id,
                        shard
                    );
                }
                NodeStage::GetQuery => {
                    log::debug!("[{:?}] Node {} waiting for incoming query", time, id);
                }
            },
        };
    }

    pub(crate) fn run_process(
        &self,
        time: Duration,
        process: Process,
        status: &mut P::Status,
    ) -> Effect<'a> {
        match process {
            Process::QueryGenerator(stage) => self.query_generator.run(stage),
            Process::Broker(stage) => {
                if let BrokerStage::Select(query) = stage {
                    status.pick_up_query(query, time);
                }
                self.broker.run(stage)
            }
            Process::Node { id, stage } => self
                .nodes
                .get(usize::from(id))
                .expect("Node ID out of bounds")
                .node
                .run(stage),
        }
    }

    pub(crate) fn handle_effect(
        &mut self,
        time: Duration,
        effect: Effect<'a>,
        status: &mut P::Status,
    ) {
        use Effect::{
            Aggregate, Dispatch, NodeQueryGet, QueryQueueGet, QueryQueuePut, Route, Schedule,
            ScheduleMany,
        };
        match effect {
            Schedule(event) => {
                self.schedule(time + event.time, event.process);
            }
            ScheduleMany(events) => {
                for event in events {
                    self.schedule(time + event.time, event.process);
                }
            }
            QueryQueuePut(query, process) => {
                push_query!(self, time, self.incoming_queries, query, process);
                log::debug!("[{:?}] [{}] Enters incoming queue", time, query);
                status.enter_query(query, time);
            }
            QueryQueueGet(callback) => {
                pop_query!(self, time, self.incoming_queries, callback);
            }
            Route {
                timeout,
                query,
                nodes,
            } => {
                self.schedule(
                    time + timeout + EPSILON,
                    Process::Broker(BrokerStage::RequestQuery),
                );
                self.schedule(
                    time + timeout,
                    Process::Broker(BrokerStage::Dispatch { nodes, query }),
                );
            }
            Dispatch { nodes, query } => {
                status.dispatch_query(query, time, nodes.len());
                for node in nodes {
                    let queue = &mut self
                        .nodes
                        .get_mut(usize::from(node.id))
                        .expect("Node ID out of bounds")
                        .entry_queue;
                    if !push_query!(self, time, queue, (query, node.shard)) {
                        status.drop_shard(query, time);
                    }
                }
            }
            NodeQueryGet { node, callback } => {
                pop_query!(
                    self,
                    time,
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
                self.schedule(
                    time + timeout,
                    Process::Node {
                        id: node,
                        stage: NodeStage::GetQuery,
                    },
                );
                status.finish_shard(query, time);
            }
        }
    }

    fn assert_time(&self, time: &Duration, process: &Process) {
        assert!(
            &self.status().time() <= time,
            "Current event scheduled at time {:?}, which is earlier than current time {:?}: {:?}",
            time,
            self.status().time(),
            Event {
                time: *time,
                process: process.clone()
            },
        );
    }

    /// Advance one step.
    ///
    /// It returns `Incomplete` if no more events are scheduled.
    /// See [`RunResult`](enum.RunResult.html).
    pub fn advance(&mut self) -> RunResult {
        if self.progression.next().is_ok() {
            return RunResult::Complete;
        }
        if let Some(Reverse(Event { time, process })) = self.scheduled_events.pop() {
            self.assert_time(&time, &process);
            Self::log_event(time, &process);
            let status = self.progression.init_step();
            let effect = self.run_process(time, process, &mut status.borrow_mut());
            self.handle_effect(time, effect, &mut status.borrow_mut());
            status.borrow_mut().log_events(
                logger::clear()
                    .expect("Unable to retrieve logs")
                    .into_iter(),
            );
            self.progression.record(status.into_inner());
            RunResult::Complete
        } else {
            RunResult::Incomplete
        }
    }

    /// Schedules `process` to be executed at `time`.
    pub fn schedule(&mut self, time: Duration, process: Process) {
        self.scheduled_events
            .push(Reverse(Event::new(time, process)));
    }

    /// Run the simulation indefinitely. It will only stop if there are no more events scheduled.
    pub fn run(&mut self) {
        while !self.scheduled_events.is_empty() {
            self.advance();
        }
    }

    /// Run simulation for a given time interval.
    ///
    /// The results describes whether or not the requested interval has passed.
    /// See [`RunResult`](enum.RunResult.html).
    pub fn run_until(&mut self, time: Duration) -> RunResult {
        while !self.scheduled_events.is_empty() && self.status().time() < time {
            self.advance();
        }
        if self.status().time() < time {
            RunResult::Incomplete
        } else {
            RunResult::Complete
        }
    }

    /// Run simulation for `steps` number of steps.
    ///
    /// The results describes whether or not the requested number of steps have been executed.
    /// See [`RunResult`](enum.RunResult.html).
    pub fn run_steps(&mut self, steps: usize) -> RunResult {
        for _ in 0..steps {
            if self.scheduled_events.is_empty() {
                return RunResult::Incomplete;
            }
            self.advance();
        }
        RunResult::Complete
    }

    /// Number of steps from the beginning of the simulation.
    pub fn step(&self) -> usize {
        self.progression.step()
    }

    /// Current status of the simulation.
    pub fn status(&self) -> &P::Status {
        self.progression.status()
    }
}

impl<'a> Simulation<'a, ReversibleProgression> {
    /// Go one step back in the history of the simulation.
    ///
    /// # Errors
    ///
    /// It returns an error if the current state is the initial state of the simulation.
    pub fn step_back(&mut self) -> Result<(), ()> {
        self.progression.prev()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::cmp::Ordering;
    use std::io::Cursor;

    #[test]
    fn test_event_order() {
        let proc = || Process::Broker(BrokerStage::RequestQuery);
        assert_eq!(
            Event::new(
                Duration::from_secs(1),
                Process::Broker(BrokerStage::RequestQuery),
            ),
            Event::new(
                Duration::from_secs(1),
                Process::Broker(BrokerStage::RequestQuery),
            )
        );
        assert!(
            Event::new(Duration::from_secs(1), proc())
                .cmp(&Event::new(Duration::from_secs(1), proc()))
                == Ordering::Equal
        );
        assert!(
            Event::new(Duration::new(1, 1), proc())
                .cmp(&Event::new(Duration::from_secs(1), proc()))
                == Ordering::Greater
        );
        assert!(
            Event::new(Duration::new(1, 1), proc()).cmp(&Event::new(Duration::new(1, 2), proc()))
                == Ordering::Less
        );
        assert!(
            Event::new(Duration::from_secs(1), proc())
                .partial_cmp(&Event::new(Duration::from_secs(1), proc()))
                == Some(Ordering::Equal)
        );
        assert!(
            Event::new(Duration::new(1, 1), proc())
                .partial_cmp(&Event::new(Duration::from_secs(1), proc()))
                == Some(Ordering::Greater)
        );
        assert!(
            Event::new(Duration::new(1, 1), proc())
                .partial_cmp(&Event::new(Duration::new(1, 2), proc()))
                == Some(Ordering::Less)
        );
    }

    #[test]
    fn test_create_sim_from_config() {
        let config = r#"
brokers: 2
cpus_per_node: 2
query_distribution:
    mean: 10
    std: 1
seed: 17
routing: static
time_unit: micro
assignment:
    - [0, 1, 0]
    - [1, 1, 0]
    - [0, 1, 1]"#;
        let sim = Simulation::<ReversibleProgression>::new(
            &Config::from_yaml(Cursor::new(config)).unwrap(),
            vec![config::Query {
                selected_shards: None,
                selection_time: 0,
                retrieval_times: vec![],
            }],
        );
        assert_eq!(sim.scheduled_events.len(), 8);
        assert!(sim.incoming_queries.is_empty());
    }
}
