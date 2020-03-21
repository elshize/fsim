use crate::config::{self, Config, TimeUnit};
use crate::im_status::Status;
use crate::logger;
use crate::process::Runnable;
use crate::query_generator::QueryGenerator;
use crate::queue::{PopResult, ProcessCallback, PushResult, Queue};
use crate::routing_matrix::RoutingMatrixBuilder;
use crate::shard_selector::ExhaustiveSelector;
use crate::{
    Broker, BrokerStage, Event, Node, NodeId, NodeRequest, NodeStage, Process, Query,
    QueryGeneratorStage, QueryId, ShardId,
};
use rand::distributions::Uniform;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaChaRng;
use rand_distr::Normal;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::rc::Rc;
use std::time::Duration;

const EPSILON: Duration = Duration::from_nanos(1);

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
            history: vec![Status::default()],
            next_steps: Vec::new(),
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

    /// Schedule a process to run at `time`.
    pub fn schedule(&mut self, time: Duration, process: Process) {
        self.scheduled_events
            .push(Reverse(Event::new(time, process)));
    }

    /// Returns the current simulation step.
    pub fn step(&self) -> usize {
        self.history.len() - 1
    }

    /// Returns the current simulation status.
    pub fn history(&self) -> impl Iterator<Item = &Status> {
        self.history.iter().rev()
    }

    /// Returns the current simulation status.
    pub fn status(&self) -> &Status {
        self.history.iter().last().unwrap()
    }

    /// Goes back in history. If no past steps exist, it returns an error.
    pub fn step_back(&mut self) -> Result<(), ()> {
        if self.history.len() > 1 {
            self.next_steps.push(self.history.pop().unwrap());
            Ok(())
        } else {
            Err(())
        }
    }

    /// Goes one step forward in history. Calls `advance` until history is modified.
    pub fn step_forward(&mut self) {
        let history_size = self.history.len();
        while history_size == self.history.len() {
            self.advance();
        }
    }

    fn log_event(&self, time: Duration, process: &Process) {
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

    fn run_process(
        &self,
        time: Duration,
        process: Process,
        status: Status,
    ) -> (Effect<'a>, Status) {
        match process {
            Process::QueryGenerator(stage) => (self.query_generator.run(stage), status),
            Process::Broker(stage) => {
                let status = if let &BrokerStage::Select(query) = &stage {
                    status.pick_up_query(query, time)
                } else {
                    status
                };
                (self.broker.run(stage), status)
            }
            Process::Node { id, stage } => (
                self.nodes
                    .get(usize::from(id))
                    .expect("Node ID out of bounds")
                    .node
                    .run(stage),
                status,
            ),
        }
    }

    fn handle_effect(&mut self, time: Duration, effect: Effect<'a>, status: Status) -> Status {
        use Effect::*;
        match effect {
            Schedule(event) => {
                self.schedule(time + event.time, event.process);
                status
            }
            ScheduleMany(events) => {
                for event in events {
                    self.schedule(time + event.time, event.process);
                }
                status
            }
            QueryQueuePut(query, process) => {
                push_query!(self, time, self.incoming_queries, query, process);
                log::debug!("[{:?}] [{}] Enters incoming queue", time, query);
                status.enter_query(query, time)
            }
            QueryQueueGet(callback) => {
                pop_query!(self, time, self.incoming_queries, callback);
                status
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
                status
            }
            Dispatch { nodes, query } => {
                let mut status = status.dispatch_query(query, time, nodes.len());
                for node in nodes {
                    let queue = &mut self
                        .nodes
                        .get_mut(usize::from(node.id))
                        .expect("Node ID out of bounds")
                        .entry_queue;
                    if !push_query!(self, time, queue, (query, node.shard)) {
                        status = status.drop_shard(query, time);
                    }
                }
                status
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
                status
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
                status.finish_shard(query, time)
            }
        }
    }

    /// Advance the simulation
    pub fn advance(&mut self) {
        if let Some(next) = self.next_steps.pop() {
            self.history.push(next);
            return;
        }
        if let Some(Reverse(Event { time, process })) = self.scheduled_events.pop() {
            assert!(
                self.status().time() <= time,
                "Current event scheduled at time {:?}, which is earlier than current time {:?}: {:?}",
                time,
                self.status().time(),
                Event { time, process },
            );

            self.log_event(time, &process);
            let (effect, status) = self.run_process(time, process, self.status().clone());
            let status = self.handle_effect(time, effect, status);
            self.history.push(
                status.log_events(
                    logger::clear()
                        .expect("Unable to retrieve logs")
                        .into_iter(),
                ),
            );
        }
    }

    /// Run until no events are found.
    pub fn run(&mut self) {
        while !self.scheduled_events.is_empty() {
            self.step_forward();
        }
    }

    /// Run until certain time.
    pub fn run_until(&mut self, time: Duration) {
        while !self.scheduled_events.is_empty() && self.status().time() < time {
            self.step_forward();
        }
    }

    /// Run until certain time.
    pub fn run_steps(&mut self, steps: usize) {
        for _ in 0..steps {
            if self.scheduled_events.is_empty() {
                break;
            }
            self.step_forward();
        }
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
    }
}
