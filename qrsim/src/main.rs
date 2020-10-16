use std::fmt;
use std::fs::File;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use eyre::WrapErr;
use itertools::Itertools;
use serde::Deserialize;
use simulation::{ComponentId, Key, QueueId};

use qrsim::{
    Broker, BrokerEvent, BrokerQueues, Dispatch, Node, NodeEvent, NodeId, NodeRequest,
    NodeResponse, NumCores, Query, QueryGenerator, QueryGeneratorEvent, QueryLog, QueryRequest,
    QueryRow, ShardId, Simulation,
};

type QueryGeneratorComponentId = ComponentId<QueryGeneratorEvent>;
type BrokerComponentId = ComponentId<BrokerEvent>;
type NodeComponentId = ComponentId<NodeEvent>;

pub fn deserialize_humantime<'de, D>(d: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct V;

    impl<'de2> serde::de::Visitor<'de2> for V {
        type Value = Duration;

        fn expecting(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            fmt.write_str("a duration")
        }

        fn visit_str<E>(self, v: &str) -> Result<Duration, E>
        where
            E: serde::de::Error,
        {
            humantime::parse_duration(v)
                .map_err(|_| E::invalid_value(serde::de::Unexpected::Str(v), &self))
        }
    }

    d.deserialize_str(V)
}

#[derive(Deserialize)]
struct QueryConfig {
    path: PathBuf,
    #[serde(deserialize_with = "deserialize_humantime")]
    average_interval: Duration,
}

fn default_broker_queue_size() -> usize {
    1000
}

fn default_node_queue_size() -> usize {
    1000
}

#[derive(Deserialize)]
struct BrokerConfig {
    #[serde(default = "default_broker_queue_size")]
    incoming_queue_size: usize,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            incoming_queue_size: default_broker_queue_size(),
        }
    }
}

//#[derive(Deserialize)]
//struct NodeConfig {
//    //shards: Vec<ShardId>,
//    queue_size: Option<usize>,
//    num_cores: Option<NumCores>,
//}
//
//impl NodeConfig {
//    fn new(shards: Vec<ShardId>) -> Self {
//        Self {
//            //shards,
//            queue_size: None,
//            num_cores: None,
//        }
//    }
//}

#[derive(Deserialize)]
struct SimulationConfig {
    queries: QueryConfig,
    #[serde(default)]
    broker: BrokerConfig,
    //nodes: Vec<NodeConfig>,
    #[serde(default = "default_node_queue_size")]
    node_queue_size: usize,
    #[serde(default)]
    num_cores: NumCores,
    num_nodes: usize,
    num_shards: usize,
}

/// Dispatches in a round-robin fashion.
///
/// NOTE: Right now, it assumes all shards are in each node.
struct RoundRobinDispatcher {
    num_nodes: usize,
    num_shards: usize,
    next_node: usize,
}

impl Dispatch for RoundRobinDispatcher {
    fn dispatch(&mut self, shards: &[ShardId]) -> Vec<(ShardId, qrsim::NodeId)> {
        shards
            .iter()
            .map(|s| {
                let node = NodeId::from(self.next_node);
                self.next_node = (self.next_node + 1) % self.num_nodes;
                (*s, node)
            })
            .collect()
    }
    fn num_nodes(&self) -> usize {
        self.num_nodes
    }
    fn num_shards(&self) -> usize {
        self.num_shards
    }
}

struct DummyDispatcher;
impl Dispatch for DummyDispatcher {
    fn dispatch(&mut self, _shards: &[ShardId]) -> Vec<(ShardId, qrsim::NodeId)> {
        vec![]
    }
    fn num_nodes(&self) -> usize {
        0
    }
    fn num_shards(&self) -> usize {
        0
    }
}

// /// Returns the average number of finished requests per second for the entire duration of
// /// the simulation so far.
// pub fn total_throughput(simulation: &Simulation, ) -> f64 {
//     simulation.state.get(query_log_id).unwrap().len();
// }

impl SimulationConfig {
    fn queries(&self) -> eyre::Result<Vec<Query>> {
        let error_wrapper = || {
            format!(
                "Failed to load queries from file: `{}`",
                self.queries.path.display()
            )
        };
        let file = File::open(&self.queries.path).wrap_err_with(error_wrapper)?;
        let rows: eyre::Result<Vec<QueryRow>> = serde_json::Deserializer::from_reader(file)
            .into_iter::<QueryRow>()
            .map(|r| Ok(r?))
            .collect();
        let mut rows = rows.wrap_err_with(error_wrapper)?;
        rows.sort_by(|lhs, rhs| {
            (&lhs.query_id, &lhs.shard_id).cmp(&(&rhs.query_id, &rhs.shard_id))
        });
        Ok(rows
            .into_iter()
            .group_by(|r| r.query_id)
            .into_iter()
            .map(|(_, rows)| {
                Query {
                    selection_time: 0, // TODO
                    selected_shards: None,
                    retrieval_times: rows.map(|r| r.time).collect(),
                }
            })
            .collect())
    }

    fn query_generator(
        &self,
        simulation: &mut Simulation,
        broker: BrokerComponentId,
        entry_queue: QueueId<QueryRequest>,
        num_queries: usize,
        query_log_key: Key<QueryLog>,
    ) -> QueryGeneratorComponentId {
        simulation.add_component(
            QueryGenerator::new(
                rand::thread_rng(),
                rand_distr::Normal::new(self.queries.average_interval.as_micros() as f32, 1.0)
                    .unwrap(),
                rand_distr::Uniform::new(0, num_queries),
                entry_queue,
                broker,
                query_log_key,
            ), //.on_generated_query(|_request| todo!()),
        )
    }

    fn nodes(
        &self,
        simulation: &mut Simulation,
        incoming_queues: &[QueueId<(NodeRequest, ComponentId<BrokerEvent>)>],
        outcoming_queue: QueueId<NodeResponse>,
        queries: Rc<Vec<Query>>,
    ) -> Vec<NodeComponentId> {
        incoming_queues
            .iter()
            .enumerate()
            .map(|(id, incoming)| {
                simulation.add_component(Node::new(
                    NodeId::from(id),
                    Rc::clone(&queries),
                    *incoming,
                    outcoming_queue,
                    self.num_cores.into(),
                ))
            })
            .collect()
    }

    fn run(&self) -> eyre::Result<()> {
        let mut sim = Simulation::default();

        let query_log_id = sim.state.insert(QueryLog::new(
            sim.scheduler.clock(),
            Duration::from_secs(10),
        ));
        let queries = Rc::new(self.queries()?);

        let entry_queue = sim.add_bounded_queue::<QueryRequest>(self.broker.incoming_queue_size);
        let node_incoming_queues: Vec<_> = (0..self.num_nodes).map(|_| sim.add_queue()).collect();
        let broker_response_queue = sim.add_queue::<NodeResponse>();

        let nodes = self.nodes(
            &mut sim,
            &node_incoming_queues,
            broker_response_queue,
            Rc::clone(&queries),
        );
        let broker = sim.add_component(Broker::new(
            BrokerQueues {
                entry: entry_queue,
                node: node_incoming_queues.iter().copied().collect(),
                response: broker_response_queue,
            },
            nodes,
            Rc::clone(&queries),
            RoundRobinDispatcher {
                next_node: 0,
                num_nodes: self.num_nodes,
                num_shards: self.num_shards,
            },
            query_log_id,
            self.num_nodes * 10,
        ));
        let query_generator =
            self.query_generator(&mut sim, broker, entry_queue, queries.len(), query_log_id);

        sim.schedule(
            std::time::Duration::default(),
            query_generator,
            QueryGeneratorEvent::GenerateQuery,
        );

        sim.run_until(Duration::from_secs(120), query_log_id);
        Ok(())
    }
}

fn setup_logger() -> Result<(), fern::InitError> {
    std::fs::remove_file("/tmp/output.log").expect("Couldn't remove log file.");
    fern::Dispatch::new()
        .format(|out, message, record| out.finish(format_args!("[{}] {}", record.level(), message)))
        .level(log::LevelFilter::Off)
        .chain(
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .append(false)
                .open("/tmp/output.log")?,
        )
        .apply()?;
    Ok(())
}

fn main() -> eyre::Result<()> {
    setup_logger()?;
    color_eyre::install()?;
    let conf = SimulationConfig {
        queries: QueryConfig {
            path: PathBuf::from("/tmp/queries"),
            average_interval: Duration::from_micros(1500),
        },
        broker: BrokerConfig::default(),
        num_cores: NumCores::from(8),
        node_queue_size: default_node_queue_size(),
        num_nodes: 20,
        num_shards: 123,
    };
    conf.run()
}
