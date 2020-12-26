//! Query routing simulation application.
#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::default_trait_access,
    clippy::inline_always
)]

use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use clap::Clap;
use eyre::{eyre, WrapErr};
use itertools::Itertools;
use ndarray::Array2;
use serde::{Deserialize, Serialize};
use simrs::{ComponentId, QueueId, Simulation};

use optimization::{AssignmentResult, LpOptimizer, Optimizer};
use qrsim::{
    run_until, write_from_channel, Broker, BrokerEvent, BrokerQueues, Dispatch, Node, NodeEvent,
    NodeId, NodeRequest, NodeResponse, NumCores, ProbabilisticDispatcher, Query, QueryLog,
    QueryRow, RequestId, ResponseStatus, RoundRobinDispatcher,
};

type NodeComponentId = ComponentId<NodeEvent>;

/// Type of dispatching policy.
#[derive(strum::EnumString, strum::ToString, Serialize, Deserialize)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
enum DispatcherOption {
    /// See [`RoundRobinDispatcher`].
    RoundRobin,
    /// See [`ProbabilisticDispatcher`].
    Probabilistic,
}

/// Runs query routing simulation.
#[derive(Clap)]
#[clap(version, author)]
struct Opt {
    /// Path to a file containing query input data.
    #[clap(long)]
    queries_path: PathBuf,

    /// Path to a file containing query events describing when queries arrrive at the broker.
    #[clap(long)]
    query_events_path: PathBuf,

    /// Path to a file containing query events describing when queries arrrive at the broker.
    #[clap(long, default_value = "16")]
    num_cores: NumCores,

    /// Path to a file containing node configuration in JSON array format.
    /// Each element (node) contains the IDs of shards it contains.
    #[clap(long)]
    nodes: PathBuf,

    /// Verbosity.
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,

    /// Dispatcher type.
    #[clap(short, long)]
    dispatcher: DispatcherOption,

    /// Store the logs this file.
    #[clap(long)]
    log_output: Option<PathBuf>,

    /// Do not log to the stderr.
    #[clap(long)]
    no_stderr: bool,
}

#[derive(Deserialize)]
struct SimulationConfig {
    queries_path: PathBuf,
    query_events_path: PathBuf,

    assignment: AssignmentResult,

    #[serde(default)]
    num_cores: NumCores,

    num_nodes: usize,
    num_shards: usize,

    dispatcher: DispatcherOption,
}

impl TryFrom<Opt> for SimulationConfig {
    type Error = eyre::Error;
    fn try_from(opt: Opt) -> eyre::Result<Self> {
        let nodes_file = File::open(&opt.nodes).wrap_err("unable to read node config")?;
        let assignment: AssignmentResult =
            serde_json::from_reader(nodes_file).wrap_err("unable to parse node config")?;
        let max_shard = *assignment
            .nodes
            .iter()
            .flatten()
            .max()
            .ok_or_else(|| eyre!("empty node config"))?;
        let num_nodes = assignment.nodes.len();
        Ok(Self {
            queries_path: opt.queries_path,
            query_events_path: opt.query_events_path,
            num_cores: opt.num_cores,
            assignment,
            num_nodes,
            num_shards: max_shard + 1,
            dispatcher: opt.dispatcher,
        })
    }
}

/// Cached query data.
///
/// The input data retrieved from PISA is slow to load, because it's bulky and need processing
/// before it can be used in the simulation. Instead of introducing another explicit preprocessing
/// phase and maintain another set of files, this program will do the required processing and cache
/// the data on the drive. If the cache is found at the beginning of the simulation and is older
/// than the file passed as the input, the cache will be loaded instead.
///
/// The cache is stored in the [Bincode](https://github.com/servo/bincode) format.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CachedQueries {
    file_path: PathBuf,
    queries: Vec<Query>,
}

impl CachedQueries {
    /// Computes the path of the cached file for the input file at `file_path`.
    ///
    /// # Errors
    ///
    /// Returns error whenever the path is not a correct file, e.g., any path that ends with `..`.
    fn cached_file_path<P: AsRef<Path>>(file_path: &P) -> eyre::Result<PathBuf> {
        Ok(file_path.as_ref().with_file_name(format!(
            "{}.cache",
            file_path
                .as_ref()
                .file_name()
                .ok_or_else(|| eyre::eyre!("invalid path"))?
                .to_string_lossy()
        )))
    }

    /// Stores the query cache.
    fn store_cache(&self) -> eyre::Result<()> {
        let cached_filename = Self::cached_file_path(&self.file_path)?;
        bincode::serialize_into(File::create(cached_filename)?, self)?;
        Ok(())
    }
}

impl SimulationConfig {
    /// Loads queries from a cached file.
    ///
    /// Returns `None` if the cache does not exist, or is older than the requested query file.
    /// See [`CachedQueries`] for more information.
    fn queries_from_cache(&self) -> Option<Vec<Query>> {
        let cached_filename = CachedQueries::cached_file_path(&self.queries_path).ok()?;
        let input_file = File::open(&self.queries_path).ok()?;
        let cached_file = File::open(cached_filename).ok()?;
        if input_file.metadata().ok()?.modified().ok()?
            <= cached_file.metadata().ok()?.modified().ok()?
        {
            log::info!("Query cache detected. Loading cache...");
            let cached: CachedQueries = bincode::deserialize_from(cached_file).ok()?;
            log::info!("Cache loaded.");
            Some(cached.queries)
        } else {
            None
        }
    }

    /// Loads and preprocesses queries from the input file, or loads the previously preprocessed
    /// cached file.
    ///
    /// See [`CachedQueries`] for more information.
    fn queries(&self) -> eyre::Result<Vec<Query>> {
        if let Some(queries) = self.queries_from_cache() {
            return Ok(queries);
        }
        let error_wrapper = || {
            format!(
                "Failed to load queries from file: `{}`",
                self.queries_path.display()
            )
        };
        log::info!("Reading, processing, and chaching query data. This could take some time.");
        log::info!("Reading query data...");
        let file = File::open(&self.queries_path).wrap_err_with(error_wrapper)?;
        let rows: eyre::Result<Vec<QueryRow>> = serde_json::Deserializer::from_reader(file)
            .into_iter::<QueryRow>()
            .map(|r| Ok(r?))
            .collect();
        let mut rows = rows.wrap_err_with(error_wrapper)?;
        log::info!("Queries loaded. Now sorting...");
        rows.sort_by(|lhs, rhs| {
            (&lhs.query_id, &lhs.shard_id).cmp(&(&rhs.query_id, &rhs.shard_id))
        });
        log::info!("Queries sorted. Now processing...");
        let queries = CachedQueries {
            file_path: self.queries_path.clone(),
            queries: rows
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
                .collect(),
        };
        log::info!("Queries processed. Now caching...");
        queries.store_cache()?;
        Ok(queries.queries)
    }

    /// Inserts the node components into `simulation`, and returns a vector of IDs of the
    /// registered components.
    fn nodes(
        &self,
        simulation: &mut Simulation,
        incoming_queues: &[QueueId<(NodeRequest, ComponentId<BrokerEvent>)>],
        outcoming_queue: QueueId<NodeResponse>,
        queries: &Rc<Vec<Query>>,
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

    /// Returns a dispatcher based on the configuration. See [`DispatcherOption`].
    fn dispatcher(&self) -> Box<dyn Dispatch> {
        match self.dispatcher {
            DispatcherOption::RoundRobin => {
                Box::new(RoundRobinDispatcher::new(&self.assignment.nodes))
            }
            DispatcherOption::Probabilistic => {
                let weights = Array2::from_shape_vec(
                    (self.num_nodes, self.num_shards),
                    self.assignment
                        .weights
                        .iter()
                        .flatten()
                        .copied()
                        .collect_vec(),
                )
                .expect("invavlid nodes config");
                let optimizer = LpOptimizer;
                let probabilities = optimizer.optimize(weights.view());
                Box::new(ProbabilisticDispatcher::new(probabilities.view()))
            }
        }
    }

    /// Runs the simulation based on the given configuration.
    fn run(&self) -> eyre::Result<()> {
        let mut sim = Simulation::default();

        let (query_sender, receiver) = std::sync::mpsc::channel();
        write_from_channel(io::BufWriter::new(File::create("queries.csv")?), receiver);
        let (node_sender, receiver) = std::sync::mpsc::channel();
        write_from_channel(io::BufWriter::new(File::create("nodes.csv")?), receiver);

        let query_log_id = sim.state.insert(
            QueryLog::new(sim.scheduler.clock(), Duration::from_secs(10))
                .query_sender(query_sender)
                .node_sender(node_sender),
        );

        let queries = Rc::new(self.queries()?);
        let query_events = read_query_events(&self.query_events_path)?;

        let node_incoming_queues: Vec<_> = (0..self.num_nodes).map(|_| sim.add_queue()).collect();
        let broker_response_queue = sim.add_queue::<NodeResponse>();

        let node_ids = self.nodes(
            &mut sim,
            &node_incoming_queues,
            broker_response_queue,
            &queries,
        );
        let responses_key = sim
            .state
            .insert(HashMap::<RequestId, ResponseStatus>::new());
        let broker = sim.add_component(Broker {
            queues: BrokerQueues {
                node: node_incoming_queues.iter().copied().collect(),
                response: broker_response_queue,
            },
            node_ids,
            queries: Rc::clone(&queries),
            dispatcher: self.dispatcher(),
            query_log_id,
            responses: responses_key,
        });

        for event in query_events {
            match event.event {
                qrsim::Event::Broker(e) => sim.schedule(event.time, broker, e),
                qrsim::Event::Node { .. } => eyre::bail!("invalid query event"),
            }
        }

        run_until(&mut sim, Duration::from_secs(60), query_log_id);
        Ok(())
    }
}

/// Reads the list of initial events passed as an input file.
///
/// If the file's extension is `.json`, then it will treat it as a JSON file.
/// Otherwise, it will be treated as a binary [Bincode](https://github.com/servo/bincode) file.
fn read_query_events(file_path: &Path) -> eyre::Result<Vec<qrsim::TimedEvent>> {
    let file = File::open(file_path)
        .wrap_err_with(|| format!("unable to open query events file: {}", file_path.display()))?;
    if file_path.extension().map_or(false, |e| e == "json") {
        serde_json::Deserializer::from_reader(file)
            .into_iter()
            .collect::<Result<Vec<qrsim::TimedEvent>, _>>()
            .wrap_err("unable to parse query events in JSON format")
    } else {
        rmp_serde::from_read::<_, Vec<qrsim::TimedEvent>>(file)
            .wrap_err("unable to parse query events in Bincode format")
    }
}

/// Set up a logger based on the given user options.
fn set_up_logger(opt: &Opt) -> Result<(), fern::InitError> {
    let log_level = match opt.verbose {
        1 => log::LevelFilter::Info,
        2 => log::LevelFilter::Debug,
        3 => log::LevelFilter::Trace,
        _ => log::LevelFilter::Warn,
    };
    let dispatch = fern::Dispatch::new()
        .format(|out, message, record| out.finish(format_args!("[{}] {}", record.level(), message)))
        .level(log_level);
    let dispatch = if let Some(path) = &opt.log_output {
        let _ = std::fs::remove_file(path);
        dispatch.chain(
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .append(false)
                .open(path)?,
        )
    } else {
        dispatch
    };
    let dispatch = if opt.no_stderr {
        dispatch
    } else {
        dispatch.chain(std::io::stderr())
    };
    dispatch.apply()?;
    Ok(())
}

fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    let opt = Opt::parse();
    set_up_logger(&opt)?;
    let conf = SimulationConfig::try_from(opt)?;
    conf.run()
}
