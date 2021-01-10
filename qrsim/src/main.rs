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

use std::cell::RefCell;
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
    run_events, write_from_channel, Broker, BrokerEvent, BrokerQueues, Dispatch, Node, NodeEvent,
    NodeId, NodeRequest, NodeResponse, NumCores, ProbabilisticDispatcher, Query, QueryLog,
    QueryRow, RequestId, ResponseStatus, RoundRobinDispatcher,
};

type NodeComponentId = ComponentId<NodeEvent>;

/// Type of dispatching policy.
#[derive(
    Debug, PartialEq, Eq, Clone, Copy, strum::EnumString, strum::ToString, Serialize, Deserialize,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
enum DispatcherOption {
    /// See [`RoundRobinDispatcher`].
    RoundRobin,
    /// Dispatches uniformly across available machines. See [`ProbabilisticDispatcher`].
    Uniform,
    /// Dispatchers with optimized probabilities computed at the beginning of the simulation.
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

    /// List of nodes to disable at the beginning of the simulation.
    #[clap(long)]
    disabled_nodes: Vec<NodeId>,

    /// Path to a file containing query events describing when queries arrrive at the broker.
    #[clap(long, default_value = "8")]
    num_cores: NumCores,

    /// Path to a file containing node configuration in JSON array format.
    /// Each element (node) contains the IDs of shards it contains.
    #[clap(long)]
    nodes: PathBuf,

    /// Path to a file containing shard ranking scores for queries.
    #[clap(long)]
    shard_scores: Option<PathBuf>,

    /// Output file where the query statistics will be stored in the CSV format.
    #[clap(long)]
    query_output: PathBuf,

    /// Output file where the node-level statistics will be stored in the CSV format.
    #[clap(long)]
    node_output: PathBuf,

    /// Verbosity.
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,

    /// Dispatcher type.
    #[clap(short, long)]
    dispatcher: DispatcherOption,

    /// File where to store the probability matrix solved by the probabilistic dispatcher.
    #[clap(long)]
    prob_matrix_output: Option<PathBuf>,

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
    shard_scores_path: Option<PathBuf>,

    assignment: AssignmentResult,

    #[serde(default)]
    num_cores: NumCores,

    num_nodes: usize,
    num_shards: usize,

    dispatcher: DispatcherOption,
    disabled_nodes: Vec<NodeId>,
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
            shard_scores_path: opt.shard_scores,
            num_cores: opt.num_cores,
            assignment,
            num_nodes,
            num_shards: max_shard + 1,
            dispatcher: opt.dispatcher,
            disabled_nodes: opt.disabled_nodes,
        })
    }
}

/// Metadata object containing file paths for [`CachedQueries`].
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct CacheMetadata {
    /// The path to the cached metadata file.
    pub file_path: PathBuf,
    /// The path to the query input file.
    pub query_input: PathBuf,
    /// The path to the shard scores input file.
    pub shard_scores_input: Option<PathBuf>,
}

impl CacheMetadata {
    /// Creates a new cache metadata from the given input files, where the second one is optional.
    pub fn new<P1, P2>(query_input: P1, shard_scores_input: Option<P2>) -> eyre::Result<Self>
    where
        P1: Into<PathBuf>,
        P2: Into<PathBuf>,
    {
        let query_input: PathBuf = query_input.into();
        Ok(Self {
            file_path: Self::cached_file_path(&query_input)?,
            query_input,
            shard_scores_input: shard_scores_input.map(|p| p.into()),
        })
    }

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
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct CachedQueries {
    meta: CacheMetadata,
    queries: Vec<Query>,
}

#[derive(Debug, thiserror::Error)]
enum CacheLoadError {
    #[error("query cache not found")]
    CacheNotFound,
    #[error("Cache file found but it is older than the query input file.")]
    NewerQueryInput,
    #[error("Cache file found but it is older than the shard scores input file.")]
    NewerShardScoreInput,
    #[error("Cache file found but it was computed on a different shard scores input file.")]
    DifferentShardScoreInput,
    #[error("error while loading query cache: {0}")]
    Io(#[from] eyre::Error),
}

impl CachedQueries {
    /// Loads queries from a cached file.
    ///
    /// # Errors
    ///
    /// Returns one of the following types of error:
    /// - [`CacheLoadError::CacheNotFound`] when cache file was not found.
    /// - [`CacheLoadError::NewerQueryInput`] when the query input is newer than the cache.
    /// - [`CacheLoadError::NewerShardScoreInput`] when the shard scores input is newer than the cache.
    /// - [`CacheLoadError::DifferentShardScoreInput`] when the cache exists but the shard score
    ///   file it was computed from is different from the one provided (or if one exists when the
    ///   other does not).
    /// - [`CacheLoadError::Io`] when any other errors occur while reading files and computing
    ///   paths.
    pub fn load(meta: &CacheMetadata) -> Result<Self, CacheLoadError> {
        let cached_file = File::open(&meta.file_path).map_err(|_| CacheLoadError::CacheNotFound)?;
        let query_input =
            File::open(&meta.query_input).wrap_err("unable to open query input file")?;
        let query_input_last_mod = query_input
            .metadata()
            .wrap_err("unable to retrieve query input metadata")?
            .modified()
            .wrap_err("unable to retrieve last modified date")?;
        let cached_file_last_mod = cached_file
            .metadata()
            .wrap_err("unable to retrieve cached file metadata")?
            .modified()
            .wrap_err("unable to retrieve last modified date")?;
        if query_input_last_mod <= cached_file_last_mod {
            if let Some(shard_scores_input) = &meta.shard_scores_input {
                let scores_input_last_mod = shard_scores_input
                    .metadata()
                    .wrap_err("unable to retrieve query input metadata")?
                    .modified()
                    .wrap_err("unable to retrieve last modified date")?;
                if scores_input_last_mod > cached_file_last_mod {
                    return Err(CacheLoadError::NewerShardScoreInput);
                }
            }
            log::info!("Query cache detected. Loading cache...");
            let cached: CachedQueries =
                bincode::deserialize_from(cached_file).wrap_err("failed to parse cached file")?;
            if cached.meta.shard_scores_input != meta.shard_scores_input {
                Err(CacheLoadError::DifferentShardScoreInput)
            } else {
                log::info!("Cache loaded.");
                Ok(cached)
            }
        } else {
            Err(CacheLoadError::NewerQueryInput)
        }
    }

    /// Stores the query cache.
    fn store_cache(&self) -> eyre::Result<()> {
        bincode::serialize_into(File::create(&self.meta.file_path)?, self)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ProbabilityMatrixOtuput {
    weights: Vec<Vec<f32>>,
    probabilities: Vec<Vec<f32>>,
    loads: Vec<Vec<f32>>,
}

impl ProbabilityMatrixOtuput {
    fn new(
        weights: ndarray::ArrayView2<f32>,
        probabilities: ndarray::ArrayView2<f32>,
    ) -> eyre::Result<Self> {
        Self::validate(weights, probabilities)?;
        Ok(Self {
            weights: array_to_vec(weights),
            probabilities: array_to_vec(probabilities),
            loads: array_to_vec((weights.to_owned() * &probabilities).view()),
        })
    }

    /// Validates the probability matrix with respect to the weights.
    ///
    /// To pass validation, the following must hold:
    ///
    /// 1. Each column in the probability matrix must sum up to (approximately) 1. Because of the
    ///    finite precision of the floating point representation, this value must be within a
    ///    certain error from 1.0.
    /// 2. For any weight `w(i, j) = 0.0` (signifying that shard `j` is not assigned to machine `i`)
    ///    `p(i, j)` must also be equal 0.0.
    fn validate(
        weights: ndarray::ArrayView2<f32>,
        probabilities: ndarray::ArrayView2<f32>,
    ) -> eyre::Result<()> {
        for (column, (probabilities, weights)) in probabilities
            .gencolumns()
            .into_iter()
            .zip(weights.gencolumns())
            .enumerate()
        {
            let sum = probabilities.iter().zip(weights).enumerate().try_fold(
                0.0,
                |sum, (row, (p, w))| {
                    if *w == 0.0 && *p > 0.0 {
                        Err(eyre!(
                            "weight at ({},{}) is 0 but probability is {}",
                            row,
                            column,
                            p
                        ))
                    } else {
                        Ok(sum + p)
                    }
                },
            )?;
            if (sum - 1.0).abs() > 0.0001 {
                eyre::bail!(
                    "probabilities for shard {} do not sum to 1 ({})",
                    column,
                    sum
                );
            }
        }
        Ok(())
    }
}

fn array_to_vec(array: ndarray::ArrayView2<f32>) -> Vec<Vec<f32>> {
    array
        .genrows()
        .into_iter()
        .map(|row| row.into_iter().copied().collect())
        .collect()
}

impl SimulationConfig {
    fn load_shard_scores(meta: &CacheMetadata) -> eyre::Result<Option<Vec<Vec<f32>>>> {
        meta.shard_scores_input
            .as_ref()
            .map(|path| -> eyre::Result<Vec<Vec<f32>>> {
                log::info!("Queries sorted. Now processing...");
                let file = File::open(path)?;
                serde_json::Deserializer::from_reader(file)
                    .into_iter::<Vec<f32>>()
                    .map(|r| Ok(r?))
                    .collect()
            })
            .transpose()
    }

    fn process_input_queries(meta: CacheMetadata) -> eyre::Result<CachedQueries> {
        let error_wrapper = || {
            format!(
                "Failed to load queries from file: `{}`",
                meta.query_input.display()
            )
        };
        log::info!("Reading, processing, and chaching query data. This could take some time.");
        log::info!("Reading query data...");
        let file = File::open(&meta.query_input).wrap_err_with(error_wrapper)?;
        let rows: eyre::Result<Vec<QueryRow>> = serde_json::Deserializer::from_reader(file)
            .into_iter::<QueryRow>()
            .map(|r| Ok(r?))
            .collect();
        let mut rows = rows.wrap_err_with(error_wrapper)?;
        log::info!("Queries loaded. Now sorting...");
        rows.sort_by(|lhs, rhs| {
            (&lhs.query_id, &lhs.shard_id).cmp(&(&rhs.query_id, &rhs.shard_id))
        });
        let mut shard_scores_iter = Self::load_shard_scores(&meta)?.map(|v| v.into_iter());
        let shard_scores = std::iter::from_fn(|| {
            if let Some(iter) = &mut shard_scores_iter {
                Some(iter.next())
            } else {
                Some(None)
            }
        });
        log::info!("Queries sorted. Now processing...");
        let queries = CachedQueries {
            meta,
            queries: rows
                .into_iter()
                .group_by(|r| r.query_id)
                .into_iter()
                .zip(shard_scores)
                .map(|((_, rows), scores)| {
                    Query {
                        selection_time: 0, // TODO
                        selected_shards: None,
                        retrieval_times: rows.map(|r| r.time).collect(),
                        shard_scores: scores,
                    }
                })
                .collect(),
        };
        log::info!("Queries processed. Now caching...");
        queries.store_cache()?;
        Ok(queries)
    }

    /// Loads and preprocesses queries from the input file, or loads the previously preprocessed
    /// cached file.
    ///
    /// See [`CachedQueries`] for more information.
    fn queries(&self) -> eyre::Result<CachedQueries> {
        let meta = CacheMetadata::new(&self.queries_path, self.shard_scores_path.as_ref())?;
        CachedQueries::load(&meta).or_else(|err| match err {
            CacheLoadError::Io(err) => Err(err),
            _ => {
                log::warn!("{}", err);
                Self::process_input_queries(meta)
            }
        })
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

    fn optimized_probabilistic_dispatcher(
        &self,
        matrix_output: Option<File>,
    ) -> eyre::Result<Box<dyn Dispatch>> {
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
        if let Some(file) = matrix_output {
            match ProbabilityMatrixOtuput::new(weights.view(), probabilities.view()) {
                Ok(matrix) => serde_json::to_writer(file, &matrix)?,
                Err(err) => panic!(
                    "matrix validation failed: {} {:?}",
                    err,
                    array_to_vec(probabilities.t())
                ),
            }
        }
        Ok(Box::new(ProbabilisticDispatcher::new(
            probabilities.view(),
        )?))
    }

    fn uniform_probabilistic_dispatcher(&self) -> eyre::Result<Box<dyn Dispatch>> {
        let probabilities = Array2::from_shape_vec(
            (self.num_nodes, self.num_shards),
            self.assignment
                .weights
                .iter()
                .flat_map(|node_weights| {
                    let len = node_weights.len();
                    node_weights
                        .iter()
                        .map(move |&w| if w > 0.0 { 1.0 / len as f32 } else { 0.0 })
                })
                .collect_vec(),
        )
        .expect("invavlid nodes config");
        Ok(Box::new(ProbabilisticDispatcher::new(
            probabilities.view(),
        )?))
    }

    /// Returns a dispatcher based on the configuration. See [`DispatcherOption`].
    ///
    /// # Errors
    ///
    /// This function might return an error because if `matrix_output.is_some()`, then the result
    /// of probability optimization will be stored in this file, and thus an I/O could occur.
    fn dispatcher(&self, matrix_output: Option<File>) -> eyre::Result<Box<dyn Dispatch>> {
        match self.dispatcher {
            DispatcherOption::RoundRobin => {
                Ok(Box::new(RoundRobinDispatcher::new(&self.assignment.nodes)))
            }
            DispatcherOption::Probabilistic => {
                self.optimized_probabilistic_dispatcher(matrix_output)
            }
            DispatcherOption::Uniform => self.uniform_probabilistic_dispatcher(),
        }
    }

    /// Runs the simulation based on the given configuration.
    fn run(
        &self,
        queries_output: File,
        nodes_output: File,
        matrix_output: Option<File>,
    ) -> eyre::Result<()> {
        let mut sim = Simulation::default();

        let (query_sender, receiver) = std::sync::mpsc::channel();
        write_from_channel(io::BufWriter::new(queries_output), receiver);
        let (node_sender, receiver) = std::sync::mpsc::channel();
        write_from_channel(io::BufWriter::new(nodes_output), receiver);

        let query_log_id = sim.state.insert(
            QueryLog::new(sim.scheduler.clock(), Duration::from_secs(10))
                .query_sender(query_sender)
                .node_sender(node_sender),
        );

        let queries = Rc::new(self.queries()?.queries);
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
        let mut dispatcher = self.dispatcher(matrix_output)?;
        for &node_id in &self.disabled_nodes {
            dispatcher.disable_node(node_id)?;
        }
        let broker = sim.add_component(Broker {
            queues: BrokerQueues {
                node: node_incoming_queues.iter().copied().collect(),
                response: broker_response_queue,
            },
            node_ids,
            queries: Rc::clone(&queries),
            dispatcher: RefCell::new(dispatcher),
            query_log_id,
            responses: responses_key,
        });

        let num_queries = query_events.len();
        for event in query_events {
            match event.event {
                qrsim::Event::Broker(e) => sim.schedule(event.time, broker, e),
                qrsim::Event::Node { .. } => eyre::bail!("invalid query event"),
            }
        }

        run_events(&mut sim, num_queries, query_log_id);
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
            .wrap_err("unable to parse query events in MsgPack format")
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
    if opt.prob_matrix_output.is_some() && opt.dispatcher != DispatcherOption::Probabilistic {
        eyre::bail!("matrix output given but dispatcher is not probabilistic");
    }
    let matrix_output = opt
        .prob_matrix_output
        .as_ref()
        .map(|p| File::create(p))
        .transpose()?;
    let query_output = File::create(&opt.query_output)?;
    let node_output = File::create(&opt.node_output)?;
    set_up_logger(&opt)?;
    let conf = SimulationConfig::try_from(opt)?;
    conf.run(query_output, node_output, matrix_output)
}
