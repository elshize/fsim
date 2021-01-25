use crate::{
    cost_select, fifo_select, run_events, weighted_cost_select, write_from_channel, BoxedSelect,
    Broker, BrokerQueues, Dispatch, Event, Node, NodeEvent, NodeId, NodeQueue, NodeQueueEntry,
    NodeResponse, NumCores, ProbabilisticDispatcher, Query, QueryLog, QueryRow, RequestId,
    ResponseStatus, RoundRobinDispatcher, TimedEvent,
};

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use eyre::WrapErr;
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use ndarray::Array2;
use serde::{Deserialize, Serialize};
use simrs::{ComponentId, Fifo, QueueId, Simulation};

use optimization::AssignmentResult;

type NodeComponentId = ComponentId<NodeEvent>;

/// Type of dispatching policy.
#[derive(
    Debug, PartialEq, Eq, Clone, Copy, strum::EnumString, strum::ToString, Serialize, Deserialize,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum DispatcherOption {
    /// See [`RoundRobinDispatcher`].
    RoundRobin,
    /// Dispatches uniformly across available machines. See [`ProbabilisticDispatcher`].
    Uniform,
    /// Dispatchers with optimized probabilities computed at the beginning of the simulation.
    /// See [`ProbabilisticDispatcher`].
    Probabilistic,
}

/// Type of queue for incoming shard requests in nodes.
#[derive(
    Debug, PartialEq, Eq, Clone, Copy, strum::EnumString, strum::ToString, Serialize, Deserialize,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum QueueType {
    /// First in, first out queue.
    Fifo,

    /// Shortest queries first.
    Priority,

    /// Taking requests ranomly from the waiting queue according to the probability given by the
    /// inverse of their estimated times.
    Weighted,
}

#[allow(missing_docs)]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Default)]
pub struct SimulationLabel {
    pub collection: String,
    pub sharding: String,
    pub ordering: String,
    pub dispatcher: String,
    pub queries_per_second: usize,
    pub queue_type: String,
    pub disabled_node: Option<usize>,
}

impl std::fmt::Display for SimulationLabel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}-{}-{}-{}-{}{}",
            self.collection,
            self.sharding,
            self.ordering,
            self.dispatcher,
            self.queries_per_second,
            self.queue_type,
            self.disabled_node
                .map(|n| format!("-dis-{}", n))
                .unwrap_or_default()
        )
    }
}

/// Configuration for a single simulation.
#[derive(Deserialize)]
pub struct SimulationConfig {
    /// Label identifying this config, e.g., in the terminal message.
    pub label: SimulationLabel,

    /// Path to the file containing query times.
    pub queries_path: PathBuf,

    /// Path to the file containing input query events.
    pub query_events_path: PathBuf,

    /// Path to the file containing Taily shard scores.
    pub shard_scores_path: Option<PathBuf>,

    /// Path to the output file with query data.
    pub query_output: PathBuf,

    /// Path to the output file with node data.
    pub node_output: PathBuf,

    /// Result of shard replica assignment to machines.
    pub assignment: AssignmentResult,

    /// Number of cores per machine.
    #[serde(default)]
    pub num_cores: NumCores,

    /// Number of nodes in the simulation.
    pub num_nodes: usize,

    /// Number of index shards.
    pub num_shards: usize,

    /// Dispatcher policy.
    pub dispatcher: DispatcherOption,

    /// Queue type. See [`QueueType`].
    pub queue_type: QueueType,

    /// List of nodes that are disabled during this simulation.
    pub disabled_nodes: Vec<NodeId>,
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
    ///
    /// # Errors
    ///
    /// Returns error whenever the path is not a correct file, e.g., any path that ends with `..`.
    pub fn new<P1, P2>(query_input: P1, shard_scores_input: Option<P2>) -> eyre::Result<Self>
    where
        P1: Into<PathBuf>,
        P2: Into<PathBuf>,
    {
        let query_input: PathBuf = query_input.into();
        Ok(Self {
            file_path: Self::cached_file_path(&query_input)?,
            query_input,
            shard_scores_input: shard_scores_input.map(Into::into),
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
    pub fn load(meta: &CacheMetadata, pb: Option<&ProgressBar>) -> Result<Self, CacheLoadError> {
        pb.map(|pb| pb.set_style(ProgressStyle::default_spinner()));
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
            pb.map(|pb| pb.set_message("Query cache detected. Loading cache: {}"));
            let cached: CachedQueries =
                bincode::deserialize_from(cached_file).wrap_err("failed to parse cached file")?;
            if cached.meta.shard_scores_input == meta.shard_scores_input {
                Ok(cached)
            } else {
                Err(CacheLoadError::DifferentShardScoreInput)
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

fn select_function(queue_type: QueueType, queries: Rc<Vec<Query>>) -> BoxedSelect<NodeQueueEntry> {
    match queue_type {
        QueueType::Fifo => fifo_select(),
        QueueType::Priority => cost_select(queries),
        QueueType::Weighted => weighted_cost_select(queries),
    }
}

impl SimulationConfig {
    /// Reads the input query events.
    ///
    /// # Errors
    ///
    /// Returns an error if it fails to read the file or parse the input.
    pub fn read_query_events(&self) -> eyre::Result<Vec<TimedEvent>> {
        read_query_events(&self.query_events_path)
    }

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

    fn process_input_queries(
        &self,
        meta: CacheMetadata,
        pb: Option<&ProgressBar>,
    ) -> eyre::Result<CachedQueries> {
        pb.map(|pb| pb.set_style(ProgressStyle::default_spinner()));
        let error_wrapper = || {
            format!(
                "Failed to load queries from file: `{}`",
                meta.query_input.display()
            )
        };
        // log::info!("Reading, processing, and chaching query data. This could take some time.");
        // log::info!("Reading query data...");
        pb.map(|pb| pb.set_message(&format!("{}: Reading query data", self.label)));
        let file = File::open(&meta.query_input).wrap_err_with(error_wrapper)?;
        let rows: eyre::Result<Vec<QueryRow>> = serde_json::Deserializer::from_reader(file)
            .into_iter::<QueryRow>()
            .map(|r| Ok(r?))
            .collect();
        let mut rows = rows.wrap_err_with(error_wrapper)?;
        // log::info!("Queries loaded. Now sorting...");
        pb.map(|pb| pb.set_message(&format!("{}: Sorting query data", self.label)));
        rows.sort_by(|lhs, rhs| {
            (&lhs.query_id, &lhs.shard_id).cmp(&(&rhs.query_id, &rhs.shard_id))
        });
        let mut shard_scores_iter = Self::load_shard_scores(&meta)?.map(IntoIterator::into_iter);
        let shard_scores = std::iter::from_fn(|| {
            shard_scores_iter
                .as_mut()
                .map_or(Some(None), |iter| Some(iter.next()))
        });
        pb.map(|pb| pb.set_message(&format!("{}: Aggregating query data", self.label)));
        // log::info!("Queries sorted. Now processing...");
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
        //log::info!("Queries processed. Now caching...");
        pb.map(|pb| pb.set_message(&format!("{}: Storing cache", self.label)));
        queries.store_cache()?;
        Ok(queries)
    }

    /// Loads and preprocesses queries from the input file, or loads the previously preprocessed
    /// cached file.
    ///
    /// See [`CachedQueries`] for more information.
    fn queries(&self, pb: &ProgressBar) -> eyre::Result<CachedQueries> {
        let meta = CacheMetadata::new(&self.queries_path, self.shard_scores_path.as_ref())?;
        CachedQueries::load(&meta, Some(pb)).or_else(|err| {
            if let CacheLoadError::Io(err) = err {
                Err(err)
            } else {
                self.process_input_queries(meta, Some(pb))
            }
        })
    }

    /// Inserts the node components into `simulation`, and returns a vector of IDs of the
    /// registered components.
    fn nodes(
        &self,
        simulation: &mut Simulation,
        incoming_queues: &[QueueId<NodeQueue<NodeQueueEntry>>],
        outcoming_queue: QueueId<Fifo<NodeResponse>>,
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

    fn optimized_probabilistic_dispatcher(&self) -> eyre::Result<Box<dyn Dispatch>> {
        let weights = Array2::from_shape_vec(
            (self.num_nodes, self.num_shards),
            self.assignment
                .weights
                .iter()
                .flatten()
                .copied()
                .collect_vec(),
        )
        .wrap_err("invavlid nodes config")?;
        Ok(Box::new(ProbabilisticDispatcher::adaptive(weights)?))
    }

    #[allow(clippy::cast_precision_loss)]
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
    fn dispatcher(&self) -> eyre::Result<Box<dyn Dispatch>> {
        match self.dispatcher {
            DispatcherOption::RoundRobin => {
                Ok(Box::new(RoundRobinDispatcher::new(&self.assignment.nodes)))
            }
            DispatcherOption::Probabilistic => self.optimized_probabilistic_dispatcher(),
            DispatcherOption::Uniform => self.uniform_probabilistic_dispatcher(),
        }
    }

    /// Runs the simulation based on the given configuration.
    ///
    /// # Errors
    ///
    /// May return an error if it fails to read files or the read configuration turns out to be
    /// invalid.
    pub fn run(&self, pb: &ProgressBar, message_type: super::MessageType) -> eyre::Result<()> {
        let mut sim = Simulation::default();

        let (query_sender, receiver) = std::sync::mpsc::channel();
        write_from_channel(
            io::BufWriter::new(File::create(&self.query_output)?),
            receiver,
        );
        let (node_sender, receiver) = std::sync::mpsc::channel();
        write_from_channel(
            io::BufWriter::new(File::create(&self.node_output)?),
            receiver,
        );

        let query_log_id = sim.state.insert(
            QueryLog::new(sim.scheduler.clock(), Duration::from_secs(10))
                .query_sender(query_sender)
                .node_sender(node_sender),
        );

        let queries = Rc::new(self.queries(&pb)?.queries);
        let query_events = read_query_events(&self.query_events_path)?;

        let node_incoming_queues: Vec<_> = (0..self.num_nodes)
            .map(|_| {
                sim.add_queue(NodeQueue::unbounded(select_function(
                    self.queue_type,
                    Rc::clone(&queries),
                )))
            })
            .collect();
        let broker_response_queue = sim.add_queue(Fifo::default());

        let node_ids = self.nodes(
            &mut sim,
            &node_incoming_queues,
            broker_response_queue,
            &queries,
        );
        let responses_key = sim
            .state
            .insert(HashMap::<RequestId, ResponseStatus>::new());
        let mut dispatcher = self.dispatcher()?;
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

        pb.set_length(query_events.len() as u64);
        pb.set_draw_delta(pb.length() / 100);
        for event in query_events {
            match event.event {
                Event::Broker(e) => sim.schedule(event.time, broker, e),
                Event::Node { .. } => eyre::bail!("invalid query event"),
            }
        }

        run_events(&mut sim, query_log_id, &pb, message_type);
        Ok(())
    }
}

/// Reads the list of initial events passed as an input file.
///
/// If the file's extension is `.json`, then it will treat it as a JSON file.
/// Otherwise, it will be treated as a binary [Bincode](https://github.com/servo/bincode) file.
pub fn read_query_events(file_path: &Path) -> eyre::Result<Vec<TimedEvent>> {
    let file = File::open(file_path)
        .wrap_err_with(|| format!("unable to open query events file: {}", file_path.display()))?;
    if file_path.extension().map_or(false, |e| e == "json") {
        serde_json::Deserializer::from_reader(file)
            .into_iter()
            .collect::<Result<Vec<TimedEvent>, _>>()
            .wrap_err("unable to parse query events in JSON format")
    } else {
        rmp_serde::from_read::<_, Vec<TimedEvent>>(file)
            .wrap_err("unable to parse query events in MsgPack format")
    }
}
