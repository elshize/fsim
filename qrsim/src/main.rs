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

use std::convert::TryFrom;
use std::fs::File;
use std::path::PathBuf;

use clap::Clap;
use eyre::{eyre, WrapErr};
use indicatif::{ProgressBar, ProgressStyle};

use optimization::AssignmentResult;
use qrsim::{DispatcherOption, NodeId, NumCores, QueueType, SimulationConfig};

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

    /// Type of queue for incoming shard requests in nodes.
    #[clap(long, default_value = "fifo")]
    queue_type: QueueType,
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
            query_output: opt.query_output,
            node_output: opt.node_output,
            num_cores: opt.num_cores,
            assignment,
            num_nodes,
            num_shards: max_shard + 1,
            dispatcher: opt.dispatcher,
            disabled_nodes: opt.disabled_nodes,
        })
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
    let query_output = File::create(&opt.query_output)?;
    let node_output = File::create(&opt.node_output)?;
    set_up_logger(&opt)?;
    let queue_type = opt.queue_type;
    let conf = SimulationConfig::try_from(opt)?;
    let events = conf.read_query_events()?;
    let pb = ProgressBar::new(events.len() as u64)
        .with_style(ProgressStyle::default_bar().template("{msg} {wide_bar} {percent}%"));
    conf.run(query_output, node_output, queue_type, events, &pb)
}
