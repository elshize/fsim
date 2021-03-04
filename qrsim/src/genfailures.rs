use std::path::PathBuf;
use std::time::Duration;

use clap::Clap;
use eyre::{Result, WrapErr};
use humantime::parse_duration;
use qrsim::NodeStatus;
use rand_chacha::{
    rand_core::{RngCore, SeedableRng},
    ChaChaRng,
};
use rand_distr::{Distribution, WeightedAliasIndex};

const INTERVAL: Duration = Duration::from_millis(50);

struct DurationArg(Duration);

impl std::str::FromStr for DurationArg {
    type Err = eyre::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_duration(s).wrap_err("invalid time").map(DurationArg)
    }
}

/// Generates input for distributed search simulation.
#[derive(Clap)]
#[clap(version, author)]
struct Opt {
    /// Time of intended simulation in microseconds.
    #[clap(short, long)]
    time: DurationArg,

    /// Seed to use for random number generator.
    #[clap(short, long)]
    seed: Option<u64>,

    #[clap(short)]
    num_nodes: usize,

    #[clap(short, long)]
    output_path: PathBuf,
}

#[derive(Debug, Clone)]
struct MarkovChain {
    state: NodeStatus,
    transitions: [WeightedAliasIndex<f32>; 3],
    injured_fraction: f32,
}

impl MarkovChain {
    fn new(transitions: [WeightedAliasIndex<f32>; 3], injured_fraction: f32) -> Self {
        Self {
            state: NodeStatus::Healthy,
            transitions,
            injured_fraction,
        }
    }

    const fn index(state: NodeStatus) -> usize {
        match state {
            NodeStatus::Healthy => 0,
            NodeStatus::Injured(_) => 1,
            NodeStatus::Unresponsive => 2,
        }
    }

    fn next<R: RngCore>(&mut self, rng: &mut R) -> NodeStatus {
        let distr = &self.transitions[Self::index(self.state)];
        let next_state = match distr.sample(rng) {
            0 => NodeStatus::Healthy,
            1 => NodeStatus::Injured(self.injured_fraction),
            2 => NodeStatus::Unresponsive,
            _ => unreachable!(),
        };
        self.state = next_state;
        next_state
    }
}

fn generate(opt: &Opt) -> Result<()> {
    let end_time = opt.time.0;
    let mut rng = if let Some(seed) = opt.seed {
        ChaChaRng::seed_from_u64(seed)
    } else {
        ChaChaRng::from_entropy()
    };
    let mut time = INTERVAL;
    let mut status = NodeStatus::Healthy;
    let mut chains = vec![
        MarkovChain::new(
            [
                WeightedAliasIndex::new(vec![0.99, 0.01, 0.0])?,
                WeightedAliasIndex::new(vec![1.0, 0.0, 0.0])?,
                WeightedAliasIndex::new(vec![0.05, 0.95, 0.0])?,
            ],
            0.95,
        );
        opt.num_nodes
    ];
    let mut events = Vec::<qrsim::TimedEvent>::new();
    while time < end_time {
        for (node_id, chain) in chains.iter_mut().enumerate() {
            let next_status = chain.next(&mut rng);
            if next_status != status {
                status = next_status;
                let event = match status {
                    NodeStatus::Healthy => qrsim::NodeEvent::Cure,
                    NodeStatus::Injured(f) => qrsim::NodeEvent::Injure(f),
                    NodeStatus::Unresponsive => qrsim::NodeEvent::Suspend,
                };
                let event = qrsim::TimedEvent {
                    event: qrsim::Event::Node {
                        node_id: qrsim::NodeId::from(node_id),
                        event,
                    },
                    time,
                };
                events.push(event);
            }
        }
        time += INTERVAL;
    }
    let mut writer = std::fs::File::create(&opt.output_path)?;
    serde_json::to_writer(&mut writer, &events)?;
    Ok(())
}

fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    let opts: Opt = Opt::parse();
    generate(&opts)
}
