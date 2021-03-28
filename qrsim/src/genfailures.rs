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

type Transitions = [[f64; 2]; 2];

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

    /// The average time in milliseconnds that should be spent in the ingured state.
    #[clap(short, long)]
    granularity: u64,

    /// The fraction of time each node spends in healthy state over time.
    #[clap(short, long)]
    health: f64,

    #[clap(short, long)]
    injury: f64,
}

/// Calculates the 2-state Markov chain transitions modeling system with the given injury granularity and health.
fn transitions(granularity: u64, health: f64) -> Transitions {
    let injured_to_healthy = 1.0 / granularity as f64;
    let healthy_to_injured = (1.0 - health) * injured_to_healthy / health;
    [
        [1.0 - healthy_to_injured, healthy_to_injured],
        [injured_to_healthy, 1.0 - injured_to_healthy],
    ]
}

/// Calculates the capacity of the system with the given health and injury coefficient, relative to its
/// base capacity.
fn relative_capacity(health: f64, injury: f64) -> f64 {
    health + injury * (1.0 - health)
}

#[derive(Debug, Clone)]
struct MarkovChain {
    state: NodeStatus,
    transitions: Transitions,
    weights: [WeightedAliasIndex<f64>; 2],
    injury: f64,
}

impl MarkovChain {
    fn new(transitions: Transitions, injury: f64, initial_state: NodeStatus) -> Result<Self> {
        let weights = [
            WeightedAliasIndex::new(transitions[0].into())?,
            WeightedAliasIndex::new(transitions[1].into())?,
        ];
        Ok(Self {
            state: initial_state,
            transitions,
            weights,
            injury,
        })
    }

    const fn index(state: NodeStatus) -> usize {
        match state {
            NodeStatus::Healthy => 0,
            NodeStatus::Injured(_) => 1,
            NodeStatus::Unresponsive => 2,
        }
    }

    fn next<R: RngCore>(&mut self, rng: &mut R) -> NodeStatus {
        let distr = &self.weights[Self::index(self.state)];
        let next_state = match distr.sample(rng) {
            0 => NodeStatus::Healthy,
            1 => NodeStatus::Injured(1.0 / self.injury as f32),
            2 => NodeStatus::Unresponsive,
            _ => unreachable!(),
        };
        self.state = next_state;
        next_state
    }
}

// #[derive(Debug, Clone)]
// struct MarkovChain {
//     state: NodeStatus,
//     transitions: [Vec<f64>; 3],
//     weights: [WeightedAliasIndex<f64>; 3],
//     injured_fraction: f32,
// }

// impl MarkovChain {
//     fn new(transitions: [Vec<f64>; 3], injured_fraction: f32) -> Result<Self> {
//         let weights = [
//             WeightedAliasIndex::new(transitions[0].clone())?,
//             WeightedAliasIndex::new(transitions[1].clone())?,
//             WeightedAliasIndex::new(transitions[2].clone())?,
//         ];
//         Ok(Self {
//             state: NodeStatus::Healthy,
//             transitions,
//             weights,
//             injured_fraction,
//         })
//     }

//     const fn index(state: NodeStatus) -> usize {
//         match state {
//             NodeStatus::Healthy => 0,
//             NodeStatus::Injured(_) => 1,
//             NodeStatus::Unresponsive => 2,
//         }
//     }

//     fn next<R: RngCore>(&mut self, rng: &mut R) -> NodeStatus {
//         let distr = &self.weights[Self::index(self.state)];
//         let next_state = match distr.sample(rng) {
//             0 => NodeStatus::Healthy,
//             1 => NodeStatus::Injured(self.injured_fraction),
//             2 => NodeStatus::Unresponsive,
//             _ => unreachable!(),
//         };
//         self.state = next_state;
//         next_state
//     }

//     fn steady_probabilities(&self) -> [f64; 3] {
//         let a = self.transitions[0][0];
//         let b = self.transitions[0][1];
//         let c = self.transitions[1][0];
//         let d = self.transitions[1][1];
//         [c / (c + 1.0 - a), (1.0 - a) / (c + 1.0 - a), 0.0]
//         // let mut p = arr2(&[
//         //     [
//         //         self.transitions[0][0],
//         //         self.transitions[1][0],
//         //         self.transitions[2][0],
//         //     ],
//         //     [
//         //         self.transitions[0][1],
//         //         self.transitions[1][1],
//         //         self.transitions[2][1],
//         //     ],
//         //     [
//         //         self.transitions[0][2],
//         //         self.transitions[1][2],
//         //         self.transitions[2][2],
//         //     ],
//         // ]);
//         // println!("{:?}", p);
//         // println!("{:?}", p.dot(&p));
//         // while !(p.column(0) == p.column(1) && p.column(1) == p.column(2)) {
//         //     p = p.dot(&p);
//         // }
//         // let col = p.column(0);
//         // [col[0], col[1], col[2]]
//     }
// }

fn initial_states<R: RngCore>(
    health: f64,
    injury: f64,
    num_nodes: usize,
    rng: &mut R,
) -> Vec<qrsim::NodeStatus> {
    let distr = WeightedAliasIndex::new(vec![health, 1.0 - health])
        .unwrap_or_else(|_| panic!("invalid health: {}", health));
    (0..num_nodes)
        .map(|_| {
            if distr.sample(rng) == 1 {
                qrsim::NodeStatus::Injured(injury as f32)
            } else {
                qrsim::NodeStatus::Healthy
            }
        })
        .collect()
}

fn generate(opt: &Opt) -> Result<()> {
    let end_time = opt.time.0;
    let mut rng = if let Some(seed) = opt.seed {
        ChaChaRng::seed_from_u64(seed)
    } else {
        ChaChaRng::from_entropy()
    };

    let transitions = transitions(opt.granularity, opt.health);
    println!("Transitions: {:?}", transitions);
    let capacity = relative_capacity(opt.health, opt.injury);
    println!("Relative capacity: {}", capacity);

    let initial_states = initial_states(opt.health, opt.injury, opt.num_nodes, &mut rng);
    let mut events = initial_states
        .iter()
        .enumerate()
        .filter_map(|(node_id, s)| match s {
            qrsim::NodeStatus::Injured(i) => Some(qrsim::TimedEvent {
                event: qrsim::Event::Node {
                    node_id: qrsim::NodeId::from(node_id),
                    event: qrsim::NodeEvent::Injure(*i),
                },
                time: Duration::default(),
            }),
            _ => None,
        })
        .collect::<Vec<_>>();

    let mut chains = initial_states
        .into_iter()
        .map(|s| MarkovChain::new(transitions, opt.injury, s).unwrap())
        .collect::<Vec<_>>();

    let mut time = Duration::from_millis(1);
    while time < end_time {
        for (node_id, chain) in chains.iter_mut().enumerate() {
            let old_status = chain.state;
            let status = chain.next(&mut rng);
            if status != old_status {
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
        time += Duration::from_millis(1);
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

#[cfg(test)]
mod test {
    use super::*;
    use float_cmp::approx_eq;

    #[test]
    fn test_compute_transitions() {
        let transitions = transitions(10, 0.9901);
        assert!(approx_eq!(f64, transitions[0][0], 0.999, epsilon = 0.0001));
        assert!(approx_eq!(f64, transitions[0][1], 0.001, epsilon = 0.0001));
        assert!(approx_eq!(f64, transitions[1][1], 0.9, epsilon = 0.0001));
        assert!(approx_eq!(f64, transitions[1][0], 0.1, epsilon = 0.0001));
    }

    #[test]
    fn test_compute_relative_capacity() {
        assert!(approx_eq!(
            f64,
            relative_capacity(0.9, 0.5),
            0.9 + 0.1 * 0.5,
            epsilon = 0.0001
        ));
    }
}
