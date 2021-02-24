use super::{Dispatch, NodeId, ShardId, State};
use crate::{NodeQueue, NodeQueueEntry, NodeThreadPool, Query, QueryEstimate, QueryId};

use std::cell::RefCell;
use std::rc::Rc;

use eyre::{Result, WrapErr};
use ndarray::{Array1, Array2, ArrayView1, ArrayView2};
use optimization::Optimizer;
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
use rand_distr::weighted_alias::WeightedAliasIndex;
use rand_distr::Distribution;
use simrs::{Key, QueueId};

#[derive(Debug)]
struct Weight {
    value: f32,
    multiplier: f32,
}

#[allow(clippy::float_cmp)] // `multiplier` is always either 0.0 or 1.0
impl Weight {
    fn enable(&mut self) -> bool {
        if self.multiplier == 1.0 {
            false
        } else {
            self.multiplier = 1.0;
            true
        }
    }
    fn disable(&mut self) -> bool {
        if self.multiplier == 0.0 {
            false
        } else {
            self.multiplier = 0.0;
            true
        }
    }
    fn value(&self) -> f32 {
        self.multiplier * self.value
    }
}

#[derive(Debug, PartialEq)]
struct WeightMatrix {
    weights: Array2<f32>,
    nodes: Array1<f32>,
}

impl WeightMatrix {
    fn new(weights: Array2<f32>) -> Self {
        let nodes = Array1::from_elem(weights.nrows(), 1.0);
        Self { weights, nodes }
    }

    fn weights(&self) -> Array2<f32> {
        let mut weights = self.weights.t().dot(&Array2::from_diag(&self.nodes));
        weights.swap_axes(0, 1);
        weights
    }

    fn weights_scaled(&self, nodes: ArrayView1<f32>) -> Array2<f32> {
        let mut weights = self.weights.t().dot(&Array2::from_diag(&nodes));
        weights.swap_axes(0, 1);
        weights
    }
}

/// Dispatches according to given probabilities.
pub struct ProbabilisticDispatcher {
    shards: Vec<WeightedAliasIndex<f32>>,
    weights: Vec<Vec<Weight>>,
    num_nodes: usize,
    num_shards: usize,
    rng: RefCell<ChaChaRng>,
    weight_matrix: Option<WeightMatrix>,
    load: Option<Load>,
}

pub struct Load {
    pub(crate) queues: Vec<QueueId<NodeQueue<NodeQueueEntry>>>,
    pub(crate) estimates: Rc<Vec<QueryEstimate>>,
    pub(crate) thread_pools: Vec<Key<NodeThreadPool>>,
}

impl Load {
    fn query_time(&self, query_id: QueryId, shard_id: ShardId) -> u64 {
        self.estimates
            .get(query_id.0 - 1)
            .expect("query out of bounds")
            .shard_estimate(shard_id)
    }

    fn queue_lengths<'a>(&'a self, state: &'a State) -> Vec<usize> {
        self.queues
            .iter()
            .map(|queue_id| state.len(*queue_id))
            .collect()
    }

    fn machine_weights(&self, state: &State) -> Array1<f32> {
        let running = self.thread_pools.iter().map(|pool| {
            state
                .get(*pool)
                .expect("unknown thread pool ID")
                .running_threads()
                .iter()
                .map(|t| t.estimated.as_micros())
                .sum::<u128>() as u64
        });
        let waiting = self.queues.iter().map(|queue| {
            state
                .queue(*queue)
                .iter()
                .map(|msg| self.query_time(msg.request.query_id(), msg.request.shard_id()))
                .sum::<u64>()
        });
        let weights: Array1<f32> = running.zip(waiting).map(|(r, w)| (r + w) as f32).collect();
        let weight_sum = weights.sum();
        if weight_sum == 0.0 {
            weights
        } else {
            weights / weight_sum
        }
    }
}

fn format_weights(weights: &[Weight]) -> String {
    use itertools::Itertools;
    format!(
        "{}",
        weights.iter().map(|w| w.value * w.multiplier).format(",")
    )
}

fn calc_distributions(
    weights: &[Vec<Weight>],
) -> Result<Vec<WeightedAliasIndex<f32>>, rand_distr::WeightedError> {
    weights
        .iter()
        .map(|weights| WeightedAliasIndex::new(weights.iter().map(Weight::value).collect()))
        .collect()
}

fn probabilities_to_weights(probabilities: ArrayView2<'_, f32>) -> Vec<Vec<Weight>> {
    probabilities
        .gencolumns()
        .into_iter()
        .map(|probabilities| {
            probabilities
                .into_iter()
                .copied()
                .map(|w| Weight {
                    value: w,
                    multiplier: 1.0,
                })
                .collect::<Vec<Weight>>()
        })
        .collect()
}

impl ProbabilisticDispatcher {
    /// Constructs a new probabilistic dispatcher from the given dispatch matrix.
    ///
    /// # Errors
    ///
    /// Returns an error if probabilities are invalid and cannot be translated into a distribution.
    pub fn new(probabilities: ArrayView2<'_, f32>) -> Result<Self> {
        Self::with_rng(probabilities, ChaChaRng::from_entropy())
    }

    /// Constructs a new probabilistic dispatcher from the given dispatch matrix,
    /// and initializes the internal PRNG from the given seed.
    ///
    /// # Errors
    ///
    /// Returns an error if probabilities are invalid and cannot be translated into a distribution.
    pub fn with_seed(probabilities: ArrayView2<'_, f32>, seed: u64) -> Result<Self> {
        Self::with_rng(probabilities, ChaChaRng::seed_from_u64(seed))
    }

    /// Constructs a new probabilistic dispatcher from the given dispatch matrix,
    /// and initializes the internal PRNG from the given seed.
    ///
    /// # Errors
    ///
    /// Returns an error if probabilities are invalid and cannot be translated into a distribution.
    pub fn with_rng(probabilities: ArrayView2<'_, f32>, rng: ChaChaRng) -> Result<Self> {
        let num_nodes = probabilities.nrows();
        let num_shards = probabilities.ncols();
        let weights: Vec<_> = probabilities_to_weights(probabilities);
        debug_assert_eq!(weights.len(), num_shards);
        for w in &weights {
            debug_assert_eq!(w.len(), num_nodes);
        }
        log::debug!(
            "Created probabilistic dispatcher with {} shards and {} nodes",
            num_shards,
            num_nodes
        );
        Ok(Self {
            num_nodes,
            num_shards,
            rng: RefCell::new(rng),
            shards: calc_distributions(&weights)?,
            weights,
            weight_matrix: None,
            load: None,
        })
    }

    /// Constructs a new dispatcher that optimizes probabilities based on the given weight matrix,
    /// and then repeats the optimization process each time there is a change indicated, such as
    /// disabling or enabling a node.
    ///
    /// # Errors
    ///
    /// Returns an error if probabilities are invalid and cannot be translated into a distribution.
    pub fn adaptive(weight_matrix: Array2<f32>) -> Result<Self> {
        Self::adaptive_with_rng(weight_matrix, ChaChaRng::from_entropy())
    }

    /// Constructs a new adaptive dispatcher with a random seed. See [`Self::adaptive`].
    ///
    /// # Errors
    ///
    /// Returns an error if probabilities are invalid and cannot be translated into a distribution.
    pub fn adaptive_with_seed(weight_matrix: Array2<f32>, seed: u64) -> Result<Self> {
        Self::adaptive_with_rng(weight_matrix, ChaChaRng::seed_from_u64(seed))
    }

    /// Constructs a new adaptive dispatcher with a PRNG. See [`Self::adaptive`].
    ///
    /// # Errors
    ///
    /// Returns an error if probabilities are invalid and cannot be translated into a distribution.
    pub fn adaptive_with_rng(weight_matrix: Array2<f32>, rng: ChaChaRng) -> Result<Self> {
        let probabilities = optimization::LpOptimizer.optimize(weight_matrix.view());
        let mut dispatcher = Self::with_rng(probabilities.view(), rng)?;
        dispatcher.weight_matrix = Some(WeightMatrix::new(weight_matrix));
        Ok(dispatcher)
    }

    pub fn with_load_info(self, load: Load) -> Self {
        Self {
            load: Some(load),
            ..self
        }
    }

    fn change_weight_status<F, G>(
        &mut self,
        node_id: NodeId,
        f: F,
        cond: G,
    ) -> Result<bool, rand_distr::WeightedError>
    where
        F: Fn(&mut Weight) -> bool,
        G: Fn(f32) -> bool,
    {
        if let Some(weight_matrix) = self.weight_matrix.as_mut() {
            let node = &mut weight_matrix.nodes[node_id.0];
            if cond(*node) {
                *node = 1.0 - *node;
                let probabilities =
                    optimization::LpOptimizer.optimize(weight_matrix.weights().view());
                self.weights = probabilities_to_weights(probabilities.view());
                self.shards = calc_distributions(&self.weights)?;
                debug_assert_eq!(self.weights.len(), self.num_shards);
                debug_assert_eq!(self.shards.len(), self.num_shards);
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            let num_nodes = self.num_nodes;
            let changed = self.weights.iter_mut().any(|weights| {
                debug_assert_eq!(weights.len(), num_nodes);
                f(&mut weights[node_id.0])
            });
            if changed {
                self.shards = calc_distributions(&self.weights)?;
                debug_assert_eq!(self.weights.len(), self.num_shards);
                debug_assert_eq!(self.shards.len(), self.num_shards);
            }
            Ok(changed)
        }
    }

    fn select_node_from(&self, distr: &WeightedAliasIndex<f32>) -> NodeId {
        let mut rng = self.rng.borrow_mut();
        NodeId::from(distr.sample(&mut *rng))
    }

    pub(crate) fn select_node(&self, shard_id: ShardId) -> NodeId {
        self.select_node_from(self.shards.get(shard_id.0).expect("shard ID out of bounds"))
    }
}

impl Dispatch for ProbabilisticDispatcher {
    fn dispatch(
        &self,
        query_id: QueryId,
        shards: &[ShardId],
        state: &State,
    ) -> Vec<(ShardId, NodeId)> {
        if let Some(load) = &self.load {
            let shard_times = (0..self.num_shards)
                .map(|shard_id| load.query_time(query_id, ShardId(shard_id)))
                .collect::<Vec<_>>();
            let min_time = shard_times.iter().min().copied().unwrap_or(0) as f32;
            let corrections: Vec<_> = if min_time == 0.0 {
                std::iter::repeat(1.0).take(self.num_shards).collect()
            } else {
                shard_times.iter().map(|&t| t as f32 / min_time).collect()
            };
            let machine_weights = load.machine_weights(state);
            let queue_lengths = load.queue_lengths(state);
            shards
                .iter()
                .map(|&s| {
                    let weights = self
                        .weights
                        .get(s.0)
                        .expect("shard ID out of bounds")
                        .iter()
                        .map(Weight::value)
                        .zip(&machine_weights)
                        .map(|(a, b)| a / (*b + 1.0))
                        // .zip(&corrections)
                        //.map(|(a, b)| a / b)
                        .collect::<Vec<_>>();
                    let distr = WeightedAliasIndex::new(weights.clone()).unwrap_or_else(|_| {
                        panic!(
                            "unable to calculate node weight distribution: {:?}\n{:?}",
                            weights, corrections
                        )
                    });
                    (s, self.select_node_from(&distr))
                })
                .collect()
        } else {
            shards.iter().map(|&s| (s, self.select_node(s))).collect()
        }
    }

    fn num_nodes(&self) -> usize {
        self.num_nodes
    }

    fn num_shards(&self) -> usize {
        self.num_shards
    }

    #[allow(clippy::float_cmp)] // n is always either 0.0 or 1.0
    fn disable_node(&mut self, node_id: NodeId) -> Result<bool> {
        self.change_weight_status(node_id, Weight::disable, |n| n == 1.0)
            .wrap_err_with(|| {
                format!(
                    "unable to disable node {} (#nodes: {}; #shards: {}) with the following weights: {}",
                    node_id,
                    self.num_nodes(),
                    self.num_shards(),
                    format_weights(&self.weights[node_id.0]),
                )
            })
    }

    fn enable_node(&mut self, node_id: NodeId) -> bool {
        let msg = "unable to enable node";
        self.change_weight_status(node_id, Weight::enable, |n| n == 0.0)
            .wrap_err(msg)
            .unwrap_or_else(|e| {
                log::error!("{:#}", e);
                panic!(msg);
            })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_probabilistic_dispatcher() -> Result<()> {
        let weight_matrix = ndarray::arr2(&[[0.5, 0.5, 0.0], [0.0, 0.5, 0.5], [0.5, 0.0, 0.5]]);
        let mut dispatcher = ProbabilisticDispatcher::adaptive(weight_matrix.clone())?;
        assert_eq!(
            weight_matrix,
            dispatcher.weight_matrix.as_ref().unwrap().weights()
        );
        assert!(dispatcher.disable_node(NodeId::from(0))?);
        assert_eq!(
            ndarray::arr2(&[[0.0, 0.0, 0.0], [0.0, 0.5, 0.5], [0.5, 0.0, 0.5]]),
            dispatcher.weight_matrix.as_ref().unwrap().weights()
        );
        assert!(dispatcher.enable_node(NodeId::from(0)));
        assert_eq!(
            weight_matrix,
            dispatcher.weight_matrix.as_ref().unwrap().weights()
        );
        Ok(())
    }
}
