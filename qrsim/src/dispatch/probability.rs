use std::cell::RefCell;

use super::{Dispatch, NodeId, ShardId};
use ndarray::{Array1, Array2, ArrayView2};
use optimization::Optimizer;
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
use rand_distr::weighted_alias::WeightedAliasIndex;
use rand_distr::Distribution;

struct Weight {
    value: f32,
    multiplier: f32,
}

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
}

/// Dispatches according to given probabilities.
pub struct ProbabilisticDispatcher {
    shards: Vec<WeightedAliasIndex<f32>>,
    weights: Vec<Vec<Weight>>,
    num_nodes: usize,
    rng: RefCell<ChaChaRng>,
    weight_matrix: Option<WeightMatrix>,
}

fn calc_distributions(weights: &[Vec<Weight>]) -> Vec<WeightedAliasIndex<f32>> {
    weights
        .iter()
        .map(|weights| {
            WeightedAliasIndex::new(weights.into_iter().map(Weight::value).collect())
                .expect("invalid probabilities")
        })
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
    #[must_use]
    pub fn new(probabilities: ArrayView2<'_, f32>) -> Self {
        Self::with_rng(probabilities, ChaChaRng::from_entropy())
    }

    /// Constructs a new probabilistic dispatcher from the given dispatch matrix,
    /// and initializes the internal PRNG from the given seed.
    #[must_use]
    pub fn with_seed(probabilities: ArrayView2<'_, f32>, seed: u64) -> Self {
        Self::with_rng(probabilities, ChaChaRng::seed_from_u64(seed))
    }

    /// Constructs a new probabilistic dispatcher from the given dispatch matrix,
    /// and initializes the internal PRNG from the given seed.
    #[must_use]
    pub fn with_rng(probabilities: ArrayView2<'_, f32>, rng: ChaChaRng) -> Self {
        let num_nodes = probabilities.nrows();
        let weights: Vec<_> = probabilities_to_weights(probabilities);
        Self {
            num_nodes,
            rng: RefCell::new(rng),
            shards: calc_distributions(&weights),
            weights,
            weight_matrix: None,
        }
    }

    /// Constructs a new dispatcher that optimizes probabilities based on the given weight matrix,
    /// and then repeats the optimization process each time there is a change indicated, such as
    /// disabling or enabling a node.
    pub fn adaptive(weight_matrix: Array2<f32>) -> Self {
        Self::adaptive_with_rng(weight_matrix, ChaChaRng::from_entropy())
    }

    /// Constructs a new adaptive dispatcher with a random seed. See [`Self::adaptive`].
    pub fn adaptive_with_seed(weight_matrix: Array2<f32>, seed: u64) -> Self {
        Self::adaptive_with_rng(weight_matrix, ChaChaRng::seed_from_u64(seed))
    }

    /// Constructs a new adaptive dispatcher with a PRNG. See [`Self::adaptive`].
    pub fn adaptive_with_rng(weight_matrix: Array2<f32>, rng: ChaChaRng) -> Self {
        let probabilities = optimization::LpOptimizer.optimize(weight_matrix.view());
        let mut dispatcher = Self::with_rng(probabilities.view(), rng);
        dispatcher.weight_matrix = Some(WeightMatrix::new(weight_matrix));
        dispatcher
    }

    fn change_weight_status<F, G>(&mut self, node_id: NodeId, f: F, cond: G) -> bool
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
                self.shards = calc_distributions(&self.weights);
                true
            } else {
                false
            }
        } else {
            let changed = self
                .weights
                .iter_mut()
                .any(|weights| f(&mut weights[node_id.0]));
            if changed {
                self.shards = calc_distributions(&self.weights);
            }
            changed
        }
    }
}

impl Dispatch for ProbabilisticDispatcher {
    fn dispatch(&self, shards: &[ShardId]) -> Vec<(ShardId, NodeId)> {
        shards
            .iter()
            .map(|s| {
                let distr = self.shards.get(s.0).expect("shard ID out of bounds");
                let mut rng = self.rng.borrow_mut();
                (*s, NodeId::from(distr.sample(&mut *rng)))
            })
            .collect()
    }

    fn num_nodes(&self) -> usize {
        self.num_nodes
    }

    fn num_shards(&self) -> usize {
        self.shards.len()
    }

    fn disable_node(&mut self, node_id: NodeId) -> bool {
        self.change_weight_status(node_id, Weight::disable, |n| n == 1.0)
    }

    fn enable_node(&mut self, node_id: NodeId) -> bool {
        self.change_weight_status(node_id, Weight::enable, |n| n == 0.0)
    }
}
