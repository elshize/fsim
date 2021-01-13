use super::{Dispatch, NodeId, ShardId};

use std::cell::RefCell;

use eyre::{Result, WrapErr};
use ndarray::{Array1, Array2, ArrayView2};
use optimization::Optimizer;
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
use rand_distr::weighted_alias::WeightedAliasIndex;
use rand_distr::Distribution;

#[derive(Debug)]
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
}

/// Dispatches according to given probabilities.
pub struct ProbabilisticDispatcher {
    shards: Vec<WeightedAliasIndex<f32>>,
    weights: Vec<Vec<Weight>>,
    num_nodes: usize,
    num_shards: usize,
    rng: RefCell<ChaChaRng>,
    weight_matrix: Option<WeightMatrix>,
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
        .map(|weights| WeightedAliasIndex::new(weights.into_iter().map(Weight::value).collect()))
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
    pub fn new(probabilities: ArrayView2<'_, f32>) -> Result<Self> {
        Self::with_rng(probabilities, ChaChaRng::from_entropy())
    }

    /// Constructs a new probabilistic dispatcher from the given dispatch matrix,
    /// and initializes the internal PRNG from the given seed.
    #[must_use]
    pub fn with_seed(probabilities: ArrayView2<'_, f32>, seed: u64) -> Result<Self> {
        Self::with_rng(probabilities, ChaChaRng::seed_from_u64(seed))
    }

    /// Constructs a new probabilistic dispatcher from the given dispatch matrix,
    /// and initializes the internal PRNG from the given seed.
    #[must_use]
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
        })
    }

    /// Constructs a new dispatcher that optimizes probabilities based on the given weight matrix,
    /// and then repeats the optimization process each time there is a change indicated, such as
    /// disabling or enabling a node.
    pub fn adaptive(weight_matrix: Array2<f32>) -> Result<Self> {
        Self::adaptive_with_rng(weight_matrix, ChaChaRng::from_entropy())
    }

    /// Constructs a new adaptive dispatcher with a random seed. See [`Self::adaptive`].
    pub fn adaptive_with_seed(weight_matrix: Array2<f32>, seed: u64) -> Result<Self> {
        Self::adaptive_with_rng(weight_matrix, ChaChaRng::seed_from_u64(seed))
    }

    /// Constructs a new adaptive dispatcher with a PRNG. See [`Self::adaptive`].
    pub fn adaptive_with_rng(weight_matrix: Array2<f32>, rng: ChaChaRng) -> Result<Self> {
        let probabilities = optimization::LpOptimizer.optimize(weight_matrix.view());
        let mut dispatcher = Self::with_rng(probabilities.view(), rng)?;
        dispatcher.weight_matrix = Some(WeightMatrix::new(weight_matrix));
        Ok(dispatcher)
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
        self.num_shards
    }

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
