use std::cell::RefCell;

use super::{Dispatch, NodeId, ShardId};
use ndarray::ArrayView2;
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
use rand_distr::weighted_alias::WeightedAliasIndex;
use rand_distr::Distribution;

/// Dispatches according to given probabilities.
pub struct ProbabilisticDispatcher {
    shards: Vec<WeightedAliasIndex<f32>>,
    num_nodes: usize,
    rng: RefCell<ChaChaRng>,
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
        let shards: Vec<_> = probabilities
            .gencolumns()
            .into_iter()
            .map(|probabilities| {
                WeightedAliasIndex::new(probabilities.into_iter().copied().collect())
                    .expect("invalid probabilities")
            })
            .collect();
        Self {
            num_nodes,
            rng: RefCell::new(rng),
            shards,
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
}
