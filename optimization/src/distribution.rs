//! Probabilistic distributions.

use rand::distributions::Distribution;
use rand::Rng;
use rand_distr::weighted::WeightedIndex;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

/// A distribution used for random assignments.
pub struct AssignmentDistribution<T: Hash> {
    weights: HashMap<T, f32>,
    min: f32,
}

impl<T: Hash + Eq + Copy> AssignmentDistribution<T> {
    /// Constructs new distribution.
    #[must_use]
    pub fn new(weights: HashMap<T, f32>) -> Self {
        Self { weights, min: 0.0 }
    }

    /// Sample single value.
    pub fn sample<R: Rng + ?Sized>(&mut self, rng: &mut R, cost: f32, blacklist: &HashSet<T>) -> T {
        let mut resources: Vec<_> = self
            .weights
            .iter_mut()
            .filter(|(value, _)| !blacklist.contains(value))
            .collect();
        let shift = self.min;
        let dist = WeightedIndex::new(resources.iter().map(|(_, w)| **w - shift))
            .expect("Distinct assignment infeasible");
        let (elem, weight) = &mut resources[dist.sample(rng)];
        **weight -= cost;
        **elem
    }
}

impl<T: Hash> Distribution<T> for AssignmentDistribution<T> {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> T {
        let mut resources: Vec<_> = self
            .weights
            .iter_mut()
            .filter(|(value, _)| !blacklist.contains(value))
            .collect();
        let shift = self.min;
        let dist = WeightedIndex::new(resources.iter().map(|(_, w)| **w - shift))
            .expect("Distinct assignment infeasible");
        let (elem, weight) = &mut resources[dist.sample(rng)];
        **weight -= cost;
        **elem
    }
}

struct FilteredWeightedIndex<T> {
    weights: HashMap<T, f32>,
    ignore: HashSet<T>,
}

// #[cfg(test)]
// mod test {
//     use super::*;
//     use proptest::prelude::*;
//     use rand::SeedableRng;
//     use rand_chacha::ChaChaRng;

//     proptest! {
//         #[test]
//         fn test_assignment_distribution(
//             seed: u64,
//             weights in proptest::collection::vec(proptest::num::f32::POSITIVE, 10..20)
//         ) {
//             let replicas = 3;
//             let distr = AssignmentDistribution::new(weights.into_iter().enumerate().collect());
//             let mut rng = ChaChaRng::seed_from_u64(seed);
//             distr.sample(&mut rng);
//         }
//     }
// }

// impl<A: Hash + Eq + Copy> std::iter::FromIterator<A> for AssignmentDistribution<A> {
//     fn from_iter<T>(iter: T) -> Self
//     where
//         T: IntoIterator<Item = A>,
//     {
//         let mut counts: HashMap<A, usize> = HashMap::default();
//         for r in iter {
//             *counts.entry(r).or_default() += 1;
//         }
//         Self { counts }
//     }
// }
