//! Replica selector is an object that decides, given a list of shards,
//! which replicas to route a query to.

use crate::shard_selector::Shards;
use crate::{NodeId, Query, ReplicaId};
use rand::distributions::{uniform::SampleUniform, Distribution, Uniform};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaChaRng;
use std::cell::RefCell;
use std::time::Duration;

/// Trait for a distribution that can be constructed from a range of values.
pub trait RangeDistribution<T> {
    /// Constructs a new uniform distribution selecting values between `T::default()` and `high`.
    fn new(high: T) -> Self;
}

impl<T: SampleUniform + Default> RangeDistribution<T> for Uniform<T> {
    fn new(high: T) -> Self {
        Uniform::new(T::default(), high)
    }
}

/// Result of shard selection.
pub struct Replicas<'a>(Box<dyn Iterator<Item = ReplicaId> + 'a>);

impl<'a> Iterator for Replicas<'a> {
    type Item = ReplicaId;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl<'a> Replicas<'a> {
    /// Type erases passed iterator and returns as `Replicas`.
    #[must_use]
    pub fn new<I>(iter: I) -> Self
    where
        I: Iterator<Item = ReplicaId> + 'a,
    {
        Self(Box::new(iter))
    }
}

/// Replica selection result.
pub struct ReplicaSelection<'a> {
    /// Time taken to select replicas.
    pub time: Duration,
    /// Selected replicas.
    pub replicas: Replicas<'a>,
}

/// **Important Note**: this interface will likely evolve to depend on more dynamic information.
pub trait ReplicaSelector {
    /// Returns replica selection time and selected replica IDs.
    fn select<'a>(&'a self, query: Query, shards: Shards<'a>) -> ReplicaSelection<'a>;
}

/// Selects replicas at random.
pub struct RandomReplicaSelector<D: Distribution<usize> + RangeDistribution<usize> = Uniform<usize>>
{
    replica_distributions: Vec<D>,
    rng: RefCell<Box<dyn RngCore>>,
}

impl<D: Distribution<usize> + RangeDistribution<usize>> RandomReplicaSelector<D> {
    /// Constructs a random replica selector from a vector containing the number replicas
    /// for each shard.
    #[must_use]
    pub fn new(replica_counts: &[usize]) -> Self {
        Self {
            replica_distributions: replica_counts.iter().map(|&c| D::new(c)).collect(),
            rng: RefCell::new(Box::new(ChaChaRng::seed_from_u64(
                rand::thread_rng().next_u64(),
            ))),
        }
    }

    /// Constructs a random replica selector from a vector containing the number replicas
    /// for each shard.
    pub fn with_prng<R: RngCore + 'static>(replica_counts: &[usize], rng: R) -> Self {
        Self {
            replica_distributions: replica_counts.iter().map(|&c| D::new(c)).collect(),
            rng: RefCell::new(Box::new(rng)),
        }
    }
}

impl<D: Distribution<usize> + RangeDistribution<usize>> ReplicaSelector
    for RandomReplicaSelector<D>
{
    fn select<'a>(&'a self, _: Query, shards: Shards<'a>) -> ReplicaSelection<'a> {
        ReplicaSelection {
            time: Duration::new(0, 0),
            replicas: Replicas::new(shards.map(move |shard| {
                let node: usize = self
                    .replica_distributions
                    .get(usize::from(shard))
                    .unwrap_or_else(|| panic!("Missing replica distribution for shard {}", shard))
                    .sample(self.rng.borrow_mut().as_mut());
                ReplicaId {
                    node: NodeId(node),
                    shard,
                }
            })),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::WrappingEchoDistribution;
    use crate::{QueryId, RequestId, ShardId};
    use float_cmp::approx_eq;
    use proptest::prelude::*;
    use std::convert::TryFrom;

    impl<T: SampleUniform + Default> RangeDistribution<T> for WrappingEchoDistribution<T> {
        fn new(high: T) -> Self {
            WrappingEchoDistribution::new(high)
        }
    }

    fn query() -> Query {
        Query {
            id: QueryId(0),
            request: RequestId(0),
        }
    }

    proptest! {
        #[test]
        fn test_random_selector(
            (replica_counts, requested) in proptest::collection::vec(1..10_usize, 50..100)
                .prop_flat_map(|v| {
                    let indices: Vec<usize> = (0..v.len()).collect();
                    let subseq = proptest::sample::subsequence(indices, 0..v.len());
                    (Just(v), subseq)
                })
        ) {
            let selector: RandomReplicaSelector<WrappingEchoDistribution<usize>> =
                RandomReplicaSelector::with_prng(
                    &replica_counts,
                    rand::rngs::mock::StepRng::new(0, 1)
                );
            let requested: Vec<_> = requested.into_iter().map(ShardId).collect();
            let expected: Vec<_> = requested.iter().enumerate().map(|(counter, &shard)| {
                ReplicaId {
                    node: NodeId(counter % replica_counts[usize::from(shard)]),
                    shard
                }
            }).collect();
            let selection = selector.select(
                query(),
                Shards::new(requested.iter().copied())
            );
            assert_eq!(selection.time, Duration::new(0, 0));
            assert_eq!(selection.replicas.collect::<Vec<_>>(), expected);
        }
    }

    #[test]
    fn test_random_selector_with_stats() {
        let selector = RandomReplicaSelector::<Uniform<usize>>::new(&[3, 3, 3]);
        let mut counts = vec![vec![0_u32, 0, 0], vec![0, 0, 0], vec![0, 0, 0]];
        for _ in 0..100_000 {
            let selection =
                selector.select(query(), Shards::new([0, 1, 2].iter().copied().map(ShardId)));
            assert_eq!(selection.time, Duration::new(0, 0));
            for ReplicaId { node, shard } in selection.replicas {
                counts[usize::from(shard)][usize::from(node)] += 1;
            }
        }
        for shard_counts in counts.iter() {
            let sum: u32 = shard_counts.iter().sum();
            for &node_count in shard_counts.iter() {
                let actual = if sum > 0 {
                    f64::try_from(node_count).unwrap() / f64::try_from(sum).unwrap()
                } else {
                    0.0
                };
                assert!(approx_eq!(f64, actual, 0.333333, epsilon = 0.02));
            }
        }
    }
}
