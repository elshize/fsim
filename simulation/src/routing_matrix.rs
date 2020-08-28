//! Routing matrix defines probabilities of routing a query to a node N given a shard S.

use super::replica_selector::{ReplicaSelection, ReplicaSelector, Replicas};
use super::shard_selector::Shards;
use super::{NodeId, Query, ReplicaId};
use rand::distributions::{uniform::SampleBorrow, Distribution, WeightedIndex};
use rand::SeedableRng;
use std::cell::RefCell;
use std::ops::DerefMut;
use std::time::Duration;

/// Builder for [`RoutingMatrix`](struct.RoutingMatrix.html).
///
/// # Examples
///
/// ```
/// # use simulation::routing_matrix::RoutingMatrixBuilder;
/// # use rand_chacha::ChaChaRng;
/// RoutingMatrixBuilder::<ChaChaRng>::new(vec![
///     vec![0.1, 0.4, 0.5], // Shard 0
///     //   ^^^  ^^^  ^^^
///     //   ^^^  ^^^  Node 2
///     //   ^^^  Node 1
///     //   Node 0
///     vec![0.3, 0.3, 0.4], // Shard 1
///     vec![0.1, 0.9, 0.0]  // Shard 2
///     //             ^^^
///     //             Shard is not assigned to this Node 2
/// ]);
/// ```
pub struct RoutingMatrixBuilder<R> {
    shard_distributions: Vec<WeightedIndex<f32>>,
    selection_time: Option<Duration>,
    rng: Option<R>,
}

impl<R: SeedableRng> RoutingMatrixBuilder<R> {
    /// Constructs a builder with given weights.
    /// See struct-level documentation for examples.
    pub fn new<W, I>(wieghts: W) -> Self
    where
        W: IntoIterator<Item = I>,
        I: IntoIterator,
        I::Item: SampleBorrow<f32>,
    {
        Self {
            shard_distributions: wieghts
                .into_iter()
                .map(|w| WeightedIndex::new(w).unwrap())
                .collect(),
            selection_time: None,
            rng: None,
        }
    }

    /// Set selection time. If not defined, it will be 0.
    #[must_use]
    pub fn selection_time(mut self, time: Duration) -> Self {
        self.selection_time = Some(time);
        self
    }

    /// Set a random number generator. If not defined, it will be created `from_entropy`.
    #[must_use]
    pub fn rng(mut self, rng: R) -> Self {
        self.rng = Some(rng);
        self
    }

    /// Constructs a [`RoutingMatrix`](struct.RoutingMatrix.html).
    #[must_use]
    pub fn build(self) -> RoutingMatrix<R> {
        RoutingMatrix {
            shard_distributions: self.shard_distributions,
            selection_time: self.selection_time.unwrap_or_default(),
            rng: RefCell::new(self.rng.unwrap_or_else(R::from_entropy)),
        }
    }
}

/// Routing matrix with probabilities of selecting a replica given a shard.
pub struct RoutingMatrix<R> {
    shard_distributions: Vec<WeightedIndex<f32>>,
    selection_time: Duration,
    rng: RefCell<R>,
}

impl<R: SeedableRng + rand::RngCore> ReplicaSelector for RoutingMatrix<R> {
    fn select<'a>(&'a self, _: Query, shards: Shards<'a>) -> ReplicaSelection<'a> {
        let replicas = shards.map(move |shard| {
            let node = NodeId(
                self.shard_distributions
                    .get(usize::from(shard))
                    .expect("Unable to retrieve shard distribution")
                    .sample(self.rng.borrow_mut().deref_mut()),
            );
            ReplicaId { node, shard }
        });
        ReplicaSelection {
            time: self.selection_time,
            replicas: Replicas::new(replicas),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{QueryId, RequestId, ShardId};
    use float_cmp::approx_eq;
    use rand_chacha::ChaChaRng;
    use std::convert::TryFrom;

    fn query() -> Query {
        Query {
            id: QueryId(0),
            request: RequestId(0),
        }
    }

    #[test]
    #[should_panic]
    fn test_routing_matrix_out_of_bounds() {
        let matrix = RoutingMatrixBuilder::new(&vec![
            vec![0.1, 0.9, 0.0],
            vec![0.0, 0.1, 0.9],
            vec![0.1, 0.0, 0.9],
        ])
        .selection_time(Duration::new(0, 1))
        .rng(ChaChaRng::from_entropy())
        .build();
        let _: Vec<_> = matrix
            .select(query(), Shards::new([3].iter().copied().map(ShardId)))
            .replicas
            .collect();
    }

    #[test]
    fn test_routing_matrix() {
        let probabilities = vec![
            vec![0.1, 0.9, 0.0],
            vec![0.0, 0.1, 0.9],
            vec![0.1, 0.0, 0.9],
        ];
        let matrix = RoutingMatrixBuilder::new(&probabilities)
            .selection_time(Duration::new(0, 1))
            .rng(ChaChaRng::from_entropy())
            .build();
        let mut counts = vec![vec![0_u32, 0, 0], vec![0, 0, 0], vec![0, 0, 0]];
        for _ in 0..1_000_000 {
            let selection =
                matrix.select(query(), Shards::new([0, 1, 2].iter().copied().map(ShardId)));
            assert_eq!(selection.time, Duration::new(0, 1));
            for ReplicaId { node, shard } in selection.replicas {
                counts[usize::from(shard)][usize::from(node)] += 1;
            }
        }
        for (shard, shard_counts) in counts.iter().enumerate() {
            let sum: u32 = shard_counts.iter().sum();
            for (node, &node_count) in shard_counts.iter().enumerate() {
                let actual = if sum > 0 {
                    f64::try_from(node_count).unwrap() / f64::try_from(sum).unwrap()
                } else {
                    0.0
                };
                let expected = f64::from(probabilities[shard][node]);
                println!("{} =? {} at {} {}", actual, expected, shard, node);
                assert!(approx_eq!(f64, actual, expected, epsilon = 0.02));
            }
        }
    }
}
