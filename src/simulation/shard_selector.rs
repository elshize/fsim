//! Shard selector is an object that decides, given a query (and possibly other information),
//! which shards to route it to. Note that this **does not** select specific replicas but only
//! shards, so an exhaustive policy will always return all possible shard IDs.

use super::{Query, ShardId};
use std::time::Duration;

/// Shard selection result.
pub struct ShardSelection<'a> {
    /// Time taken to select shards.
    pub time: Duration,
    /// Selected shards.
    pub shards: Shards<'a>,
}

/// Result of shard selection.
pub struct Shards<'a>(Box<dyn Iterator<Item = ShardId> + 'a>);

impl<'a> Iterator for Shards<'a> {
    type Item = ShardId;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl<'a> Shards<'a> {
    /// Type erases passed iterator and returns as `Shards`.
    #[must_use]
    pub fn new<I>(iter: I) -> Self
    where
        I: Iterator<Item = ShardId> + 'a,
    {
        Self(Box::new(iter))
    }
}

/// **Important Note**: this interface will likely evolve to depend on more dynamic information.
pub trait ShardSelector {
    /// Returns shard selection time and selected shard IDs.
    fn select(&self, query: Query) -> ShardSelection;
}

/// Selector that always selects all shards. It also returns 0 as selection time, since it is
/// trivial to do so.
pub struct ExhaustiveSelector {
    num_shards: usize,
}

impl ExhaustiveSelector {
    /// Constructs an exhaustive selector.
    pub fn new(num_shards: usize) -> Self {
        Self { num_shards }
    }
}

impl ShardSelector for ExhaustiveSelector {
    fn select(&self, _: Query) -> ShardSelection {
        log::trace!("Selected {} shards", self.num_shards);
        ShardSelection {
            time: Duration::new(0, 0),
            shards: Shards::new((0..self.num_shards).map(ShardId)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::simulation::{QueryId, RequestId};
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_exhaustive_selector(num_shards in 0..100_usize, query_id in 0..100_usize) {
            let selector = ExhaustiveSelector::new(num_shards);
            let query = Query {id: QueryId(query_id), request: RequestId(0)};
            let selection = selector.select(query);
            let expected = ShardSelection {
                time: Duration::new(0, 0),
                shards: Shards::new((0..num_shards).map(ShardId))
            };
            assert_eq!(selection.time, expected.time);
            assert_eq!(
                selection.shards.collect::<Vec<_>>(),
                expected.shards.collect::<Vec<_>>(),
            );
        }
    }
}
