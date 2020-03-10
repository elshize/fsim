use crate::replica_selector::{ReplicaSelection, ReplicaSelector, Replicas};
use crate::shard_selector::{ShardSelection, ShardSelector, Shards};
use crate::{Query, ReplicaId, ShardId};

/// This is a container for all immutable information about the simulation that
/// is typically loaded from a file and describes characteristics such as:
/// query times, shard rankings, selection times, etc.
pub struct Data {
    shard_selections: Vec<(u64, Vec<ShardId>)>,
    replica_selections: Vec<(u64, Vec<ReplicaId>)>,
}

impl<'a> Data {
    /// Constructs the data object with all information passed in directly.
    #[must_use]
    pub fn new(
        shard_selections: Vec<(u64, Vec<ShardId>)>,
        replica_selections: Vec<(u64, Vec<ReplicaId>)>,
    ) -> Self {
        Self {
            shard_selections,
            replica_selections,
        }
    }
}

impl ShardSelector for Data {
    fn select(&self, query: Query) -> ShardSelection {
        let (time, shards) = self
            .shard_selections
            .get(query.id())
            .unwrap_or_else(|| panic!("Unable to retrieve shard selections for query {:?}", query));
        ShardSelection {
            time: *time,
            shards: Shards::new(shards.iter().copied()),
        }
    }
}

impl ReplicaSelector for Data {
    fn select(&self, query: Query, shards: Shards) -> ReplicaSelection {
        let (time, replicas) = self
            .replica_selections
            .get(query.id())
            .unwrap_or_else(|| panic!("Unable to retrieve shard selections for query {:?}", query));
        ReplicaSelection {
            time: *time,
            replicas: Replicas::new(replicas.iter().copied()),
        }
    }
}
