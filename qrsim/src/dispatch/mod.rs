use crate::{NodeId, ShardId};

pub mod dummy;
pub mod probability;
pub mod round_robin;

/// Implementors are dispatch policies that select nodes for requested shards.
pub trait Dispatch {
    /// Selects a node for each requested shard.
    fn dispatch(&self, shards: &[ShardId]) -> Vec<(ShardId, NodeId)>;
    /// Total number of existing shards.
    fn num_shards(&self) -> usize;
    /// Total number of existing nodes.
    fn num_nodes(&self) -> usize;
}
