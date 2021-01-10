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
    /// Disablese `node_id` and stops routing there. Returns the `true` if the node was not
    /// disabled before.
    fn disable_node(&mut self, node_id: NodeId) -> eyre::Result<bool>;
    /// Enables `node_id` if it was previously disabled. Returns `true` if the node was disabled.
    fn enable_node(&mut self, node_id: NodeId) -> bool;
}
