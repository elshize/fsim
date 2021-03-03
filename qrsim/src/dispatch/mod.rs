use crate::{NodeId, QueryId, ShardId};

use itertools::Itertools;
use simrs::State;

pub mod dummy;
pub mod dynamic;
pub mod least_loaded;
pub mod opt_plus;
pub mod probability;
pub mod round_robin;
pub mod shortest_queue;

/// Implementors are dispatch policies that select nodes for requested shards.
pub trait Dispatch {
    /// Selects a node for each requested shard.
    fn dispatch(
        &self,
        query_id: QueryId,
        shards: &[ShardId],
        state: &State,
    ) -> Vec<(ShardId, NodeId)>;

    /// Total number of existing shards.
    fn num_shards(&self) -> usize;

    /// Total number of existing nodes.
    fn num_nodes(&self) -> usize;

    /// Disablese `node_id` and stops routing there. Returns the `true` if the node was not
    /// disabled before.
    ///
    /// # Errors
    ///
    /// Returns an error if disabling the given node leads to an invalid state.
    fn disable_node(&mut self, node_id: NodeId) -> eyre::Result<bool>;

    /// Enables `node_id` if it was previously disabled. Returns `true` if the node was disabled.
    fn enable_node(&mut self, node_id: NodeId) -> bool;
}

fn invert_nodes_to_shards(nodes: &[Vec<usize>]) -> Vec<Vec<NodeId>> {
    nodes
        .iter()
        .enumerate()
        .flat_map(|(node_id, shards)| {
            shards
                .iter()
                .map(move |shard_id| (*shard_id, NodeId::from(node_id)))
        })
        .into_group_map()
        .into_iter()
        .map(|(_, assigned_nodes)| assigned_nodes)
        .collect()
}
