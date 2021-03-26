use crate::{NodeId, NodeStatus, QueryId, ShardId};

use std::time::Duration;

use itertools::Itertools;
use simrs::{Key, State};

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

    /// Recompute the policy based on the current state.
    fn recompute(&mut self, _: &[Key<NodeStatus>], _: &State) {}

    /// Returns the time needed to recompute the policy.
    fn recompute_delay(&self) -> Duration {
        Duration::default()
    }
}

fn invert_nodes_to_shards(nodes: &[Vec<usize>]) -> Vec<Vec<NodeId>> {
    let mut map = nodes
        .iter()
        .enumerate()
        .flat_map(|(node_id, shards)| {
            shards
                .iter()
                .map(move |shard_id| (*shard_id, NodeId::from(node_id)))
        })
        .into_group_map()
        .into_iter()
        .collect_vec();
    map.sort();
    map.into_iter()
        .enumerate()
        .map(|(idx, (shard_id, assigned_nodes))| {
            assert_eq!(idx, shard_id, "missing shard {}", idx);
            assigned_nodes
        })
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_invert_nodes_to_shards() {
        let input = "[
            [20,38,99],
            [52,67,116],
            [7,11,71],
            [27,29,88],
            [41,45,102],
            [37,57,78],
            [2,93,98,103],
            [31,40,84],
            [23,55,111],
            [81,82,109],
             [0,13,86,112],
             [5,26,110],
             [32,51,113],
             [56,73,90],
             [1,97,108],
             [79,92,114],
             [15,77,80],
             [17,22,89],
             [24,76,100],
             [18,54,74],
             [28,70,72,85],
             [42,44,66],
             [21,83,94],
             [65,68,96],
             [12,50,106],
             [34,58,107],
             [39,91,95],
             [53,101,121],
             [3,30,43],
             [35,36,117],
             [48,49,75],
             [6,25,59],
             [4,62,63],
             [104,115,119],
             [46,118,122],
             [8,10,87],
             [14,16,64],
             [33,47,61],
             [19,60,69],
             [9,105,120]]";
        let nodes: Vec<Vec<usize>> = serde_json::from_str(input).unwrap();
        let shards = invert_nodes_to_shards(&nodes);
        assert_eq!(
            &shards[..41],
            [
                10, 14, 6, 28, 32, 11, 31, 2, 35, 39, 35, 2, 24, 10, 36, 16, 36, 17, 19, 38, 0, 22,
                17, 8, 18, 31, 11, 3, 20, 3, 28, 7, 12, 37, 25, 29, 29, 5, 0, 26, 7
            ]
            .iter()
            .copied()
            .map(|node| vec![NodeId(node)])
            .collect_vec()
        );
    }
}
