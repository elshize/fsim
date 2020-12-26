use std::cell::Cell;

use super::{Dispatch, NodeId, ShardId};
use itertools::Itertools;

struct RoundRobinShard {
    assigned_nodes: Vec<NodeId>,
    next_node: Cell<usize>,
}

/// Dispatches in a round-robin fashion.
pub struct RoundRobinDispatcher {
    shards: Vec<RoundRobinShard>,
    num_nodes: usize,
}

impl RoundRobinDispatcher {
    /// Constructs a new round-robin dispatcher from the given node configuration.
    #[must_use]
    pub fn new(nodes: &[Vec<usize>]) -> Self {
        let shards = nodes
            .iter()
            .enumerate()
            .flat_map(|(node_id, shards)| {
                shards
                    .iter()
                    .map(move |shard_id| (*shard_id, NodeId::from(node_id)))
            })
            .into_group_map()
            .into_iter()
            .map(|(_, assigned_nodes)| RoundRobinShard {
                assigned_nodes,
                next_node: Cell::new(0),
            })
            .collect();
        Self {
            shards,
            num_nodes: nodes.len(),
        }
    }
}

impl Dispatch for RoundRobinDispatcher {
    fn dispatch(&self, shards: &[ShardId]) -> Vec<(ShardId, NodeId)> {
        shards
            .iter()
            .map(|s| {
                let shard = self.shards.get(s.0).expect("shard ID out of bounds");
                let next = shard.next_node.get();
                let node_id = shard.assigned_nodes.get(next).expect("internal error");
                shard
                    .next_node
                    .replace((next + 1) % shard.assigned_nodes.len());
                (*s, *node_id)
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
