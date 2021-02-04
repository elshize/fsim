use std::cell::RefCell;
use std::collections::HashSet;

use super::{Dispatch, NodeId, ShardId, State};
use itertools::Itertools;

struct RoundRobinShard {
    assigned_nodes: Vec<NodeId>,
    next_node: usize,
}

/// Dispatches in a round-robin fashion.
pub struct RoundRobinDispatcher {
    shards: Vec<RefCell<RoundRobinShard>>,
    num_nodes: usize,
    disabled_nodes: HashSet<NodeId>,
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
            .map(|(_, assigned_nodes)| {
                RefCell::new(RoundRobinShard {
                    assigned_nodes,
                    next_node: 0,
                })
            })
            .collect();
        Self {
            shards,
            num_nodes: nodes.len(),
            disabled_nodes: HashSet::new(),
        }
    }

    fn next_node(&self, shard_id: ShardId) -> NodeId {
        let mut shard = self
            .shards
            .get(shard_id.0)
            .expect("shard ID out of bounds")
            .borrow_mut();
        let num_nodes = shard.assigned_nodes.len();
        let mut node_id = shard.assigned_nodes[shard.next_node];
        while self.disabled_nodes.contains(&node_id) {
            shard.next_node = (shard.next_node + 1) % num_nodes;
            node_id = shard.assigned_nodes[shard.next_node];
        }
        shard.next_node = (shard.next_node + 1) % num_nodes;
        node_id
    }
}

impl Dispatch for RoundRobinDispatcher {
    fn dispatch(&self, shards: &[ShardId], _: &State) -> Vec<(ShardId, NodeId)> {
        shards.iter().map(|&s| (s, self.next_node(s))).collect()
    }
    fn num_nodes(&self) -> usize {
        self.num_nodes
    }
    fn num_shards(&self) -> usize {
        self.shards.len()
    }
    fn disable_node(&mut self, node_id: NodeId) -> eyre::Result<bool> {
        Ok(self.disabled_nodes.insert(node_id))
    }
    fn enable_node(&mut self, node_id: NodeId) -> bool {
        self.disabled_nodes.remove(&node_id)
    }
}
