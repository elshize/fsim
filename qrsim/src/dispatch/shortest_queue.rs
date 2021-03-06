use super::{Dispatch, NodeId, QueryId, ShardId, State};
use crate::{NodeQueue, NodeQueueEntry, NodeStatus, NodeThreadPool};

use std::collections::HashSet;

use ordered_float::OrderedFloat;
use simrs::{Key, QueueId};

/// Always selects the node with the fewer requests in the queue.
pub struct ShortestQueueDispatch {
    node_queues: Vec<QueueId<NodeQueue<NodeQueueEntry>>>,
    node_weights: Vec<f32>,
    shards: Vec<Vec<NodeId>>,
    disabled_nodes: HashSet<NodeId>,
    thread_pools: Vec<Key<NodeThreadPool>>,
}

impl ShortestQueueDispatch {
    /// Constructs a new dispatcher.
    #[must_use]
    pub fn new(
        nodes: &[Vec<usize>],
        node_queues: Vec<QueueId<NodeQueue<NodeQueueEntry>>>,
        thread_pools: Vec<Key<NodeThreadPool>>,
    ) -> Self {
        let node_weights = vec![1.0; node_queues.len()];
        Self {
            shards: super::invert_nodes_to_shards(nodes),
            disabled_nodes: HashSet::new(),
            node_queues,
            thread_pools,
            node_weights,
        }
    }

    fn select_node(&self, shard_id: ShardId, state: &State) -> NodeId {
        *self.shards[shard_id.0]
            .iter()
            .zip(&self.node_weights)
            .filter(|(n, _)| !self.disabled_nodes.contains(n))
            .min_by_key(|(n, w)| {
                let queue_len = state.len(self.node_queues[n.0]);
                let active = state
                    .get(self.thread_pools[n.0])
                    .expect("unknown thread pool ID")
                    .num_active();
                OrderedFloat((queue_len + active) as f32 / **w)
            })
            .unwrap()
            .0
    }
}

impl Dispatch for ShortestQueueDispatch {
    fn dispatch(&self, _: QueryId, shards: &[ShardId], state: &State) -> Vec<(ShardId, NodeId)> {
        shards
            .iter()
            .map(|&shard_id| (shard_id, self.select_node(shard_id, &state)))
            .collect()
    }

    fn num_shards(&self) -> usize {
        self.shards.len()
    }

    fn num_nodes(&self) -> usize {
        self.node_queues.len()
    }

    fn disable_node(&mut self, node_id: NodeId) -> eyre::Result<bool> {
        Ok(self.disabled_nodes.insert(node_id))
    }

    fn enable_node(&mut self, node_id: NodeId) -> bool {
        self.disabled_nodes.remove(&node_id)
    }

    fn recompute(&mut self, node_statuses: &[Key<NodeStatus>], state: &State) {
        self.node_weights = node_statuses
            .iter()
            .copied()
            .map(|key| match state.get(key).expect("missing node status") {
                NodeStatus::Healthy => 1.0,
                NodeStatus::Injured(i) => *i,
                NodeStatus::Unresponsive => 0.0,
            })
            .collect();
    }
}
