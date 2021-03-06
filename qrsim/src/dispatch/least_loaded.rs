use crate::{
    Dispatch, NodeId, NodeQueue, NodeQueueEntry, NodeStatus, NodeThreadPool, QueryEstimate,
    QueryId, ShardId,
};

use std::collections::HashSet;
use std::rc::Rc;

use ordered_float::OrderedFloat;
use simrs::{Key, QueueId, State};

/// Always selects the node with the least load waiting in the queue.
pub struct LeastLoadedDispatch {
    node_queues: Vec<QueueId<NodeQueue<NodeQueueEntry>>>,
    node_weights: Vec<f32>,
    shards: Vec<Vec<NodeId>>,
    disabled_nodes: HashSet<NodeId>,
    estimates: Rc<Vec<QueryEstimate>>,
    thread_pools: Vec<Key<NodeThreadPool>>,
}

impl LeastLoadedDispatch {
    /// Constructs a new dispatcher.
    #[must_use]
    pub fn new(
        nodes: &[Vec<usize>],
        node_queues: Vec<QueueId<NodeQueue<NodeQueueEntry>>>,
        estimates: Rc<Vec<QueryEstimate>>,
        thread_pools: Vec<Key<NodeThreadPool>>,
    ) -> Self {
        let node_weights = vec![1.0; node_queues.len()];
        Self {
            shards: super::invert_nodes_to_shards(nodes),
            disabled_nodes: HashSet::new(),
            node_queues,
            estimates,
            thread_pools,
            node_weights,
        }
    }

    fn query_time(&self, query_id: QueryId, shard_id: ShardId) -> u64 {
        self.estimates
            .get(query_id.0 - 1)
            .expect("query out of bounds")
            .shard_estimate(shard_id)
    }

    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    fn select_node(&self, shard_id: ShardId, state: &State) -> NodeId {
        *self.shards[shard_id.0]
            .iter()
            .zip(&self.node_weights)
            .filter(|(n, _)| !self.disabled_nodes.contains(n))
            .min_by_key(|(n, w)| {
                let running = state
                    .get(self.thread_pools[n.0])
                    .expect("unknown thread pool ID")
                    .running_threads()
                    .iter()
                    .map(|t| t.estimated.as_micros())
                    .sum::<u128>();
                let waiting = state
                    .queue(self.node_queues[n.0])
                    .iter()
                    .map(|msg| self.query_time(msg.request.query_id(), msg.request.shard_id()))
                    .sum::<u64>();
                OrderedFloat((running as u64 + waiting) as f32 / **w)
            })
            .unwrap()
            .0
    }
}

impl Dispatch for LeastLoadedDispatch {
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
