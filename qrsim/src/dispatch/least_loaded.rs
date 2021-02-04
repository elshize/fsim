use super::{Dispatch, NodeId, ShardId, State};
use crate::{NodeQueue, NodeQueueEntry, Query, QueryId};
use simrs::QueueId;
use std::collections::HashSet;
use std::rc::Rc;

/// Always selects the node with the least load waiting in the queue.
pub struct LeastLoadedDispatch {
    node_queues: Vec<QueueId<NodeQueue<NodeQueueEntry>>>,
    shards: Vec<Vec<NodeId>>,
    disabled_nodes: HashSet<NodeId>,
    queries: Rc<Vec<Query>>,
}

impl LeastLoadedDispatch {
    /// Constructs a new dispatcher.
    #[must_use]
    pub fn new(
        nodes: &[Vec<usize>],
        node_queues: Vec<QueueId<NodeQueue<NodeQueueEntry>>>,
        queries: Rc<Vec<Query>>,
    ) -> Self {
        Self {
            shards: super::invert_nodes_to_shards(nodes),
            disabled_nodes: HashSet::new(),
            node_queues,
            queries,
        }
    }

    fn query_time(&self, query_id: QueryId, shard_id: ShardId) -> u64 {
        self.queries
            .get(query_id.0)
            .expect("query out of bounds")
            .retrieval_times[shard_id.0]
    }

    fn select_node(&self, shard_id: ShardId, state: &State) -> NodeId {
        *self.shards[shard_id.0]
            .iter()
            .filter(|n| !self.disabled_nodes.contains(n))
            .min_by_key(|n| {
                state
                    .queue(self.node_queues[n.0])
                    .iter()
                    .map(|msg| self.query_time(msg.request.query_id(), shard_id))
                    .sum::<u64>()
            })
            .unwrap()
    }
}

impl Dispatch for LeastLoadedDispatch {
    fn dispatch(&self, shards: &[ShardId], state: &State) -> Vec<(ShardId, NodeId)> {
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
}
