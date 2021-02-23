use super::{Dispatch, NodeId, ShardId, State};
use crate::{
    NodeQueue, NodeQueueEntry, NodeThreadPool, ProbabilisticDispatcher, Query, QueryEstimate,
    QueryId,
};

use std::collections::HashSet;
use std::rc::Rc;

use simrs::{Key, QueueId};

pub struct OptPlusDispatch {
    node_queues: Vec<QueueId<NodeQueue<NodeQueueEntry>>>,
    shards: Vec<Vec<NodeId>>,
    disabled_nodes: HashSet<NodeId>,
    estimates: Rc<Vec<QueryEstimate>>,
    queries: Rc<Vec<Query>>,
    thread_pools: Vec<Key<NodeThreadPool>>,
    probabilistic: ProbabilisticDispatcher,
    threshold: f32,
}

impl OptPlusDispatch {
    /// Constructs a new dispatcher.
    #[must_use]
    pub fn new(
        nodes: &[Vec<usize>],
        node_queues: Vec<QueueId<NodeQueue<NodeQueueEntry>>>,
        estimates: Rc<Vec<QueryEstimate>>,
        queries: Rc<Vec<Query>>,
        thread_pools: Vec<Key<NodeThreadPool>>,
        probabilistic: ProbabilisticDispatcher,
        threshold: f32,
    ) -> Self {
        Self {
            shards: super::invert_nodes_to_shards(nodes),
            disabled_nodes: HashSet::new(),
            node_queues,
            estimates,
            queries,
            thread_pools,
            probabilistic,
            threshold,
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
            .filter(|n| !self.disabled_nodes.contains(n))
            .min_by_key(|n| {
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
                    .map(|msg| self.query_time(msg.request.query_id(), shard_id))
                    .sum::<u64>();
                //waiting
                running as u64 + waiting
            })
            .unwrap()
    }

    fn any_over_threshold(&self, shard_id: ShardId, state: &State) -> bool {
        use simrs::Queue;
        let nodes = &self.shards[shard_id.0];
        let all_queues_empty = !nodes
            .iter()
            .all(|n| state.queue(self.node_queues[n.0]).is_empty());
        if !all_queues_empty {
            return true;
        }
        nodes.iter().any(|n| {
            let pool = state
                .get(self.thread_pools[n.0])
                .expect("unknown thread pool ID");
            pool.num_active() as f32 / (pool.num_idle() + pool.num_active()) as f32 > self.threshold
        })
    }
}

impl Dispatch for OptPlusDispatch {
    fn dispatch(&self, shards: &[ShardId], state: &State) -> Vec<(ShardId, NodeId)> {
        shards
            .iter()
            .map(|&shard_id| {
                if self.any_over_threshold(shard_id, &state) {
                    (shard_id, self.select_node(shard_id, &state))
                } else {
                    (shard_id, self.probabilistic.select_node(shard_id))
                }
            })
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
