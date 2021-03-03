use super::{Dispatch, NodeId, ShardId, State};
use crate::{NodeQueue, NodeQueueEntry, NodeThreadPool, Query, QueryEstimate, QueryId};

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use rand_chacha::ChaChaRng;
use simrs::{Key, QueueId};

/// Always selects the node with the least load waiting in the queue.
pub struct DynamicDispatch {
    node_queues: Vec<QueueId<NodeQueue<NodeQueueEntry>>>,
    shards: Vec<Vec<NodeId>>,
    disabled_nodes: HashSet<NodeId>,
    estimates: Rc<Vec<QueryEstimate>>,
    thread_pools: Vec<Key<NodeThreadPool>>,
    clock: simrs::ClockRef,
    rng: RefCell<ChaChaRng>,
}

impl DynamicDispatch {
    /// Constructs a new dispatcher.
    #[must_use]
    pub fn new(
        nodes: &[Vec<usize>],
        node_queues: Vec<QueueId<NodeQueue<NodeQueueEntry>>>,
        estimates: Rc<Vec<QueryEstimate>>,
        queries: Rc<Vec<Query>>,
        thread_pools: Vec<Key<NodeThreadPool>>,
        clock: simrs::ClockRef,
        rng: RefCell<ChaChaRng>,
    ) -> Self {
        Self {
            shards: super::invert_nodes_to_shards(nodes),
            disabled_nodes: HashSet::new(),
            node_queues,
            estimates,
            thread_pools,
            clock,
            rng,
        }
    }

    fn query_time(&self, query_id: QueryId, shard_id: ShardId) -> u64 {
        // self.queries
        //     .get(query_id.0)
        //     .expect("query out of bounds")
        //     .retrieval_times[shard_id.0]
        self.estimates
            .get(query_id.0 - 1)
            .expect("query out of bounds")
            .shard_estimate(shard_id)
    }

    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    fn _select_node(&self, shard_id: ShardId, state: &State) -> NodeId {
        *self.shards[shard_id.0]
            .iter()
            .filter(|n| !self.disabled_nodes.contains(n))
            .min_by_key(|n| {
                let running = state
                    .get(self.thread_pools[n.0])
                    .expect("unknown thread pool ID")
                    .running_threads()
                    .iter()
                    .map(|t| {
                        let elapsed = self.clock.time() - t.start;
                        t.estimated
                            .as_micros()
                            .checked_sub(elapsed.as_micros())
                            .unwrap_or_default()
                    })
                    .sum::<u128>();
                let waiting = state
                    .queue(self.node_queues[n.0])
                    .iter()
                    .map(|msg| self.query_time(msg.request.query_id(), msg.request.shard_id()))
                    .sum::<u64>();
                //waiting
                running as u64 + waiting
            })
            .unwrap()
    }

    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    fn select_node(&self, shard_id: ShardId, state: &State) -> NodeId {
        use rand_distr::Distribution;
        let weights: Vec<_> = self.shards[shard_id.0]
            .iter()
            .filter(|n| !self.disabled_nodes.contains(n))
            .map(|n| {
                let running = state
                    .get(self.thread_pools[n.0])
                    .expect("unknown thread pool ID")
                    .running_threads()
                    .iter()
                    .map(|t| {
                        let elapsed = self.clock.time() - t.start;
                        t.estimated
                            .as_micros()
                            .checked_sub(elapsed.as_micros())
                            .unwrap_or_default()
                    })
                    .sum::<u128>();
                let waiting = state
                    .queue(self.node_queues[n.0])
                    .iter()
                    .map(|msg| self.query_time(msg.request.query_id(), msg.request.shard_id()))
                    .sum::<u64>();
                //waiting
                1.0 / (running as f64 + waiting as f64 + 1.0)
            })
            .collect();
        let distr = rand_distr::WeightedAliasIndex::new(weights).unwrap();
        let mut rng = self.rng.borrow_mut();
        NodeId(distr.sample(&mut *rng))
    }
}

impl Dispatch for DynamicDispatch {
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
}
