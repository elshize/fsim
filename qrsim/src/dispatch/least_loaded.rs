use crate::{
    Dispatch, NodeId, NodeQueue, NodeQueueEntry, NodeStatus, NodeThreadPool, QueryEstimate,
    QueryId, ShardId,
};

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
use simrs::{ClockRef, Key, QueueId, State};

#[derive(Debug, Clone, Copy)]
pub enum ActiveRequestPolicy {
    Ignore,
    OnlyEstimate,
    ConsiderElapsed,
}

/// Always selects the node with the least load waiting in the queue.
pub struct LeastLoadedDispatch {
    node_queues: Vec<QueueId<NodeQueue<NodeQueueEntry>>>,
    node_weights: Vec<f32>,
    shards: Vec<Vec<NodeId>>,
    disabled_nodes: HashSet<NodeId>,
    estimates: Rc<Vec<QueryEstimate>>,
    thread_pools: Vec<Key<NodeThreadPool>>,
    rng: RefCell<ChaChaRng>,
    active_request_policy: ActiveRequestPolicy,
    clock: ClockRef,
}

impl LeastLoadedDispatch {
    /// Constructs a new dispatcher.
    #[must_use]
    pub fn new(
        nodes: &[Vec<usize>],
        node_queues: Vec<QueueId<NodeQueue<NodeQueueEntry>>>,
        estimates: Rc<Vec<QueryEstimate>>,
        thread_pools: Vec<Key<NodeThreadPool>>,
        active_request_policy: ActiveRequestPolicy,
        clock: ClockRef,
    ) -> Self {
        let node_weights = vec![1.0; node_queues.len()];
        Self {
            shards: super::invert_nodes_to_shards(nodes),
            disabled_nodes: HashSet::new(),
            node_queues,
            estimates,
            thread_pools,
            node_weights,
            rng: RefCell::new(ChaChaRng::from_entropy()),
            active_request_policy,
            clock,
        }
    }

    fn query_time(&self, query_id: QueryId, shard_id: ShardId) -> u64 {
        self.estimates
            .get(query_id.0 - 1)
            .expect("query out of bounds")
            .shard_estimate(shard_id)
    }

    fn running_load(&self, state: &State, node_id: NodeId) -> u64 {
        match self.active_request_policy {
            ActiveRequestPolicy::OnlyEstimate => state
                .get(self.thread_pools[node_id.0])
                .expect("unknown thread pool ID")
                .running_threads()
                .iter()
                .map(|t| t.estimated.as_micros())
                .sum::<u128>() as u64,
            ActiveRequestPolicy::ConsiderElapsed => {
                let now = self.clock.time().as_micros();
                state
                    .get(self.thread_pools[node_id.0])
                    .expect("unknown thread pool ID")
                    .running_threads()
                    .iter()
                    .map(|t| {
                        let elapsed = now - t.start.as_micros();
                        t.estimated.as_micros().checked_sub(elapsed).unwrap_or(0)
                    })
                    .sum::<u128>() as u64
            }
            ActiveRequestPolicy::Ignore => 0,
        }
    }

    fn waiting_load(&self, state: &State, node_id: NodeId) -> u64 {
        state
            .queue(self.node_queues[node_id.0])
            .iter()
            .map(|msg| self.query_time(msg.request.query_id(), msg.request.shard_id()))
            .sum()
    }

    fn calc_loads(&self, state: &State) -> Vec<f32> {
        (0..self.num_nodes())
            .map(NodeId)
            .zip(&self.node_weights)
            .map(|(node_id, weight)| {
                if !self.disabled_nodes.contains(&node_id) {
                    let load =
                        self.running_load(state, node_id) + self.waiting_load(state, node_id);
                    load as f32 * *weight
                } else {
                    f32::MAX
                }
            })
            .collect()
    }

    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    fn select_node(&self, shard_id: ShardId, loads: &[f32]) -> NodeId {
        use rand::Rng;
        let mut min_load = f32::MAX;
        let mut min_node = NodeId(0);
        let mut min_count = 0;
        let nodes = &self.shards[shard_id.0];
        if nodes.len() == 1 {
            return nodes[0];
        }
        for &node_id in nodes.iter().filter(|n| !self.disabled_nodes.contains(n)) {
            let load = loads[node_id.0];
            if load < min_load {
                min_load = load;
                min_node = node_id;
                min_count = 1;
            } else if load == min_load {
                min_count += 1;
            }
        }
        if min_count == 1 {
            min_node
        } else {
            let selected: usize = self.rng.borrow_mut().gen_range(0..min_count);
            self.shards[shard_id.0]
                .iter()
                .copied()
                .filter(|n| loads[n.0] == min_load)
                .nth(selected)
                .unwrap()
        }
    }
}

impl Dispatch for LeastLoadedDispatch {
    fn dispatch(
        &self,
        query_id: QueryId,
        shards: &[ShardId],
        state: &State,
    ) -> Vec<(ShardId, NodeId)> {
        let mut loads = self.calc_loads(state);
        let mut selection = shards
            .iter()
            .copied()
            .map(|sid| (sid, NodeId(0)))
            .collect::<Vec<_>>();
        for (shard_id, node_id) in &mut selection {
            let nid = self.select_node(*shard_id, &loads);
            loads[nid.0] += self.query_time(query_id, *shard_id) as f32 * self.node_weights[nid.0];
            *node_id = nid;
        }
        selection
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
                NodeStatus::Injured(i) => {
                    assert!(*i > 1.0);
                    *i
                }
                NodeStatus::Unresponsive => 0.0,
            })
            .collect();
    }
}
