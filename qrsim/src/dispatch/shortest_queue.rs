use super::{Dispatch, NodeId, QueryId, ShardId, State};
use crate::{NodeQueue, NodeQueueEntry, NodeStatus, NodeThreadPool};

use std::cell::RefCell;
use std::collections::HashSet;

use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
use simrs::{Key, QueueId};

/// Always selects the node with the fewer requests in the queue.
pub struct ShortestQueueDispatch {
    node_queues: Vec<QueueId<NodeQueue<NodeQueueEntry>>>,
    node_weights: Vec<f32>,
    shards: Vec<Vec<NodeId>>,
    disabled_nodes: HashSet<NodeId>,
    thread_pools: Vec<Key<NodeThreadPool>>,
    rng: RefCell<ChaChaRng>,
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
            rng: RefCell::new(ChaChaRng::from_entropy()),
        }
    }

    fn calc_length(&self, state: &State, node_id: NodeId) -> usize {
        let queue_len = state.len(self.node_queues[node_id.0]);
        let active = state
            .get(self.thread_pools[node_id.0])
            .expect("unknown thread pool ID")
            .num_active();
        queue_len + active
    }

    fn calc_loads(&self, state: &State) -> Vec<f32> {
        (0..self.num_nodes())
            .map(NodeId)
            .zip(&self.node_weights)
            .map(|(node_id, weight)| {
                if self.disabled_nodes.contains(&node_id) {
                    self.calc_length(state, node_id) as f32 * *weight
                } else {
                    f32::MAX
                }
            })
            .collect()
    }

    fn select_node(&self, shard_id: ShardId, loads: &[f32]) -> NodeId {
        use rand::Rng;
        let mut min_load = f32::MAX;
        let mut min_node = NodeId(0);
        let mut min_count = 0;
        let nodes = &self.shards[shard_id.0];
        if nodes.len() == 1 {
            return nodes[0];
        }
        for &node_id in nodes {
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
                .nth(selected)
                .unwrap()
        }
    }
}

impl Dispatch for ShortestQueueDispatch {
    fn dispatch(&self, _: QueryId, shards: &[ShardId], state: &State) -> Vec<(ShardId, NodeId)> {
        let mut selection = shards
            .iter()
            .copied()
            .map(|sid| (sid, NodeId(0)))
            .collect::<Vec<_>>();
        let mut loads = self.calc_loads(state);
        for (shard_id, node_id) in &mut selection {
            *node_id = self.select_node(*shard_id, &loads);
            loads[node_id.0] += self.node_weights[node_id.0];
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
                NodeStatus::Injured(i) => *i,
                NodeStatus::Unresponsive => 0.0,
            })
            .collect();
    }
}
