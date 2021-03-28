use super::{random_enabled_node_from, Dispatch, NodeId, QueryId, ShardId, State};
use crate::{NodeQueue, NodeQueueEntry, NodeStatus, NodeThreadPool};

use std::cell::RefCell;
use std::collections::HashSet;

use itertools::izip;
use ordered_float::OrderedFloat;
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

    fn select_node(&self, shard_id: ShardId, state: &State, current_loads: &[usize]) -> NodeId {
        let (node_id, load) = izip!(&self.shards[shard_id.0], &self.node_weights, current_loads)
            .filter(|(n, _, _)| !self.disabled_nodes.contains(n))
            .map(|(n, w, c)| {
                let queue_len = state.len(self.node_queues[n.0]);
                let active = state
                    .get(self.thread_pools[n.0])
                    .expect("unknown thread pool ID")
                    .num_active();
                (n, (queue_len + active + *c) as f32 * *w)
            })
            .min_by_key(|(_, load)| OrderedFloat(*load))
            .unwrap();
        if load == 0.0 {
            let nodes = izip!(&self.shards[shard_id.0], &self.node_weights, current_loads)
                .filter_map(|(n, _, c)| {
                    if self.disabled_nodes.contains(n) {
                        None
                    } else {
                        let queue_len = state.len(self.node_queues[n.0]);
                        let active = state
                            .get(self.thread_pools[n.0])
                            .expect("unknown thread pool ID")
                            .num_active();
                        if queue_len + active + *c == 0 {
                            None
                        } else {
                            Some(*n)
                        }
                    }
                })
                .collect::<Vec<_>>();
            random_enabled_node_from(&nodes, &mut *self.rng.borrow_mut(), &self.disabled_nodes)
        } else {
            *node_id
        }
    }
}

impl Dispatch for ShortestQueueDispatch {
    fn dispatch(&self, _: QueryId, shards: &[ShardId], state: &State) -> Vec<(ShardId, NodeId)> {
        let mut current_loads = vec![0; self.num_nodes()];
        let mut selection = shards
            .iter()
            .copied()
            .map(|sid| (sid, NodeId(0)))
            .collect::<Vec<_>>();
        for (shard_id, node_id) in &mut selection {
            *node_id = self.select_node(*shard_id, &state, &current_loads);
            current_loads[node_id.0] += 1;
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
