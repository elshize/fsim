use super::random_enabled_node_from;
use crate::{
    Dispatch, NodeId, NodeQueue, NodeQueueEntry, NodeStatus, NodeThreadPool, QueryEstimate,
    QueryId, ShardId,
};

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use itertools::izip;
use ordered_float::OrderedFloat;
use rand_chacha::{rand_core::SeedableRng, ChaChaRng};
use simrs::{Key, QueueId, State};

/// Always selects the node with the least load waiting in the queue.
pub struct LeastLoadedDispatch {
    node_queues: Vec<QueueId<NodeQueue<NodeQueueEntry>>>,
    node_weights: Vec<f32>,
    shards: Vec<Vec<NodeId>>,
    disabled_nodes: HashSet<NodeId>,
    estimates: Rc<Vec<QueryEstimate>>,
    thread_pools: Vec<Key<NodeThreadPool>>,
    rng: RefCell<ChaChaRng>,
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
            rng: RefCell::new(ChaChaRng::from_entropy()),
        }
    }

    fn query_time(&self, query_id: QueryId, shard_id: ShardId) -> u64 {
        self.estimates
            .get(query_id.0 - 1)
            .expect("query out of bounds")
            .shard_estimate(shard_id)
    }

    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    fn select_node(
        &self,
        shard_id: ShardId,
        state: &State,
        current_loads: &[u64],
        query_id: QueryId,
    ) -> (NodeId, u64) {
        let node_loads = izip!(&self.shards[shard_id.0], &self.node_weights, current_loads)
            .filter(|(n, _, _)| !self.disabled_nodes.contains(n))
            .map(|(n, w, c)| {
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
                let load = running as u64 + waiting + *c;
                (n, load as f32 * *w)
            })
            .collect::<Vec<_>>();
        let min = node_loads
            .iter()
            .min_by_key(|(_, load)| OrderedFloat(*load))
            .unwrap()
            .1;
        let min_nodes = node_loads
            .into_iter()
            .filter_map(|(n, l)| if l == min { Some(*n) } else { None })
            .collect::<Vec<_>>();
        let node_id = if min_nodes.len() == 1 {
            *min_nodes.first().unwrap()
        } else {
            random_enabled_node_from(
                &min_nodes,
                &mut *self.rng.borrow_mut(),
                &self.disabled_nodes,
            )
        };
        (node_id, self.query_time(query_id, shard_id))
    }
}

impl Dispatch for LeastLoadedDispatch {
    fn dispatch(
        &self,
        query_id: QueryId,
        shards: &[ShardId],
        state: &State,
    ) -> Vec<(ShardId, NodeId)> {
        let mut current_loads = vec![0_u64; self.num_nodes()];
        let mut selection = shards
            .iter()
            .copied()
            .map(|sid| (sid, NodeId(0)))
            .collect::<Vec<_>>();
        for (shard_id, node_id) in &mut selection {
            let (nid, load) = self.select_node(*shard_id, &state, &current_loads, query_id);
            current_loads[nid.0] += load;
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
                NodeStatus::Injured(i) => *i,
                NodeStatus::Unresponsive => 0.0,
            })
            .collect();
    }
}
