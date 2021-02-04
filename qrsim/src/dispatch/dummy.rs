use super::{Dispatch, NodeId, ShardId, State};

/// A dummy implementor of the [`Dispatch`] trait used for testing purposes.
/// It always selects an empty list of replicas.
pub struct DummyDispatcher;

impl Dispatch for DummyDispatcher {
    fn dispatch(&self, _shards: &[ShardId], _: &State) -> Vec<(ShardId, NodeId)> {
        vec![]
    }
    fn num_nodes(&self) -> usize {
        0
    }
    fn num_shards(&self) -> usize {
        0
    }
    fn disable_node(&mut self, _: NodeId) -> eyre::Result<bool> {
        Ok(false)
    }
    fn enable_node(&mut self, _: NodeId) -> bool {
        false
    }
}
