use super::{Dispatch, NodeId, ShardId};

/// A dummy implementor of the [`Dispatch`] trait used for testing purposes.
/// It always selects an empty list of replicas.
pub struct DummyDispatcher;

impl Dispatch for DummyDispatcher {
    fn dispatch(&self, _shards: &[ShardId]) -> Vec<(ShardId, NodeId)> {
        vec![]
    }
    fn num_nodes(&self) -> usize {
        0
    }
    fn num_shards(&self) -> usize {
        0
    }
}

