//! Represents a broker in the simulation.

use crate::replica_selector::{ReplicaSelection, ReplicaSelector};
use crate::shard_selector::{ShardSelection, ShardSelector};
use crate::{
    process::Runnable, query::Query, queue::ProcessCallback, Effect, NodeRequest, Process,
    ReplicaId,
};

/// Entry points to the broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerStage {
    /// Request a query from an incoming query queue.
    RequestQuery,
    /// Route the query to replica nodes.
    Route(Query),
}

/// Broker process.
pub struct Broker<'a> {
    shard_selector: Box<dyn ShardSelector + 'a>,
    replica_selector: Box<dyn ReplicaSelector + 'a>,
}

impl<'a> Broker<'a> {
    /// Constructs a new broker.
    #[must_use]
    pub fn new<S, R>(shard_selector: S, replica_selector: R) -> Self
    where
        S: ShardSelector + 'a,
        R: ReplicaSelector + 'a,
    {
        Self {
            shard_selector: Box::new(shard_selector),
            replica_selector: Box::new(replica_selector),
        }
    }
}

impl<'a> Runnable for Broker<'a> {
    type Payload = BrokerStage;
    type Effect = Effect<'a>;

    fn run(&self, entry: Self::Payload) -> Self::Effect {
        use BrokerStage::*;
        match entry {
            RequestQuery => {
                //log::info!("[Broker] requesting a query from incoming queue");
                Effect::QueryQueueGet(ProcessCallback::new(|q| Process::Broker(Route(q))))
            }
            Route(query) => {
                //trace!("[Broker] picked up a query");
                let ShardSelection {
                    time: shard_selection_time,
                    shards,
                } = self.shard_selector.select(query);
                let ReplicaSelection {
                    time: replica_selection_time,
                    replicas,
                } = self.replica_selector.select(query, shards);
                let timeout = shard_selection_time + replica_selection_time;
                //trace!("[Broker] selected shards in {:?}", &timeout);
                Effect::Route {
                    timeout,
                    query,
                    nodes: replicas
                        .into_iter()
                        .map(|ReplicaId { node, shard }| NodeRequest { id: node, shard })
                        .collect(),
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::replica_selector::{RandomReplicaSelector, Replicas};
    use crate::shard_selector::{ExhaustiveSelector, Shards};
    use crate::{NodeId, NodeRequest, QueryId, RequestId, ShardId};
    use rand::distributions::Uniform;
    use std::time::Duration;

    fn test_query() -> Query {
        Query {
            id: QueryId(56),
            request: RequestId(76),
        }
    }

    #[test]
    fn test_get_query() {
        let broker = Broker::new(
            ExhaustiveSelector::new(3),
            RandomReplicaSelector::<Uniform<usize>>::new(&[3, 3, 3]),
        );
        if let Effect::QueryQueueGet(callback) = broker.run(BrokerStage::RequestQuery) {
            assert_eq!(
                callback.process(test_query()),
                Process::Broker(BrokerStage::Route(test_query()))
            );
        } else {
            panic!("The returned effect doesn't match the expected one");
        }
    }

    struct MockReplicaSelector {}

    impl ReplicaSelector for MockReplicaSelector {
        fn select<'a>(&'a self, query: Query, shards: Shards<'a>) -> ReplicaSelection<'a> {
            ReplicaSelection {
                time: Duration::new(0, 17),
                replicas: Replicas::new(shards.map(move |shard| ReplicaId {
                    shard,
                    node: NodeId(usize::from(shard) + usize::from(query.id)),
                })),
            }
        }
    }

    #[test]
    fn test_route() {
        let broker = Broker::new(ExhaustiveSelector::new(3), MockReplicaSelector {});
        if let Effect::Route {
            timeout,
            query,
            nodes,
        } = broker.run(BrokerStage::Route(test_query()))
        {
            assert_eq!(timeout, Duration::new(0, 17));
            assert_eq!(query, test_query());
            assert_eq!(
                nodes,
                vec![
                    NodeRequest {
                        id: NodeId(56),
                        shard: ShardId(0)
                    },
                    NodeRequest {
                        id: NodeId(57),
                        shard: ShardId(1)
                    },
                    NodeRequest {
                        id: NodeId(58),
                        shard: ShardId(2)
                    },
                ]
            );
        } else {
            panic!("The returned effect doesn't match the expected one");
        }
    }
}
