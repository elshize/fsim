//! Represents a node in the simulation.

use crate::{
    process::Runnable, query::Query, queue::ProcessCallback, Effect, Event, NodeId, Process,
    ShardId,
};
use std::rc::Rc;
use std::time::Duration;

/// Entry points to the node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStage {
    /// Request a query from an incoming query queue.
    GetQuery,
    /// Run retrieval for a query on a shard.
    Retrieval(Query, ShardId),
}

/// Node process.
pub struct Node<'a> {
    /// ID of the node.
    pub id: NodeId,
    /// Reference to query data, such as retrieval times, etc.
    pub query_data: Rc<Vec<crate::config::Query>>,
    /// Function converting int values to durations.
    pub duration_from_u64: Box<dyn Fn(u64) -> Duration + 'a>,
}

impl<'a> Runnable for Node<'a> {
    type Payload = NodeStage;
    type Effect = Effect<'a>;

    fn run(&self, entry: Self::Payload) -> Self::Effect {
        use NodeStage::*;
        match entry {
            GetQuery => {
                let id = self.id;
                Effect::NodeQueryGet {
                    node: id,
                    callback: ProcessCallback::new(move |(query, shard)| Process::Node {
                        id,
                        stage: Retrieval(query, shard),
                    }),
                }
            }
            Retrieval(query, shard) => {
                let retrieval_time = *self
                    .query_data
                    .get(usize::from(query.id))
                    .expect("Query ID out of bounds")
                    .retrieval_times
                    .get(usize::from(shard))
                    .expect("Shard ID out of bounds");
                Effect::Schedule(Event::new(
                    (self.duration_from_u64)(retrieval_time),
                    Process::Node {
                        id: self.id,
                        stage: GetQuery,
                    },
                ))
            }
        }
    }
}

#[cfg(test)]
mod test {
    //
}
