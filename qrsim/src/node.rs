use crate::{BrokerEvent, NodeId, NodeRequest, NodeResponse, Query};

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use simrs::{Component, ComponentId, Fifo, Key, Queue, QueueId, Scheduler, State};

/// Node events.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    /// Node has finished doing work and is ready to pick up the next request.
    Idle,
    /// New request has just arrived at the node's queue.
    NewRequest,
    /// Processing of the request passed in the event has finished.
    #[serde(skip)]
    ProcessingFinished {
        /// When the processing started, i.e., request was picked up from the queue.
        start: Duration,
        /// Where the request is stored in the state.
        request_key: Key<NodeRequest>,
        /// Where to send the response.
        broker: ComponentId<BrokerEvent>,
    },
}

/// Function returning the index of the next element (according to some strategy).
pub type BoxedSelect<T> = Box<dyn FnMut(&[T]) -> Option<usize>>;

/// Queue holding incoming node requests.
pub struct NodeQueue<T> {
    inner: Vec<T>,
    capacity: usize,
    select: BoxedSelect<T>,
}

impl<T> NodeQueue<T> {
    /// Constructs an unbounded queue.
    #[must_use]
    pub fn unbounded(select: BoxedSelect<T>) -> Self {
        Self {
            inner: Vec::new(),
            capacity: usize::MAX,
            select,
        }
    }

    /// Constructs a bounded queue with the given capacity.
    #[must_use]
    pub fn bounded(select: BoxedSelect<T>, capacity: usize) -> Self {
        Self {
            inner: Vec::with_capacity(capacity),
            capacity,
            select,
        }
    }
}

impl<T> Queue for NodeQueue<T> {
    type Item = T;

    fn push(&mut self, value: T) -> Result<(), simrs::PushError> {
        if self.inner.len() < self.capacity {
            self.inner.push(value);
            Ok(())
        } else {
            Err(simrs::PushError)
        }
    }

    fn pop(&mut self) -> Option<T> {
        (self.select)(&mut self.inner).map(|idx| self.inner.swap_remove(idx))
    }

    fn len(&self) -> usize {
        self.inner.len()
    }
}

/// Trait implemented by structs that contain a node request.
pub trait GetNodeRequest {
    /// Returns a reference to the contained node request.
    fn node_request(&self) -> &NodeRequest;
}

/// Entry in the node queue containing a priority value, which is used to decide the order of
/// popping values from the queue.
#[derive(Debug)]
pub struct NodeQueueEntry {
    /// Request being sent to the node.
    pub request: NodeRequest,
    /// Broker from which the request is being sent.
    pub broker: ComponentId<BrokerEvent>,
}

impl NodeQueueEntry {
    /// Constructs a new [`NodeQueueEntry`].
    #[must_use]
    pub fn new(request: NodeRequest, broker: ComponentId<BrokerEvent>) -> Self {
        Self { request, broker }
    }
}

impl GetNodeRequest for NodeQueueEntry {
    fn node_request(&self) -> &NodeRequest {
        &self.request
    }
}

/// A node is responsible for executing a query on one of its replicas and returning these results
/// to the broker.
///
/// Each node has a limited number of cores/threads running simultaneously. If all threads are
/// busy at the arrival of a query request, then it must wait in the queue.
pub struct Node {
    id: NodeId,
    queries: Rc<Vec<Query>>,
    incoming: QueueId<NodeQueue<NodeQueueEntry>>,
    outgoing: QueueId<Fifo<NodeResponse>>,
    idle_cores: Cell<usize>,
}

impl Node {
    /// Constructs a node with the given ID and queue setup, having the given number of cores/threads.
    #[must_use]
    pub fn new(
        id: NodeId,
        queries: Rc<Vec<Query>>,
        incoming: QueueId<NodeQueue<NodeQueueEntry>>,
        outgoing: QueueId<Fifo<NodeResponse>>,
        num_cores: usize,
    ) -> Self {
        Self {
            id,
            queries,
            incoming,
            outgoing,
            idle_cores: Cell::new(num_cores),
        }
    }
}

impl Component for Node {
    type Event = Event;

    fn process_event(
        &self,
        self_id: ComponentId<Self::Event>,
        event: &Self::Event,
        scheduler: &mut Scheduler,
        state: &mut State,
    ) {
        match event {
            Event::Idle | Event::NewRequest => {
                log::trace!("Node is idle");
                let idle_cores = self.idle_cores.get();
                if idle_cores > 0 {
                    self.idle_cores.replace(idle_cores - 1);
                    if let Some(NodeQueueEntry {
                        request, broker, ..
                    }) = state.recv(self.incoming)
                    {
                        log::debug!(
                            "Node {} picked up request {}",
                            self.id,
                            request.request_id()
                        );
                        let time = self.queries[usize::from(request.query_id())].retrieval_times
                            [usize::from(request.shard_id)];
                        scheduler.schedule(
                            Duration::from_micros(time),
                            self_id,
                            Event::ProcessingFinished {
                                start: scheduler.time(),
                                request_key: state.insert(request),
                                broker,
                            },
                        );
                    } else {
                        self.idle_cores.replace(idle_cores);
                    }
                }
            }
            Event::ProcessingFinished {
                start,
                request_key,
                broker,
            } => {
                let request = state
                    .remove(*request_key)
                    .expect("Cannot find node request");
                if state
                    .send(
                        self.outgoing,
                        NodeResponse::new(request, self.id, *start, scheduler.time()),
                    )
                    .is_err()
                {
                    todo!("Right now, we assume unlimited throughput in the broker.");
                };
                scheduler.schedule_now(*broker, BrokerEvent::Response);
                scheduler.schedule_now(self_id, Event::Idle);
                let idle_cores = self.idle_cores.get();
                self.idle_cores.replace(idle_cores + 1);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{fifo_select, test::make_entry, test::NodeQueueEntryStub, QueryId, ShardId};
    use quickcheck::{Arbitrary, Gen};
    use quickcheck_macros::quickcheck;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum QueueOperation {
        Push,
        Pop,
        TimeIncrement,
    }

    impl Arbitrary for QueueOperation {
        fn arbitrary(g: &mut Gen) -> Self {
            *g.choose(&[
                QueueOperation::Push,
                QueueOperation::Pop,
                QueueOperation::TimeIncrement,
            ])
            .unwrap()
        }
    }

    #[quickcheck]
    fn test_node_queue_with_requests(ops: Vec<QueueOperation>) -> eyre::Result<()> {
        let mut clock = Duration::default();
        let mut queue = NodeQueue::<NodeQueueEntryStub>::unbounded(fifo_select());
        let mut buffer = vec![];
        for op in ops {
            match op {
                QueueOperation::Push => {
                    queue.push(make_entry(QueryId::from(0), ShardId::from(0), clock))?;
                }
                QueueOperation::Pop => {
                    if let Some(v) = queue.pop() {
                        buffer.push(v);
                    }
                }
                QueueOperation::TimeIncrement => {
                    clock += Duration::from_micros(1);
                }
            }
        }
        let times = buffer
            .into_iter()
            .map(|x| x.node_request().dispatch_time())
            .collect::<Vec<_>>();
        let sorted = {
            let mut sorted = times.clone();
            sorted.sort();
            sorted
        };
        assert_eq!(times, sorted);
        Ok(())
    }
}
