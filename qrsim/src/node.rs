use crate::{BrokerEvent, BrokerId, NodeId, NodeRequest, NodeResponse, Query, RequestId};

use std::rc::Rc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use simrs::{Component, ComponentId, Fifo, Key, Queue, QueueId, Scheduler, State};

const BROKER_NOTIFY_DELAY: Duration = Duration::from_millis(20);

/// The state of a node.
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Node works as expected.
    Healthy,
    /// Node is currently working at a fraction of speed.
    Injured(f32),
    /// Node is currently unavailable.
    Unresponsive,
}

/// Node events.
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
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

    /// Change the status of this node to [`NodeStatus::Unresponsive`].
    Suspend,

    /// Change the status of this node to [`NodeStatus::Injured`] with the given fraction.
    Injure(f32),

    /// Change the status of this node to [`NodeStatus::Healthy`].
    Cure,
}

/// Implementors of this trait are used to select the next request out of many waiting in a queue.
pub trait Select {
    /// Type of queue element that holds a request.
    type Item: GetNodeRequest;

    /// Selects the next request to process.
    fn select(&self, requests: &[Self::Item]) -> Option<usize>;
}

/// Function returning the index of the next element (according to some strategy).
pub type BoxedSelect<T> = Box<dyn Select<Item = T>>;

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

    /// Iterate queue elements.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.inner.iter()
    }
}

impl<T: GetNodeRequest> Queue for NodeQueue<T> {
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
        self.select
            .select(&self.inner)
            .map(|idx| self.inner.swap_remove(idx))
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
#[derive(Debug, PartialEq)]
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

#[derive(Debug, Clone)]
pub struct RunningThread {
    pub request_id: RequestId,
    pub start: Duration,
    pub estimated: Duration,
}

/// Stores number of active and idle threads in a node.
pub struct NodeThreadPool {
    active: usize,
    idle: usize,
    running: Vec<RunningThread>,
}

impl NodeThreadPool {
    /// Constructs a new node thread pool with the given number of threads. Initially, all threads
    /// are idle.
    #[must_use]
    pub fn new(num_threads: usize) -> Self {
        Self {
            active: 0,
            idle: num_threads,
            running: Vec::with_capacity(num_threads),
        }
    }

    fn request_thread(&mut self) -> bool {
        if self.idle > 0 {
            self.idle -= 1;
            self.active += 1;
            true
        } else {
            false
        }
    }

    fn release_thread(&mut self) {
        self.idle += 1;
        self.active -= 1;
    }

    fn start_request(&mut self, request_id: RequestId, start: Duration, estimated: Duration) {
        self.running.push(RunningThread {
            request_id,
            start,
            estimated,
        });
    }

    fn finish_request(&mut self, request_id: RequestId) {
        if let Some(idx) = self.running.iter_mut().enumerate().find_map(|(idx, r)| {
            if r.request_id == request_id {
                Some(idx)
            } else {
                None
            }
        }) {
            self.running.swap_remove(idx);
        }
    }

    /// Returns the number of active threads in the node.
    #[must_use]
    pub fn num_active(&self) -> usize {
        self.active
    }

    /// Returns the number of idle threads in the node.
    #[must_use]
    pub fn num_idle(&self) -> usize {
        self.idle
    }

    /// Returns the slice of running threads.
    #[must_use]
    pub fn running_threads(&self) -> &[RunningThread] {
        &self.running
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
    thread_pool: Key<NodeThreadPool>,
    status: Key<NodeStatus>,
    broker_id: Key<Option<BrokerId>>,
}

impl Node {
    /// Constructs a node with the given ID and queue setup, having the given number of cores/threads.
    #[must_use]
    pub fn new(
        id: NodeId,
        queries: Rc<Vec<Query>>,
        incoming: QueueId<NodeQueue<NodeQueueEntry>>,
        outgoing: QueueId<Fifo<NodeResponse>>,
        thread_pool: Key<NodeThreadPool>,
        status: Key<NodeStatus>,
        broker_id: Key<Option<BrokerId>>,
    ) -> Self {
        Self {
            id,
            queries,
            incoming,
            outgoing,
            thread_pool,
            status,
            broker_id,
        }
    }

    fn thread_pool<'a>(&self, state: &'a mut State) -> &'a mut NodeThreadPool {
        state
            .get_mut(self.thread_pool)
            .expect("missing thread pool")
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
                let status = *state.get(self.status).expect("missing node status");
                if status == NodeStatus::Unresponsive {
                    return;
                }
                if self.thread_pool(state).request_thread() {
                    if let Some(NodeQueueEntry {
                        request, broker, ..
                    }) = state.recv(self.incoming)
                    {
                        log::debug!(
                            "Node {} picked up request {}",
                            self.id,
                            request.request_id()
                        );
                        let micros = {
                            let base = self.queries[usize::from(request.query_id()) - 1]
                                .retrieval_times[usize::from(request.shard_id)];
                            if let NodeStatus::Injured(f) = status {
                                (base as f32 * f).round() as u64
                            } else {
                                base
                            }
                        };
                        let time = Duration::from_micros(micros);
                        self.thread_pool(state).start_request(
                            request.request_id(),
                            scheduler.time(),
                            time,
                        );
                        scheduler.schedule(
                            time,
                            self_id,
                            Event::ProcessingFinished {
                                start: scheduler.time(),
                                request_key: state.insert(request),
                                broker,
                            },
                        );
                    } else {
                        self.thread_pool(state).release_thread();
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
                let thread_pool = self.thread_pool(state);
                thread_pool.finish_request(request.request_id());
                thread_pool.release_thread();
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
            }
            Event::Injure(fraction) => {
                let new_status = NodeStatus::Injured(*fraction);
                let status = state.get_mut(self.status).expect("missing node status");
                let old_status = *status;
                *status = new_status;
                let broker_id = state
                    .get(self.broker_id)
                    .expect("missing broker ID")
                    .expect("broker ID hasn't been registered");
                scheduler.schedule(
                    BROKER_NOTIFY_DELAY,
                    broker_id,
                    BrokerEvent::NodeStatusChanged {
                        node_id: self.id,
                        new_status,
                        old_status,
                    },
                );
            }
            Event::Cure => {
                let new_status = NodeStatus::Healthy;
                let status = state.get_mut(self.status).expect("missing node status");
                let old_status = *status;
                let broker_id = state
                    .get(self.broker_id)
                    .expect("missing broker ID")
                    .expect("broker ID hasn't been registered");
                scheduler.schedule(
                    BROKER_NOTIFY_DELAY,
                    broker_id,
                    BrokerEvent::NodeStatusChanged {
                        node_id: self.id,
                        new_status,
                        old_status,
                    },
                );
            }
            Event::Suspend => {
                let new_status = NodeStatus::Unresponsive;
                let status = state.get_mut(self.status).expect("missing node status");
                let old_status = *status;
                let broker_id = state
                    .get(self.broker_id)
                    .expect("missing broker ID")
                    .expect("broker ID hasn't been registered");
                scheduler.schedule(
                    BROKER_NOTIFY_DELAY,
                    broker_id,
                    BrokerEvent::NodeStatusChanged {
                        node_id: self.id,
                        new_status,
                        old_status,
                    },
                );
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{test::make_entry, test::NodeQueueEntryStub, FifoSelect, QueryId, ShardId};
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
        let mut queue = NodeQueue::<NodeQueueEntryStub>::unbounded(Box::new(FifoSelect::default()));
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
