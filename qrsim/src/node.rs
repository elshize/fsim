use crate::{BrokerEvent, NodeId, NodeRequest, NodeResponse, Query};

use std::rc::Rc;
use std::time::Duration;

use sim20::{Component, ComponentId, Key, QueueId, Scheduler, State};

/// Node events.
#[derive(Debug, Copy, Clone)]
pub enum Event {
    /// Node has finished doing work and is ready to pick up the next request.
    Idle,
    /// New request has just arrived at the node's queue.
    NewRequest,
    /// Processing of the request passed in the event has finished.
    ProcessingFinished {
        /// When the processing started, i.e., request was picked up from the queue.
        start: Duration,
        /// Where the request is stored in the state.
        request_key: Key<NodeRequest>,
        /// Where to send the response.
        broker: ComponentId<BrokerEvent>,
    },
}

/// A node is responsible for executing a query on one of its replicas and returning these results
/// to the broker.
///
/// Each node has a limited number of cores/threads running simultaneously. If all threads are
/// busy at the arrival of a query request, then it must wait in the queue.
pub struct Node {
    id: NodeId,
    queries: Rc<Vec<Query>>,
    incoming: QueueId<(NodeRequest, ComponentId<BrokerEvent>)>,
    outgoing: QueueId<NodeResponse>,
    idle_cores: usize,
}

impl Node {
    /// Constructs a node with the given ID and queue setup, having the given number of cores/threads.
    #[must_use]
    pub fn new(
        id: NodeId,
        queries: Rc<Vec<Query>>,
        incoming: QueueId<(NodeRequest, ComponentId<BrokerEvent>)>,
        outgoing: QueueId<NodeResponse>,
        idle_cores: usize,
    ) -> Self {
        Self {
            id,
            queries,
            incoming,
            outgoing,
            idle_cores,
        }
    }
}

impl Component for Node {
    type Event = Event;

    fn process_event(
        &mut self,
        self_id: ComponentId<Self::Event>,
        event: &Self::Event,
        scheduler: &mut Scheduler,
        state: &mut State,
    ) {
        match event {
            Event::Idle | Event::NewRequest => {
                log::trace!("Node is idle");
                if self.idle_cores > 0 {
                    self.idle_cores -= 1;
                    if let Some((request, broker)) = state.recv(self.incoming) {
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
                        self.idle_cores += 1;
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
                scheduler.schedule_immediately(*broker, BrokerEvent::Response);
                scheduler.schedule_immediately(self_id, Event::Idle);
                self.idle_cores += 1;
            }
        }
    }
}
