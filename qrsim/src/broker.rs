use crate::{
    node, BrokerRequest, Dispatch, NodeId, NodeQueue, NodeQueueEntry, NodeRequest, NodeResponse,
    NodeStatus, Query, QueryLog, QueryRequest, QueryResponse, RequestId, ShardId,
};

use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::{hash_map::Entry, HashMap};
use std::rc::Rc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use simrs::{Component, ComponentId, Fifo, Key, QueueId, Scheduler, State};

/// Broker events.
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum Event {
    /// Broker has become idle after finishing a task.
    Idle,
    /// A new query request has reached the incoming queue.
    NewRequest(QueryRequest),
    /// Broker is ready to dispatch node requests.
    #[serde(skip)]
    Dispatch {
        /// Request to issue.
        request: BrokerRequest,
        /// A key to the list of shards contained within the key-value store.
        /// In selective search scenario, this list will be incomplete, as only
        /// selected shards are queried.
        shards: Key<Vec<ShardId>>,
    },
    /// New response has been received from a node to the queue.
    Response,
    /// Node is no longer available for dispatch.
    DisableNode(NodeId),

    /// Reissue these requests.
    NodeStatusChanged {
        node_id: NodeId,
        old_status: NodeStatus,
        new_status: NodeStatus,
    },
    RecomputePolicy,
}

/// Information about received and dropped responses to a request.
pub struct ResponseStatus {
    received: Vec<NodeResponse>,
    dropped: Vec<NodeRequest>,
    requested: usize,
}

impl ResponseStatus {
    fn new(requested_shards: usize) -> Self {
        Self {
            requested: requested_shards,
            received: Vec::new(),
            dropped: Vec::new(),
        }
    }
    fn is_finished(&self) -> bool {
        self.received.len() + self.dropped.len() == self.requested
    }
}

/// Container for queues coming in to and out of the broker.
pub struct BrokerQueues {
    // /// The main entry queue where the queries originate.
    // pub entry: QueueId<QueryRequest>,
    /// Outgoing queues to shard nodes.
    pub node: Vec<QueueId<NodeQueue<NodeQueueEntry>>>,
    /// Incoming queues from shard nodes.
    pub response: QueueId<Fifo<NodeResponse>>,
}

/// Broker does two types of jobs:
/// - picks up new incoming queries and dispatches them to nodes,
/// - aggregates responses from nodes.
pub struct Broker {
    /// Incoming/outgoing queues.
    pub queues: BrokerQueues,

    /// List of all component IDs for the nodes in the system.
    pub node_ids: Vec<ComponentId<node::Event>>,

    /// A reference to the list of all available queries, used to access times.
    pub queries: Rc<Vec<Query>>,

    /// The routing policy.
    pub dispatcher: RefCell<Box<dyn Dispatch>>,

    /// The key to the map of all responses, used to track when each query is fully processed.
    pub responses: Key<HashMap<RequestId, ResponseStatus>>,

    /// The key of the query log for status updates.
    pub query_log_id: Key<QueryLog>,

    /// Only dispatch to this many top ranked shards.
    pub selective: Option<usize>,

    /// Dispatch overhead.
    pub dispatch_overhead: Duration,

    /// List of current statuses of nodes.
    pub node_statuses: Vec<Key<NodeStatus>>,

    /// Whether to recompute policies on node status changes.
    pub recompute_policies: bool,
}

impl Component for Broker {
    type Event = Event;

    fn process_event(
        &self,
        self_id: ComponentId<Self::Event>,
        event: &Self::Event,
        scheduler: &mut Scheduler,
        state: &mut State,
    ) {
        match event {
            Event::Idle => {
                // For unlimited throughput, as it is now, this doesn't matter.
                // Should be handled, though, if limited resources are implemented.
            }
            Event::NewRequest(request) => {
                // let waiting_requests: usize =
                //     self.node_queues.iter().map(|&qid| state.len(qid)).sum();
                // let active_requests = state.get(self.query_log_id).unwrap().active_requests();
                // if active_requests < self.capacity {
                // if let Some(request) = state.recv(self.queues.entry) {
                self.process_new_request(self_id, scheduler, state, *request);
                // }
                // }
            }
            Event::Dispatch { request, shards } => {
                self.process_dispatch(self_id, scheduler, state, *request, *shards)
            }
            Event::Response => {
                if let Some(response) = state.recv(self.queues.response) {
                    self.process_response(self_id, scheduler, state, response)
                }
            }
            Event::DisableNode(node_id) => {
                self.dispatcher
                    .borrow_mut()
                    .disable_node(*node_id)
                    .unwrap_or_else(|err| {
                        log::error!("{}", err);
                        panic!();
                    });
            }
            Event::NodeStatusChanged {
                node_id,
                old_status,
                new_status,
            } => match (old_status, new_status) {
                (NodeStatus::Healthy, NodeStatus::Unresponsive)
                | (NodeStatus::Injured(_), NodeStatus::Unresponsive) => {
                    self.dispatcher
                        .borrow_mut()
                        .disable_node(*node_id)
                        .unwrap_or_else(|err| {
                            log::error!("{}", err);
                            panic!();
                        });
                    while let Some(request) = state.recv(self.queues.node[node_id.0]) {
                        let query_id = request.request.query_id();
                        let shard_id = request.request.shard_id();
                        let node_id = self
                            .dispatcher
                            .borrow()
                            .dispatch(query_id, &[shard_id], &state)
                            .first()
                            .unwrap()
                            .1;
                        let queue = self.queues.node[usize::from(node_id)];
                        if state.send(queue, request).is_ok() {
                            scheduler.schedule(
                                self.dispatch_overhead,
                                self.node_ids[usize::from(node_id)],
                                super::node::Event::NewRequest,
                            );
                        } else {
                            todo!("")
                        }
                    }
                }
                (NodeStatus::Unresponsive, NodeStatus::Healthy) => {
                    self.dispatcher.borrow_mut().enable_node(*node_id);
                }
                _ => {
                    if self.recompute_policies {
                        let time = self.dispatcher.borrow().recompute_delay();
                        scheduler.schedule(time, self_id, Event::RecomputePolicy);
                    }
                }
            },
            Event::RecomputePolicy => {
                self.dispatcher
                    .borrow_mut()
                    .recompute(&self.node_statuses, &state);
            }
        }
    }
}

impl Broker {
    // /// Constructs a new broker. It takes 3 types of queues: incoming query queue, outgoing node
    // /// request queues, and node response queue.
    // /// It also takes the component IDs of the nodes to schedule their events.
    // /// Furthermore, queries are passed for access to selection times.
    // /// Finally, a dispatcher is given for query routing, and an ID to access the state's query log
    // /// to log finished queries.
    // pub fn new<D: Dispatch + 'static>(
    //     queues: BrokerQueues,
    //     node_ids: Vec<ComponentId<node::Event>>,
    //     queries: Rc<Vec<Query>>,
    //     dispatcher: D,
    //     query_log_id: Key<QueryLog>,
    //     responses: Key<HashMap<RequestId, ResponseStatus>>,
    // ) -> Self {
    //     Self {
    //         queues,
    //         node_ids,
    //         queries,
    //         dispatcher: Box::new(dispatcher),
    //         responses,
    //         query_log_id,
    //     }
    // }

    fn process_new_request(
        &self,
        self_id: ComponentId<Event>,
        scheduler: &mut Scheduler,
        state: &mut State,
        request: QueryRequest,
    ) {
        state
            .get_mut(self.query_log_id)
            .unwrap()
            .new_request(request);
        if state.get(self.query_log_id).unwrap().active_requests() > 1000 {
            state
                .get_mut(self.query_log_id)
                .unwrap()
                .drop_request(request);
            log::warn!("Broker dropped request: {:?}", request);
            return;
        };
        log::debug!("Broker picked up request: {:?}", request);
        let query = &self.queries[usize::from(request.query_id)];
        let (selection_time, shards) = if let Some(top) = self.selective {
            let selection_time = Duration::from_micros(query.selection_time);
            let mut scored_shards = query
                .shard_scores
                .as_ref()
                .expect("shard scores needed for selective search")
                .iter()
                .enumerate()
                .map(|(shard_id, &score)| Reverse((ordered_float::OrderedFloat(score), shard_id)))
                .collect::<Vec<_>>();
            scored_shards.select_nth_unstable(top);
            (
                selection_time,
                scored_shards
                    .into_iter()
                    .take(top)
                    .map(|Reverse((_, id))| ShardId(id))
                    .collect(),
            )
        } else {
            let shards: Vec<ShardId> = (0..self.dispatcher.borrow().num_shards())
                .map(ShardId::from)
                .collect();
            (Duration::default(), shards)
        };
        scheduler.schedule(
            selection_time,
            self_id,
            Event::Dispatch {
                request: BrokerRequest::new(request, scheduler.time()),
                shards: state.insert(shards),
            },
        );
    }

    fn modify_responses<T, F: FnOnce(&mut HashMap<RequestId, ResponseStatus>) -> T>(
        responses_key: Key<HashMap<RequestId, ResponseStatus>>,
        state: &mut State,
        modify: F,
    ) -> T {
        let responses = state
            .get_mut(responses_key)
            .expect("Response hash map not found.");
        modify(responses)
    }

    fn insert_response_status(
        &self,
        state: &mut State,
        request_id: RequestId,
        response_status: ResponseStatus,
    ) {
        Self::modify_responses(self.responses, state, |responses| {
            responses.insert(request_id, response_status);
        });
    }

    fn process_dispatch(
        &self,
        self_id: ComponentId<Event>,
        scheduler: &mut Scheduler,
        state: &mut State,
        request: BrokerRequest,
        shards: Key<Vec<ShardId>>,
    ) {
        log::debug!("Dispatching request {}", request.request_id());
        state
            .get_mut(self.query_log_id)
            .unwrap()
            .dispatch_request(request);
        let shards = state
            .remove(shards)
            .expect("Dispatcher cannot find selected shards/replicas");
        let mut response_status = ResponseStatus::new(shards.len());
        let query_id = request.query_id();
        let request = Rc::new(request);
        for (shard_id, node_id) in self.dispatcher.borrow().dispatch(query_id, &shards, &state) {
            let queue = self.queues.node[usize::from(node_id)];
            let request = NodeRequest::new(Rc::clone(&request), shard_id, scheduler.time());
            let entry = NodeQueueEntry::new(request.clone(), self_id);
            if state.send(queue, entry).is_ok() {
                scheduler.schedule(
                    self.dispatch_overhead,
                    self.node_ids[usize::from(node_id)],
                    super::node::Event::NewRequest,
                );
            } else {
                log::warn!("Dropped query: {:?}", request);
                response_status.dropped.push(request);
                todo!("Check if all dropped")
            }
        }
        self.insert_response_status(state, request.request_id(), response_status);
        scheduler.schedule(self.dispatch_overhead, self_id, Event::Idle);
    }

    fn process_response(
        &self,
        self_id: ComponentId<Event>,
        scheduler: &mut Scheduler,
        state: &mut State,
        response: NodeResponse,
    ) {
        log::debug!(
            "[{:?}] Processing response: {}",
            scheduler.time(),
            response.request_id()
        );
        let broker_request = response.broker_request();
        let query_response = Self::modify_responses(self.responses, state, |responses| {
            let mut entry = match responses.entry(response.request_id()) {
                Entry::Vacant(_) => panic!("Cannot find response status"),
                Entry::Occupied(entry) => entry,
            };
            entry.get_mut().received.push(response);
            if entry.get().is_finished() {
                log::debug!(
                    "[{:?}] Finished processing response: {}",
                    scheduler.time(),
                    broker_request.request_id()
                );
                let node_responses = entry.remove();
                Some(QueryResponse::new(
                    broker_request,
                    node_responses.received,
                    node_responses.dropped,
                    scheduler.time(),
                ))
            } else {
                None
            }
        });
        if let Some(query_response) = query_response {
            state
                .get_mut(self.query_log_id)
                .expect("Cannot find query log")
                .finish(query_response);
        }
        scheduler.schedule_now(self_id, Event::Idle);
    }
}
