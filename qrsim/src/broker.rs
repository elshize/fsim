use crate::{
    node, BrokerRequest, Dispatch, NodeId, NodeQueueEntry, NodeRequest, NodeResponse, Query,
    QueryLog, QueryRequest, QueryResponse, RequestId, ShardId,
};

use std::cell::RefCell;
use std::collections::{hash_map::Entry, HashMap};
use std::rc::Rc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use simrs::{Component, ComponentId, Key, QueueId, Scheduler, State};

/// Broker events.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    pub node: Vec<QueueId<NodeQueueEntry>>,
    /// Incoming queues from shard nodes.
    pub response: QueueId<NodeResponse>,
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
    /// Function calculating the priority of a node request.
    pub priority: Box<dyn Fn(&Query, &NodeRequest) -> u64>,
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
        log::debug!("Broker picked up request: {:?}", request);
        let query = &self.queries[usize::from(request.query_id)];
        let selection_time = Duration::from_micros(query.selection_time);
        let shards: Vec<ShardId> = query.selected_shards.clone().unwrap_or_else(|| {
            (0..self.dispatcher.borrow().num_shards())
                .map(ShardId::from)
                .collect()
        });
        state
            .get_mut(self.query_log_id)
            .unwrap()
            .new_request(request);
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

    fn priority(&self, request: &NodeRequest) -> u64 {
        request.dispatch_time().as_micros() as u64
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
        let dispatch_overhead = Duration::from_micros(0);
        let mut response_status = ResponseStatus::new(shards.len());
        let request = Rc::new(request);
        for (shard_id, node_id) in self.dispatcher.borrow().dispatch(&shards) {
            let queue = self.queues.node[usize::from(node_id)];
            let request = NodeRequest::new(Rc::clone(&request), shard_id, scheduler.time());
            let entry = NodeQueueEntry::new(self.priority(&request), request.clone(), self_id);
            if state.send(queue, entry).is_ok() {
                scheduler.schedule(
                    dispatch_overhead,
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
        scheduler.schedule(dispatch_overhead, self_id, Event::Idle);
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
