use crate::{
    node, BrokerRequest, Dispatch, NodeRequest, NodeResponse, Query, QueryLog, QueryRequest,
    QueryResponse, RequestId, ShardId,
};

use std::collections::{hash_map::Entry, HashMap};
use std::rc::Rc;
use std::time::Duration;

use simrs::{Component, ComponentId, Key, QueueId, Scheduler, State};

/// Broker events.
#[derive(Debug, Copy, Clone)]
pub enum Event {
    /// Broker has become idle after finishing a task.
    Idle,
    /// A new query request has reached the incoming queue.
    NewRequest,
    /// Broker is ready to dispatch node requests.
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
    /// The main entry queue where the queries originate.
    pub entry: QueueId<QueryRequest>,
    /// Outgoing queues to shard nodes.
    pub node: Vec<QueueId<(NodeRequest, ComponentId<Event>)>>,
    /// Incoming queues from shard nodes.
    pub response: QueueId<NodeResponse>,
}

/// Broker does two types of jobs:
/// - picks up new incoming queries and dispatches them to nodes,
/// - aggregates responses from nodes.
pub struct Broker {
    queues: BrokerQueues,
    node_ids: Vec<ComponentId<node::Event>>,
    queries: Rc<Vec<Query>>,
    dispatcher: Box<dyn Dispatch>,
    responses: Key<HashMap<RequestId, ResponseStatus>>,
    query_log_id: Key<QueryLog>,
    capacity: usize,
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
            Event::NewRequest => {
                // let waiting_requests: usize =
                //     self.node_queues.iter().map(|&qid| state.len(qid)).sum();
                let waiting_requests = state.get(self.query_log_id).unwrap().active_requests();
                if waiting_requests < self.capacity {
                    if let Some(request) = state.recv(self.queues.entry) {
                        self.process_new_request(self_id, scheduler, state, request);
                    }
                }
            }
            Event::Dispatch { request, shards } => {
                self.process_dispatch(self_id, scheduler, state, *request, *shards)
            }
            Event::Response => {
                if let Some(response) = state.recv(self.queues.response) {
                    self.process_response(self_id, scheduler, state, response)
                }
            }
        }
    }
}

impl Broker {
    /// Constructs a new broker. It takes 3 types of queues: incoming query queue, outgoing node
    /// request queues, and node response queue.
    /// It also takes the component IDs of the nodes to schedule their events.
    /// Furthermore, queries are passed for access to selection times.
    /// Finally, a dispatcher is given for query routing, and an ID to access the state's query log
    /// to log finished queries.
    pub fn new<D: Dispatch + 'static>(
        queues: BrokerQueues,
        node_ids: Vec<ComponentId<node::Event>>,
        queries: Rc<Vec<Query>>,
        dispatcher: D,
        query_log_id: Key<QueryLog>,
        capacity: usize,
        responses: Key<HashMap<RequestId, ResponseStatus>>,
    ) -> Self {
        Self {
            queues,
            node_ids,
            queries,
            dispatcher: Box::new(dispatcher),
            responses,
            query_log_id,
            capacity,
        }
    }

    fn process_new_request(
        &self,
        self_id: ComponentId<Event>,
        scheduler: &mut Scheduler,
        state: &mut State,
        request: QueryRequest,
    ) {
        log::info!("Broker picked up request: {:?}", request);
        let query = &self.queries[usize::from(request.query_id)];
        let selection_time = Duration::from_micros(query.selection_time);
        let shards: Vec<ShardId> = query.selected_shards.clone().unwrap_or_else(|| {
            (0..self.dispatcher.num_shards())
                .map(ShardId::from)
                .collect()
        });
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
        log::info!("Dispatching request {}", request.request_id());
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
        for (shard_id, node_id) in self.dispatcher.dispatch(&shards) {
            let queue = self.queues.node[usize::from(node_id)];
            let request = NodeRequest::new(Rc::clone(&request), shard_id, scheduler.time());
            if state.send(queue, (request.clone(), self_id)).is_ok() {
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
        log::info!(
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
                log::info!(
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
        scheduler.schedule_immediately(self_id, Event::Idle);
    }
}
