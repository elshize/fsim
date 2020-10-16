use crate::{BrokerRequest, NodeRequest, QueryRequest, QueryResponse, RequestId, ResponseStatus};

use std::time::Duration;

use im_rc::{HashMap, Vector};

/// Timeline is used to move forward and rewind the simulation.
/// To this end, the user-fronting information is stored in a sequence of immuatble structures.
pub struct Timeline {
    time: Duration,
    past: Vec<Snapshot>,
    future: Vec<Snapshot>,
}

pub struct Snapshot {
    queries_in_entry_queue: HashMap<RequestId, QueryRequest>,
    queries_in_broker: HashMap<RequestId, BrokerRequest>,
    requests_in_node_queues: HashMap<RequestId, NodeRequest>,
    node_responses: HashMap<RequestId, ResponseStatus>,
    past_queries: HashMap<RequestId, QueryResponse>,
    logs: Vector<String>,
}
