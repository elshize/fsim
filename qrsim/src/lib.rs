//! Query routing simulation.

#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::default_trait_access,
    clippy::inline_always
)]

use std::rc::Rc;
use std::time::Duration;

use delegate::delegate;
use derive_more::{Display, From, Into};
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use sim20::{simulation, Components, Key, Scheduler, State};

// #[derive(Debug, Clone)]
// pub struct Clock {
//     time: Rc<Cell<Duration>>,
// }

// impl Clock {

// }

mod broker;
pub use broker::{Broker, BrokerQueues, Event as BrokerEvent, ResponseStatus};

mod query_generator;
pub use query_generator::{Event as QueryGeneratorEvent, QueryGenerator};

mod node;
pub use node::{Event as NodeEvent, Node};

// mod timeline;
// pub use timeline::Timeline;

mod query_log;
pub use query_log::QueryLog;

/// Shard ID.
#[derive(
    From,
    Into,
    Debug,
    PartialEq,
    PartialOrd,
    Eq,
    Ord,
    Serialize,
    Deserialize,
    Copy,
    Clone,
    Hash,
    Display,
)]
pub struct ShardId(usize);

/// Node ID.
#[derive(
    From,
    Into,
    Debug,
    PartialEq,
    PartialOrd,
    Eq,
    Ord,
    Serialize,
    Deserialize,
    Copy,
    Clone,
    Hash,
    Display,
)]
pub struct NodeId(usize);

/// Query ID.
#[derive(
    From,
    Into,
    Debug,
    PartialEq,
    PartialOrd,
    Eq,
    Ord,
    Serialize,
    Deserialize,
    Copy,
    Clone,
    Hash,
    Display,
)]
pub struct QueryId(usize);

/// Query request ID.
#[derive(
    From,
    Into,
    Debug,
    PartialEq,
    PartialOrd,
    Eq,
    Ord,
    Serialize,
    Deserialize,
    Copy,
    Clone,
    Hash,
    Display,
)]
pub struct RequestId(usize);

/// Represents numbers of cores in a node.
#[derive(
    From, Into, Debug, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize, Copy, Clone, Hash,
)]
pub struct NumCores(usize);
impl Default for NumCores {
    fn default() -> Self {
        NumCores(1)
    }
}

// /// Contains all past queries.
// pub type QueryLog = HashMap<RequestId, QueryResponse>;

/// Representation of a row read from queries file.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryRow {
    /// Shard ID for which it was timed.
    pub shard_id: ShardId,
    /// Query ID for which it was timed.
    pub query_id: QueryId,
    /// Time of query processing.
    pub time: u64,
}

/// Query data.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Query {
    /// Shards that will be selected for this query. If `None`, then all shards selected.
    pub selected_shards: Option<Vec<ShardId>>,
    /// Shard selection time.
    #[serde(default)]
    pub selection_time: u64,
    /// A list of retrieval times in all shards. This is the reference time that later can
    /// be scaled in nodes by multiplying by a retrieval speed factor.
    /// By default, though, all nodes are identical in terms of processing power.
    pub retrieval_times: Vec<u64>,
}

/// A query request sent to the broker by the query generator.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct QueryRequest {
    id: RequestId,
    query_id: QueryId,
    time: Duration,
}

impl QueryRequest {
    fn new(request_id: RequestId, query_id: QueryId, time: Duration) -> Self {
        Self {
            id: request_id,
            query_id,
            time,
        }
    }

    /// The ID of the query.
    #[must_use]
    pub fn query_id(&self) -> QueryId {
        self.query_id
    }

    /// The time of the simulation when the request was generated.
    #[must_use]
    pub fn generation_time(&self) -> Duration {
        self.time
    }

    /// The ID of this request, unique throughout the entire simulation.
    #[must_use]
    pub fn request_id(&self) -> RequestId {
        self.id
    }
}

/// A request picked up by the broker.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct BrokerRequest {
    query_request: QueryRequest,
    time: Duration,
}

impl BrokerRequest {
    fn new(query_request: QueryRequest, time: Duration) -> Self {
        Self {
            query_request,
            time,
        }
    }

    /// The time of the simulation when the request was picked up by the broker from its incoming queue.
    #[must_use]
    pub fn broker_time(&self) -> Duration {
        self.time
    }

    delegate! {
        to self.query_request {
            /// The ID of the query.
            #[must_use]
            pub fn query_id(&self) -> QueryId;
            /// The ID of the query request, unique throughout the entire simulation.
            #[must_use]
            pub fn request_id(&self) -> RequestId;
            /// The time of the simulation when the request was generated.
            #[must_use]
            pub fn generation_time(&self) -> Duration;
        }
    }
}

/// A request sent to the node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRequest {
    shard_id: ShardId,
    broker_request: Rc<BrokerRequest>, // stored in an Rc to ensure they don't diverge later on
    time: Duration,
}

impl NodeRequest {
    fn new(broker_request: Rc<BrokerRequest>, shard_id: ShardId, time: Duration) -> Self {
        Self {
            shard_id,
            broker_request,
            time,
        }
    }

    #[must_use]
    fn broker_request(&self) -> Rc<BrokerRequest> {
        Rc::clone(&self.broker_request)
    }

    /// The ID of the shard of which this query should be executed.
    #[must_use]
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// The time of the simulation when this request was dispatched to a node.
    #[must_use]
    pub fn dispatch_time(&self) -> Duration {
        self.time
    }

    delegate! {
        to self.broker_request {
            /// The ID of the query.
            #[must_use]
            pub fn query_id(&self) -> QueryId;
            /// The ID of the query request, unique throughout the entire simulation.
            #[must_use]
            pub fn request_id(&self) -> RequestId;
            /// The time of the simulation when the request was generated.
            #[must_use]
            pub fn generation_time(&self) -> Duration;
            /// The time of the simulation when the request was picked up by the broker
            /// from its incoming queue.
            #[must_use]
            pub fn broker_time(&self) -> Duration;
        }
    }
}

/// A response sent back from a node to the broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeResponse {
    request: NodeRequest,
    node_id: NodeId,
    start: Duration,
    end: Duration,
}

impl NodeResponse {
    #[must_use]
    fn new(request: NodeRequest, node_id: NodeId, start: Duration, end: Duration) -> Self {
        NodeResponse {
            request,
            node_id,
            start,
            end,
        }
    }

    /// The time of the simulation when the request was issued locally to a shard replica.
    #[must_use]
    pub fn processing_start_time(&self) -> Duration {
        self.start
    }

    /// The time of the simulation when the results were returned by a shards replica.
    #[must_use]
    pub fn processing_end_time(&self) -> Duration {
        self.end
    }

    delegate! {
        to self.request {
            #[must_use]
            fn broker_request(&self) -> Rc<BrokerRequest>;
            /// The ID of the query.
            #[must_use]
            pub fn query_id(&self) -> QueryId;
            /// The ID of the query request, unique throughout the entire simulation.
            #[must_use]
            pub fn request_id(&self) -> RequestId;
            /// The time of the simulation when the request was generated.
            #[must_use]
            pub fn generation_time(&self) -> Duration;
            /// The time of the simulation when the request was picked up by the broker
            /// from its incoming queue.
            #[must_use]
            pub fn broker_time(&self) -> Duration;
            /// The time of the simulation when the request was dispatched to a node.
            #[must_use]
            pub fn dispatch_time(&self) -> Duration;
        }
    }
}

/// A query response returned to the user from the broker after aggregating partial results.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResponse {
    broker_request: Rc<BrokerRequest>,
    received_responses: Vec<NodeResponse>,
    dropped_requests: Vec<NodeRequest>,
    time: Duration,
}

impl QueryResponse {
    #[must_use]
    fn new(
        broker_request: Rc<BrokerRequest>,
        received_responses: Vec<NodeResponse>,
        dropped_requests: Vec<NodeRequest>,
        time: Duration,
    ) -> Self {
        Self {
            broker_request,
            received_responses,
            dropped_requests,
            time,
        }
    }

    /// The time of finishing the entire query request and sending it back to the user.
    #[must_use]
    pub fn finished_time(&self) -> Duration {
        self.time
    }

    /// Iterates over all received node responses.
    pub fn received_responses(&self) -> impl Iterator<Item = &NodeResponse> {
        self.received_responses.iter()
    }

    /// Iterates over all dropped node requests.
    pub fn dropped_requests(&self) -> impl Iterator<Item = &NodeRequest> {
        self.dropped_requests.iter()
    }

    /// Number of total issued node requests.
    #[must_use]
    pub fn issued_requests(&self) -> usize {
        self.received_responses.len() + self.dropped_requests.len()
    }

    delegate! {
        to self.broker_request {
            /// The ID of the query.
            #[must_use]
            pub fn query_id(&self) -> QueryId;
            /// The ID of the query request, unique throughout the entire simulation.
            #[must_use]
            pub fn request_id(&self) -> RequestId;
            /// The time of the simulation when the request was generated.
            #[must_use]
            pub fn generation_time(&self) -> Duration;
            /// The time of the simulation when the request was picked up by the broker
            /// from its incoming queue.
            #[must_use]
            pub fn broker_time(&self) -> Duration;
        }
    }
}

/// Implementors are dispatch policies that select nodes for requested shards.
pub trait Dispatch {
    /// Selects a node for each requested shard.
    fn dispatch(&mut self, shards: &[ShardId]) -> Vec<(ShardId, NodeId)>;
    /// Total number of existing shards.
    fn num_shards(&self) -> usize;
    /// Total number of existing nodes.
    fn num_nodes(&self) -> usize;
}

simulation! {
    /// The trait implemented by all valid events.
    #[events(QueryGeneratorEvent, BrokerEvent, NodeEvent)]
    pub trait ValidEvent {}

    /// The main simulation object.
    #[simulation]
    pub struct Simulation {
        /// Current state of the simulation meant to be mutated by the components.
        pub state: State,
        /// Schedules events and maintains the clock.
        pub scheduler: Scheduler,
        components: Components,
    }
}

impl Simulation {
    /// Runs for the specified time, and exits afterwards.
    pub fn run_until(&mut self, time: Duration, key: Key<QueryLog>) -> Duration {
        let pb = ProgressBar::new(time.as_secs())
            .with_style(ProgressStyle::default_bar().template("{msg} {wide_bar} {percent}%"));
        while self.scheduler.time() < time {
            let end = !self.step();
            let time = self.scheduler.time();
            let query_log = self.state.get(key).expect("Missing query log in state");
            let secs = time.as_secs();
            if pb.position() < secs {
                pb.set_position(secs);
                pb.set_message(&format!(
                    "[{time}s] [W={waiting}] [A={active}] [X={dropped}] [F={finished}] [CT={current}] [TT={total}]",
                    time = time.as_secs(),
                    active = query_log.active_requests(),
                    finished = query_log.finished_requests(),
                    dropped = query_log.dropped_requests(),
                    waiting = query_log.waiting_requests(),
                    current = query_log.current_throughput().round(),
                    total = query_log.total_throughput().round(),
                ));
            }
            if end {
                pb.finish();
                return time;
            }
        }
        pb.finish();
        time
    }
}
