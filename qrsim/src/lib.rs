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
use simrs::{Key, Simulation};
use strum::IntoEnumIterator;

mod broker;
pub use broker::{Broker, BrokerQueues, Event as BrokerEvent, ResponseStatus};

mod node;
pub use node::{Event as NodeEvent, Node};

mod query_log;
pub use query_log::{write_from_channel, QueryLog};

mod dispatch;
pub use dispatch::dummy::DummyDispatcher;
pub use dispatch::probability::ProbabilisticDispatcher;
pub use dispatch::round_robin::RoundRobinDispatcher;
pub use dispatch::Dispatch;

/// See [`TimedEvent`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "component")]
#[serde(rename_all = "snake_case")]
pub enum Event {
    /// Broker-level event.
    Broker(BrokerEvent),
    /// Node-level event.
    Node {
        /// Node ID on which to execute the event.
        node_id: NodeId,
        /// Type of event.
        event: NodeEvent,
    },
}

/// Event that can be provided at the input of the simulation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimedEvent {
    /// Event occurring at `time`.
    pub event: Event,
    /// The moment from the start of the simulation when the event occurs.
    #[serde(with = "micro")]
    pub time: Duration,
}

use label::Label;
mod label {
    #[derive(Debug, Clone, Copy, strum::EnumIter)]
    pub enum Label {
        Waiting,
        Active,
        Dropped,
        Finished,
        CurrentThroughput,
        OverallThroughput,
    }

    impl Label {
        pub fn short(self) -> &'static str {
            match self {
                Label::Waiting => "W",
                Label::Active => "A",
                Label::Dropped => "X",
                Label::Finished => "F",
                Label::CurrentThroughput => "CT",
                Label::OverallThroughput => "OT",
            }
        }
        pub fn long(self) -> &'static str {
            match self {
                Label::Waiting => "Waiting requests",
                Label::Active => "Active requests",
                Label::Dropped => "Dropped requests",
                Label::Finished => "Finished requests",
                Label::CurrentThroughput => "Current throughput",
                Label::OverallThroughput => "Overall throughput",
            }
        }
    }
}

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

impl std::str::FromStr for NumCores {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<usize>().map(Self)
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
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryRequest {
    #[serde(rename = "request_id")]
    id: RequestId,
    query_id: QueryId,
    #[serde(with = "micro", rename = "entry_time")]
    time: Duration,
}

mod micro {
    use std::time::Duration;

    pub fn serialize<S>(time: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        use std::convert::TryFrom;
        serializer.serialize_u64(u64::try_from(time.as_micros()).expect("int out of bounds"))
    }

    use std::fmt;

    use serde::de::{self, Visitor};

    struct I64Visitor;

    impl<'de> Visitor<'de> for I64Visitor {
        type Value = u64;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an integer between 0 and 2^64")
        }

        fn visit_u8<E>(self, value: u8) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(u64::from(value))
        }

        fn visit_u16<E>(self, value: u16) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(u64::from(value))
        }

        fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(u64::from(value))
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer
            .deserialize_u64(I64Visitor)
            .map(Duration::from_micros)
    }
}

impl QueryRequest {
    /// Constructs a new query request at `time`.
    #[must_use]
    pub fn new(request_id: RequestId, query_id: QueryId, time: Duration) -> Self {
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
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize)]
pub struct BrokerRequest {
    #[serde(flatten)]
    query_request: QueryRequest,
    #[serde(with = "micro", rename = "broker_time")]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NodeRequest {
    shard_id: ShardId,
    #[serde(flatten)]
    broker_request: Rc<BrokerRequest>, // stored in an Rc to ensure they don't diverge later on
    #[serde(with = "micro", rename = "dispatch_time")]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NodeResponse {
    #[serde(flatten)]
    request: NodeRequest,
    node_id: NodeId,
    #[serde(with = "micro", rename = "node_start_time")]
    start: Duration,
    #[serde(with = "micro", rename = "node_end_time")]
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

    /// Convert to csv record.
    #[must_use]
    pub fn to_csv_record(&self) -> String {
        format!(
            "{},{},{},{}\n",
            self.request_id(),
            self.node_id,
            self.start.as_micros(),
            self.end.as_micros(),
        )
    }

    /// Produces the CSV header.
    #[must_use]
    pub const fn csv_header() -> &'static str {
        "request_id,node_id,node_start,node_end\n"
    }
}

/// A query response returned to the user from the broker after aggregating partial results.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QueryResponse {
    #[serde(flatten)]
    broker_request: Rc<BrokerRequest>,
    #[serde(skip_serializing)]
    received_responses: Vec<NodeResponse>,
    #[serde(skip_serializing)]
    dropped_requests: Vec<NodeRequest>,
    #[serde(with = "micro")]
    dispatch_time: Duration,
    #[serde(with = "micro")]
    response_time: Duration,
}

impl QueryResponse {
    #[must_use]
    fn new(
        broker_request: Rc<BrokerRequest>,
        received_responses: Vec<NodeResponse>,
        dropped_requests: Vec<NodeRequest>,
        time: Duration,
    ) -> Self {
        let dispatch_time = received_responses
            .iter()
            .map(NodeResponse::dispatch_time)
            .chain(dropped_requests.iter().map(NodeRequest::dispatch_time))
            .next()
            .unwrap_or_default();
        Self {
            broker_request,
            received_responses,
            dropped_requests,
            dispatch_time,
            response_time: time,
        }
    }

    /// Convert to csv record.
    #[must_use]
    pub fn to_csv_record(&self) -> String {
        format!(
            "{},{},{},{},{},{}\n",
            self.request_id(),
            self.query_id(),
            self.generation_time().as_micros(),
            self.broker_time().as_micros(),
            self.dispatch_time.as_micros(),
            self.response_time.as_micros(),
        )
    }

    /// Produces the CSV header.
    #[must_use]
    pub const fn csv_header() -> &'static str {
        "request_id,query_id,generation_time,broker_time,dispatch_time,response_time\n"
    }

    /// The time of finishing the entire query request and sending it back to the user.
    #[must_use]
    pub fn finished_time(&self) -> Duration {
        self.response_time
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

fn print_legend() {
    println!("Legend:");
    for label in Label::iter() {
        println!("\t{}\t{}", label.short(), label.long());
    }
}

/// Runs for the specified time, and exits afterwards.
pub fn run_until(simulation: &mut Simulation, time: Duration, key: Key<QueryLog>) -> Duration {
    print_legend();
    let pb = ProgressBar::new(time.as_secs())
        .with_style(ProgressStyle::default_bar().template("{msg} {wide_bar} {percent}%"));
    while simulation.scheduler.time() < time {
        let end = !simulation.step();
        let time = simulation.scheduler.time();
        let query_log = simulation
            .state
            .get_mut(key)
            .expect("Missing query log in state");
        let secs = time.as_secs();
        if pb.position() < secs {
            pb.set_position(secs);
            pb.set_message(&format!(
                "[{time}s] \
                 [{waiting_label}={waiting}] \
                 [{active_label}={active}] \
                 [{dropped_label}={dropped}] \
                 [{finished_label}={finished}] \
                 [{current_throughput_label}={current}] \
                 [{overall_throughput_label}={total}]",
                time = time.as_secs(),
                active = query_log.active_requests(),
                finished = query_log.finished_requests(),
                dropped = query_log.dropped_requests(),
                waiting = query_log.waiting_requests(),
                current = query_log.current_throughput().round(),
                total = query_log.total_throughput().round(),
                waiting_label = Label::Waiting.short(),
                active_label = Label::Active.short(),
                dropped_label = Label::Dropped.short(),
                finished_label = Label::Finished.short(),
                current_throughput_label = Label::CurrentThroughput.short(),
                overall_throughput_label = Label::OverallThroughput.short(),
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_deserialize_event() {
        let event = TimedEvent {
            event: Event::Broker(BrokerEvent::NewRequest(QueryRequest::new(
                RequestId::from(0),
                QueryId::from(2),
                Duration::from_micros(1),
            ))),
            time: Duration::from_micros(1),
        };
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{
    "event": {
        "component":"broker",
        "type":"new_request",
        "request_id":0,
        "query_id":2,
        "entry_time":1
    },
    "time":1
}"#
            .chars()
            .filter(|c| !c.is_ascii_whitespace())
            .collect::<String>()
        );
        assert_eq!(
            rmp_serde::from_read_ref::<_, TimedEvent>(&rmp_serde::to_vec(&event).unwrap()).unwrap(),
            event
        );
    }
}
