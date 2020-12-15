use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::io;
use std::sync::mpsc::{Receiver, Sender};
use std::time::Duration;

use serde::ser::{SerializeSeq, Serializer};
use serde::Serialize;
use simrs::ClockRef;

use crate::{BrokerRequest, NodeResponse, QueryRequest, QueryResponse, RequestId};

/// Stores the log of queries at different stages of simulation.
#[derive(Serialize)]
pub struct QueryLog {
    #[serde(serialize_with = "serialize_hash_map_values")]
    generated: HashMap<RequestId, QueryRequest>,
    #[serde(serialize_with = "serialize_hash_map_values")]
    dropped: HashMap<RequestId, QueryRequest>,
    #[serde(serialize_with = "serialize_hash_map_values")]
    responses: HashMap<RequestId, QueryResponse>,
    #[serde(serialize_with = "serialize_hash_map_values")]
    dispatched: HashMap<RequestId, BrokerRequest>,
    #[serde(skip_serializing)]
    recent: RefCell<BinaryHeap<(Reverse<Duration>, RequestId)>>,
    #[serde(skip_serializing)]
    current_interval: Duration,
    #[serde(skip_serializing)]
    clock: ClockRef,
    #[serde(skip_serializing)]
    fail_on_removing: bool,
    #[serde(skip_serializing)]
    query_sender: Option<Sender<String>>,
    #[serde(skip_serializing)]
    node_sender: Option<Sender<String>>,
    #[serde(skip_serializing)]
    flushed_responses: usize,
}

/// Serializes the values of a hash map, completely disregarding the keys.
/// This is useful for structures that need quick access by ID, but also contain the ID within
/// values.
fn serialize_hash_map_values<K, V, S>(map: &HashMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    V: Serialize,
{
    let mut seq = serializer.serialize_seq(Some(map.len()))?;
    for val in map.values() {
        seq.serialize_element(val)?;
    }
    seq.end()
}

impl QueryLog {
    /// Constructs a new query log.
    ///
    /// The value of `current_interval` determines how long into the past we will look to calculate
    /// the *current* throughput. The query log still stores all past queries, but the ones within
    /// the interval are optimized for faster throughput computation.
    ///
    /// `clock` is the reference to the simulation clock.
    #[must_use]
    pub fn new(clock: ClockRef, current_interval: Duration) -> Self {
        Self {
            generated: HashMap::new(),
            dropped: HashMap::new(),
            responses: HashMap::new(),
            dispatched: HashMap::new(),
            recent: RefCell::new(BinaryHeap::new()),
            current_interval,
            clock,
            fail_on_removing: true,
            query_sender: None,
            node_sender: None,
            flushed_responses: 0,
        }
    }

    /// Register a query sender for flushing query log to a file right away.
    pub fn query_sender(mut self, sender: Sender<String>) -> Self {
        sender
            .send(String::from(QueryResponse::csv_header()))
            .expect("channel closed");
        self.query_sender = Some(sender);
        self
    }

    /// Register a node sender for flushing node-level data to a file right away.
    pub fn node_sender(mut self, sender: Sender<String>) -> Self {
        sender
            .send(String::from(NodeResponse::csv_header()))
            .expect("channel closed");
        self.node_sender = Some(sender);
        self
    }

    /// The average number of finished queries per second for the entire time of the simulation.
    #[allow(clippy::cast_precision_loss)]
    pub fn total_throughput(&self) -> f64 {
        if self.responses.is_empty() || self.clock.time() == Duration::default() {
            0.0
        } else {
            self.finished_requests() as f64 / self.clock.time().as_secs_f64()
        }
    }

    /// The average number of finished queries per second for the most recent time interval.
    #[allow(clippy::cast_precision_loss)]
    pub fn current_throughput(&mut self) -> f64 {
        self.pop_old();
        let recent = self.recent.borrow();
        if recent.is_empty() || self.clock.time() == Duration::default() {
            0.0
        } else {
            let recent_interval = std::cmp::min(self.current_interval, self.clock.time());
            recent.len() as f64 / recent_interval.as_secs_f64()
        }
    }

    /// Inserts a new query request.
    pub fn new_request(&mut self, request: QueryRequest) {
        self.generated.insert(request.request_id(), request);
    }

    /// Inserts a new query request.
    pub fn drop_request(&mut self, request: QueryRequest) {
        if self.generated.remove(&request.request_id()).is_none() && self.fail_on_removing {
            panic!("Tried to drop unknown request.");
        }
        self.dropped.insert(request.request_id(), request);
    }

    /// Changes the query request from newly generated to dispatched.
    pub fn dispatch_request(&mut self, request: BrokerRequest) {
        let request_id = request.request_id();
        if self.generated.remove(&request_id).is_none() && self.fail_on_removing {
            panic!("Tried to dispatch unknown request.");
        }
        self.dispatched.insert(request_id, request);
    }

    /// Inserts a finished query response.
    pub fn finish(&mut self, response: QueryResponse) {
        let request_id = response.request_id();
        let generation_time = response.generation_time();
        if self.dispatched.remove(&request_id).is_none() && self.fail_on_removing {
            panic!("Tried to finish unknown request.");
        }
        self.pop_old();
        self.responses.insert(request_id, response);
        self.recent
            .borrow_mut()
            .push((Reverse(generation_time), request_id));
    }

    /// Returns the number of query requests that have been dropped.
    pub fn dropped_requests(&self) -> usize {
        self.dropped.len()
    }

    /// Returns the number of query requests currently waiting to be started.
    pub fn waiting_requests(&self) -> usize {
        self.generated.len()
    }

    /// Returns the number of currently active queries, i.e., any queries that have entered the
    /// system but have not been processed or dropped.
    pub fn active_requests(&self) -> usize {
        self.dispatched.len()
    }

    /// Returns the number of successfully finished query requests.
    pub fn finished_requests(&self) -> usize {
        self.responses.len() + self.flushed_responses
    }

    fn pop_old(&mut self) {
        let mut recent = self.recent.borrow_mut();
        let mut popped = Vec::<RequestId>::new();
        while let Some((Reverse(generation_time), request_id)) = recent.peek().copied() {
            if generation_time + self.current_interval >= self.clock.time() {
                break;
            }
            recent.pop();
            popped.push(request_id);
        }
        drop(recent);
        for request_id in popped {
            self.flush_request(request_id);
        }
    }

    fn flush_request(&mut self, request_id: RequestId) {
        if let Some(sender) = &self.query_sender {
            let response = self
                .responses
                .remove(&request_id)
                .expect("entry must exist");
            sender
                .send(response.to_csv_record())
                .expect("channel dropped");
            self.flushed_responses += 1;
            if let Some(sender) = &self.node_sender {
                for response in response.received_responses() {
                    sender
                        .send(response.to_csv_record())
                        .expect("channel dropped");
                }
            }
        }
    }
}

impl Drop for QueryLog {
    fn drop(&mut self) {
        if self.query_sender.is_some() {
            let mut recent = self.recent.borrow_mut();
            let mut popped = Vec::<RequestId>::new();
            while let Some((_, request_id)) = recent.peek().copied() {
                recent.pop();
                popped.push(request_id);
            }
            drop(recent);
            for request_id in popped {
                self.flush_request(request_id);
            }
        }
    }
}

/// Writes messages sent to the receiver until the channel is closed or an error occurred.
pub fn write_from_channel<W, T>(mut writer: W, receiver: Receiver<T>)
where
    W: io::Write + Send + 'static,
    T: AsRef<[u8]> + Send + 'static,
{
    std::thread::spawn(move || {
        while let Ok(msg) = receiver.recv() {
            if writer.write_all(msg.as_ref()).is_err() {
                eprintln!("error writing to file");
                break;
            }
        }
    });
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{BrokerRequest, NodeId, NodeRequest, NodeResponse, QueryId, QueryRequest, ShardId};

    use std::cell::Cell;
    use std::rc::Rc;

    use rstest::{fixture, rstest};

    #[test]
    fn test_clock() {
        let clock = Rc::new(Cell::new(Duration::default()));
        let log = QueryLog::new(Rc::clone(&clock).into(), Duration::from_secs(3));

        assert_eq!(log.clock.time(), clock.get());
        clock.replace(Duration::from_secs(1));
        assert_eq!(log.clock.time(), clock.get());
        clock.replace(Duration::from_secs(2));
        assert_eq!(log.clock.time(), clock.get());
    }

    fn make_response(id: usize, secs: u64) -> QueryResponse {
        QueryResponse::new(
            Rc::new(BrokerRequest::new(
                QueryRequest::new(RequestId(id), QueryId(0), Duration::from_secs(secs)),
                Duration::default(),
            )),
            Vec::new(),
            Vec::new(),
            Duration::default(),
        )
    }

    #[fixture]
    fn log() -> QueryLog {
        let clock = Rc::new(Cell::new(Duration::default()));
        let log = QueryLog::new(clock.into(), Duration::from_secs(3));
        log
    }

    #[rstest]
    fn test_request_pipeline_dropped(mut log: QueryLog) {
        let query_request =
            QueryRequest::new(RequestId::from(0), QueryId::from(0), Duration::default());
        log.new_request(query_request);
        log.drop_request(query_request);
    }

    #[rstest]
    fn test_request_pipeline_finished(mut log: QueryLog) {
        assert_eq!(log.active_requests(), 0);
        assert_eq!(log.dropped_requests(), 0);
        assert_eq!(log.waiting_requests(), 0);
        assert_eq!(log.finished_requests(), 0);

        let query_request =
            QueryRequest::new(RequestId::from(0), QueryId::from(0), Duration::default());
        log.new_request(query_request);

        assert_eq!(log.active_requests(), 0);
        assert_eq!(log.dropped_requests(), 0);
        assert_eq!(log.waiting_requests(), 1);
        assert_eq!(log.finished_requests(), 0);

        let broker_request = BrokerRequest::new(query_request, Duration::default());
        log.dispatch_request(broker_request);

        assert_eq!(log.active_requests(), 1);
        assert_eq!(log.dropped_requests(), 0);
        assert_eq!(log.waiting_requests(), 0);
        assert_eq!(log.finished_requests(), 0);

        let query_response = QueryResponse::new(
            Rc::new(broker_request),
            Vec::new(),
            Vec::new(),
            Duration::default(),
        );
        log.finish(query_response);

        assert_eq!(log.active_requests(), 0);
        assert_eq!(log.dropped_requests(), 0);
        assert_eq!(log.waiting_requests(), 0);
        assert_eq!(log.finished_requests(), 1);
    }

    mod fail_on_not_found {
        use super::*;

        #[rstest]
        #[should_panic]
        fn drop(mut log: QueryLog) {
            log.drop_request(QueryRequest::new(
                RequestId::from(0),
                QueryId::from(0),
                Duration::default(),
            ));
        }

        #[rstest]
        #[should_panic]
        fn dispatch(mut log: QueryLog) {
            log.dispatch_request(BrokerRequest::new(
                QueryRequest::new(RequestId::from(0), QueryId::from(0), Duration::default()),
                Duration::default(),
            ));
        }

        #[rstest]
        #[should_panic]
        fn finish(mut log: QueryLog) {
            log.finish(make_response(0, 0))
        }
    }

    #[test]
    fn test_threshold() {
        let clock = Rc::new(Cell::new(Duration::default()));
        let mut log = QueryLog::new(Rc::clone(&clock).into(), Duration::from_secs(3));
        log.fail_on_removing = false;
        assert_eq!(log.total_throughput(), 0.0);
        log.finish(make_response(0, 0));
        clock.replace(Duration::from_secs(1));
        assert_eq!(log.total_throughput(), 1.0);
        assert_eq!(log.current_throughput(), 1.0);
        clock.replace(Duration::from_secs(2));
        assert_eq!(log.total_throughput(), 0.5);
        assert_eq!(log.current_throughput(), 0.5);
        log.finish(make_response(1, 1));
        assert_eq!(log.total_throughput(), 1.0);
        assert_eq!(log.current_throughput(), 1.0);
        log.finish(make_response(2, 2));
        assert_eq!(log.total_throughput(), 1.5);
        assert_eq!(log.current_throughput(), 1.5);
        clock.replace(Duration::from_secs(3));
        assert_eq!(log.total_throughput(), 1.0);
        assert_eq!(log.current_throughput(), 1.0);
        clock.replace(Duration::from_secs(4));
        float_cmp::approx_eq!(f64, log.total_throughput(), 3.0 / 4.0);
        float_cmp::approx_eq!(f64, log.current_throughput(), 2.0 / 4.0);
        clock.replace(Duration::from_secs(5));
        float_cmp::approx_eq!(f64, log.total_throughput(), 3.0 / 5.0);
        float_cmp::approx_eq!(f64, log.current_throughput(), 1.0 / 5.0);
        clock.replace(Duration::from_secs(6));
        float_cmp::approx_eq!(f64, log.total_throughput(), 3.0 / 6.0);
        assert_eq!(log.current_throughput(), 0.0);
    }

    #[rstest]
    fn test_serialize() {
        let query_request =
            QueryRequest::new(RequestId::from(0), QueryId::from(0), Duration::default());
        assert_eq!(
            &serde_json::to_string(&query_request).unwrap(),
            r#"{"request_id":0,"query_id":0,"entry_time":0}"#
        );
        let broker_request = BrokerRequest::new(query_request, Duration::from_micros(1));
        assert_eq!(
            &serde_json::to_string(&broker_request).unwrap(),
            r#"{"request_id":0,"query_id":0,"entry_time":0,"broker_time":1}"#
        );
        let node_request = NodeRequest::new(
            Rc::new(broker_request.clone()),
            ShardId::from(12),
            Duration::from_micros(2),
        );
        assert_eq!(
            &serde_json::to_string(&node_request).unwrap(),
            r#"{"shard_id":12,"request_id":0,"query_id":0,"entry_time":0,"broker_time":1,"dispatch_time":2}"#
        );
        let node_response = NodeResponse::new(
            node_request.clone(),
            NodeId::from(7),
            Duration::from_micros(3),
            Duration::from_micros(4),
        );
        assert_eq!(
            &serde_json::to_string(&node_response).unwrap(),
            r#"{"shard_id":12,"request_id":0,"query_id":0,"entry_time":0,"broker_time":1,"dispatch_time":2,"node_id":7,"node_start_time":3,"node_end_time":4}"#
        );
        let query_response = QueryResponse::new(
            Rc::new(broker_request),
            vec![node_response],
            vec![node_request],
            Duration::from_micros(5),
        );
        assert_eq!(
            &serde_json::to_string(&query_response).unwrap(),
            r#"{"request_id":0,"query_id":0,"entry_time":0,"broker_time":1,"dispatch_time":2,"response_time":5}"#
        );
        assert_eq!(&query_response.to_csv_record(), "0,0,0,1,2,5");
    }

    // #[rstest]
    // fn test_serialize_query_log(mut log: QueryLog) {
    //     let query_request =
    //         QueryRequest::new(RequestId::from(0), QueryId::from(0), Duration::default());
    //     log.new_request(query_request);
    //     let broker_request = BrokerRequest::new(query_request, Duration::default());
    //     log.dispatch_request(broker_request);
    //     let query_response = QueryResponse::new(
    //         Rc::new(broker_request),
    //         Vec::new(),
    //         Vec::new(),
    //         Duration::default(),
    //     );
    //     log.finish(query_response);
    //     let serialized = serde_json::to_string(&log).unwrap();
    //     assert_eq!(&serialized, "");
    // }
}
