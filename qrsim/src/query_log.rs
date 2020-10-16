use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::time::Duration;

use sim20::ClockRef;

use crate::{BrokerRequest, QueryRequest, QueryResponse, RequestId};

/// Stores the log of queries.
pub struct QueryLog {
    generated: HashMap<RequestId, QueryRequest>,
    dropped: HashMap<RequestId, QueryRequest>,
    responses: HashMap<RequestId, QueryResponse>,
    dispatched: HashMap<RequestId, BrokerRequest>,
    recent: RefCell<BinaryHeap<(Reverse<Duration>, RequestId)>>,
    current_interval: Duration,
    clock: ClockRef,
}

impl QueryLog {
    /// Constructs a new query log.
    ///
    /// The value of `current_interval` determines how long into the past we will look to calculate
    /// the *current* throughput. The query log still stores all past queries, but the ones within
    /// the interval are optimized for faster threshold computation.
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
        }
    }

    /// The average number of finished queries per second for the entire time of the simulation.
    #[allow(clippy::cast_precision_loss)]
    pub fn total_throughput(&self) -> f64 {
        if self.responses.is_empty() || self.clock.time() == Duration::default() {
            0.0
        } else {
            self.responses.len() as f64 / self.clock.time().as_secs_f64()
        }
    }

    /// The average number of finished queries per second for the most recent time interval.
    #[allow(clippy::cast_precision_loss)]
    pub fn current_throughput(&self) -> f64 {
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
        self.generated.remove(&request.request_id());
        self.dropped.insert(request.request_id(), request);
    }

    /// Changes the query request from newly generated to dispatched.
    pub fn dispatch_request(&mut self, request: BrokerRequest) {
        let request_id = request.request_id();
        self.generated.remove(&request_id);
        self.dispatched.insert(request_id, request);
    }

    /// Inserts a finished query response.
    pub fn finish(&mut self, response: QueryResponse) {
        let request_id = response.request_id();
        let generation_time = response.generation_time();
        self.pop_old();
        self.responses.insert(request_id, response);
        self.recent
            .borrow_mut()
            .push((Reverse(generation_time), request_id));
        self.dispatched.remove(&request_id).expect("XXX");
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
        self.responses.len()
    }

    // /// Returns the number of queries that are currently dispatched to nodes.
    // pub fn dispatched_requests(&self) -> usize {
    //     self.dispatched.len()
    // }

    fn pop_old(&self) {
        let mut recent = self.recent.borrow_mut();
        while let Some((Reverse(generation_time), _)) = recent.peek().copied() {
            log::trace!("TIME {:?} v. {:?}", generation_time, self.clock.time());
            if generation_time + self.current_interval >= self.clock.time() {
                break;
            }
            recent.pop();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::cell::Cell;
    use std::rc::Rc;

    use crate::{BrokerRequest, QueryId, QueryRequest};

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

    #[test]
    fn test_threshold() {
        let clock = Rc::new(Cell::new(Duration::default()));
        let mut log = QueryLog::new(Rc::clone(&clock).into(), Duration::from_secs(3));
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
}
