use crate::{BrokerEvent, QueryId, QueryLog, QueryRequest, RequestId};
use rand::Rng;
use rand_distr::Distribution;
use simrs::{Component, ComponentId, Key, QueueId, Scheduler, State};
use std::cell::{Cell, RefCell};
use std::convert::TryFrom;
use std::time::Duration;

/// Query generator has only one job and hence one event.
#[derive(Debug, Copy, Clone)]
pub enum Event {
    /// At this event, we generate a new query at random from available queries.
    /// Then, the generator schedules itself to run again after a time randomly selected
    /// from a given time interval distribution, which is a parameter of the generator.
    GenerateQuery,
}

/// Generates queries at regular intervals. See [`Event`].
pub struct QueryGenerator<R, T, Q>
where
    R: Rng,
    T: Distribution<f32>,
    Q: Distribution<usize>,
{
    rng: RefCell<R>,
    time_dist: T,
    query_dist: Q,
    counter: Cell<usize>,
    query_entry_queue: QueueId<QueryRequest>,
    broker: ComponentId<BrokerEvent>,
    // generated_query_listener: Option<Box<dyn Fn(&QueryRequest) -> ()>>,
    query_log_key: Key<QueryLog>,
}

impl<R, TD, QD> Component for QueryGenerator<R, TD, QD>
where
    R: Rng,
    TD: Distribution<f32>,
    QD: Distribution<usize>,
{
    type Event = Event;

    fn process_event(
        &self,
        self_id: ComponentId<Self::Event>,
        event: &Self::Event,
        scheduler: &mut Scheduler,
        state: &mut State,
    ) {
        match event {
            Event::GenerateQuery => {
                let request = self.generate_request(scheduler.time());
                log::info!("Generated query at {:?}", scheduler.time());
                state
                    .get_mut(self.query_log_key)
                    .expect("Query log not found in state")
                    .new_request(request);
                if state.send(self.query_entry_queue, request).is_err() {
                    log::warn!(
                        "[{:?}] Request {} was dropped",
                        scheduler.time(),
                        request.request_id()
                    );
                    state
                        .get_mut(self.query_log_key)
                        .expect("Query log not found in state")
                        .drop_request(request);
                }
                scheduler.schedule(
                    Duration::from_micros(self.next_interval()),
                    self_id,
                    Event::GenerateQuery,
                );
                scheduler.schedule_immediately(self.broker, super::broker::Event::NewRequest);
            }
        }
    }
}

impl<R, T, Q> QueryGenerator<R, T, Q>
where
    R: Rng,
    T: Distribution<f32>,
    Q: Distribution<usize>,
{
    /// Creates a new query generator with given distributions for selecting queries and time
    /// intervals. A generator also takes the ID of the broker to schedule its events, and a query
    /// queue to send the generated queries to.
    pub fn new(
        rng: R,
        time_dist: T,
        query_dist: Q,
        query_entry_queue: QueueId<QueryRequest>,
        broker: ComponentId<BrokerEvent>,
        query_log_key: Key<QueryLog>,
    ) -> Self {
        Self {
            rng: RefCell::new(rng),
            time_dist,
            query_dist,
            counter: Cell::new(0),
            query_entry_queue,
            broker,
            query_log_key,
        }
    }

    fn generate_request(&self, time: Duration) -> QueryRequest {
        let query_id = QueryId(self.query_dist.sample(&mut *self.rng.borrow_mut()));
        let request_id = self.counter.get();
        self.counter.replace(request_id + 1);
        QueryRequest::new(RequestId(request_id), query_id, time)
    }

    fn next_interval(&self) -> u64 {
        let timeout = self.time_dist.sample(&mut *self.rng.borrow_mut());
        let timeout = match timeout.partial_cmp(&0_f32) {
            None | Some(std::cmp::Ordering::Less) => 0_f32,
            _ => timeout,
        };
        #[allow(clippy::cast_possible_truncation)]
        u64::try_from(timeout.round() as i64).unwrap()
    }
}
