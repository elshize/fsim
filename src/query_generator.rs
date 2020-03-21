use crate::{process::Runnable, query::Query, Effect, Event, Process, QueryId, RequestId};
use log::trace;
use rand::distributions::Distribution;
use rand::RngCore;
use std::cell::{Cell, RefCell};
use std::time::Duration;

/// Entry points to the query generator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryGeneratorStage {
    /// Generate and yield a query to be put into the incoming queue.
    Generate,
    /// Wait a random time (from a given distribution) until generating again.
    Timeout,
}

/// Process that generates queries at an interval given by a distribution.
/// Queries are drawn from a set and distribution given at construction.
pub struct QueryGenerator<'a, T, Q, F>
where
    T: Distribution<f32>,
    Q: Distribution<usize>,
    F: Fn(u64) -> Duration,
{
    /// All available queries.
    queries: Vec<QueryId>,
    /// PRNG
    rand: RefCell<Box<dyn RngCore + 'a>>,
    /// Time distribution.
    time_dist: T,
    /// Query distribution.
    query_dist: Q,

    duration_from_u64: F,
    counter: Cell<usize>,
}

impl<'a, T, Q, F> QueryGenerator<'a, T, Q, F>
where
    T: Distribution<f32>,
    Q: Distribution<usize>,
    F: Fn(u64) -> Duration,
{
    /// Constructs a new query generator, typically only once per simulation.
    ///
    /// A generator takes a list of queries, random number generator `rng`, and two distributions:
    /// - distribution of time intervals between generating a query,
    /// - distribution of selected queries (usually uniform across all queries).
    pub fn new<R: RngCore + 'static>(
        queries: Vec<QueryId>,
        rng: R,
        time_dist: T,
        query_dist: Q,
        duration_from_u64: F,
    ) -> Self {
        Self {
            queries,
            rand: RefCell::new(Box::new(rng)),
            time_dist,
            query_dist,
            duration_from_u64,
            counter: Cell::new(0),
        }
    }
}

impl<'a, TimeDist, QueryDist, F> Runnable for QueryGenerator<'a, TimeDist, QueryDist, F>
where
    TimeDist: Distribution<f32>,
    QueryDist: Distribution<usize>,
    F: Fn(u64) -> Duration,
{
    type Payload = QueryGeneratorStage;
    type Effect = Effect<'a>;

    fn run(&self, entry: Self::Payload) -> Self::Effect {
        use std::ops::DerefMut;
        use Process::QueryGenerator;
        use QueryGeneratorStage::*;
        match entry {
            Generate => {
                let query_idx = self.query_dist.sample(self.rand.borrow_mut().deref_mut());
                trace!("Query {} entering incoming queue", query_idx);
                let request_id = self.counter.get();
                self.counter.set(request_id + 1);
                Effect::QueryQueuePut(
                    Query {
                        id: self.queries[query_idx],
                        request: RequestId(request_id),
                    },
                    QueryGenerator(Timeout),
                )
            }
            Timeout => {
                let timeout = self.time_dist.sample(self.rand.borrow_mut().deref_mut());
                let timeout = match timeout.partial_cmp(&0_f32) {
                    None | Some(std::cmp::Ordering::Less) => 0_f32,
                    _ => timeout,
                };
                let timeout = (self.duration_from_u64)(timeout.round() as u64);
                trace!("Scheduling new query in {:?}", timeout);
                Effect::Schedule(Event::new(timeout, QueryGenerator(Generate)))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::{ToFloatDistribution, WrappingEchoDistribution};
    use rand::distributions::Uniform;
    use rand::rngs::mock::StepRng;
    use QueryGeneratorStage::{Generate, Timeout};

    #[test]
    fn test_generate() {
        let query_generator = QueryGenerator::new(
            vec![
                QueryId(0),
                QueryId(1),
                QueryId(2),
                QueryId(3),
                QueryId(4),
                QueryId(5),
                QueryId(6),
            ],
            StepRng::new(0, 1),
            Uniform::from(0.0..100.0),
            WrappingEchoDistribution::new(7),
            Duration::from_secs,
        );
        for iteration in 0..100 {
            if let Effect::QueryQueuePut(query, resume) = query_generator.run(Generate) {
                assert_eq!(resume, Process::QueryGenerator(Timeout));
                assert_eq!(query.id, QueryId(iteration % 7));
                assert_eq!(query.request, RequestId(iteration));
            } else {
                panic!("Generate must return QueryQueuePut effect.");
            }
        }
    }

    #[test]
    fn test_timeout() {
        let query_generator = QueryGenerator::new(
            vec![],
            StepRng::new(0, 2),
            ToFloatDistribution(WrappingEchoDistribution::new(10000)),
            WrappingEchoDistribution::new(7),
            Duration::from_secs,
        );
        for iteration in 0..100 {
            if let Effect::Schedule(Event { time, process }) = query_generator.run(Timeout) {
                assert_eq!(time, Duration::from_secs(iteration * 2));
                assert_eq!(process, Process::QueryGenerator(Generate));
            } else {
                panic!("Timeout must return Schedule effect.");
            }
        }
    }
}
