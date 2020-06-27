use crate::simulation::{Query, QueryStatus};
use im_rc::{HashMap, Vector};
use std::time::Duration;

#[derive(Clone)]
/// Immutable version of `Status`.
pub struct Status {
    time: Duration,
    queries_incomplete: usize,
    active_queries: HashMap<Query, QueryStatus>,
    past_queries: HashMap<Query, QueryStatus>,
    logs: Vector<String>,
}

impl Status {
    /// Iterates over log events.
    pub fn logs<'a>(&'a self) -> Box<dyn DoubleEndedIterator<Item = &'a String> + 'a> {
        Box::new(self.logs.iter())
    }

    /// Iterates over finished queries.
    pub fn finished<'a>(&'a self) -> Box<dyn Iterator<Item = (Query, QueryStatus)> + 'a> {
        Box::new(self.past_queries.iter().map(|(&q, &s)| (q, s)))
    }
}

impl Default for Status {
    fn default() -> Self {
        Self {
            time: Duration::new(0, 0),
            queries_incomplete: 0,
            active_queries: HashMap::new(),
            past_queries: HashMap::new(),
            logs: Vector::new(),
        }
    }
}

impl crate::simulation::Status for Status {
    fn time(&self) -> Duration {
        self.time
    }

    fn queries_entered(&self) -> usize {
        self.queries_active() + self.queries_finished()
    }

    fn queries_finished(&self) -> usize {
        self.past_queries.len()
    }

    fn queries_active(&self) -> usize {
        self.active_queries.len()
    }

    fn queries_incomplete(&self) -> usize {
        self.queries_incomplete
    }

    fn active<'a>(&'a self) -> Box<dyn Iterator<Item = (Query, QueryStatus)> + 'a> {
        Box::new(self.active_queries.iter().map(|(&q, &s)| (q, s)))
    }

    fn active_query(&self, query: &Query) -> &QueryStatus {
        self.active_queries
            .get(&query)
            .expect("Request ID out of bounds")
    }

    fn enter_query(&mut self, query: Query, time: Duration) {
        self.active_queries.insert(query, QueryStatus::new(time));
    }

    fn pick_up_query(&mut self, query: Query, time: Duration) {
        self.active_queries.entry(query).and_modify(|s| {
            *s = s.pick_up(time);
        });
    }

    fn dispatch_query(&mut self, query: Query, time: Duration, num_shards: usize) {
        self.active_queries.entry(query).and_modify(|s| {
            *s = s.dispatch(time, num_shards);
        });
    }

    fn finish_shard(&mut self, query: Query, time: Duration) {
        match self.active_query(&query).finish_shard(time) {
            s if s.is_finished() => {
                self.queries_incomplete += if s.num_dropped() > 0 { 1 } else { 0 };
                self.active_queries.remove(&query);
                self.past_queries.insert(query, s);
            }
            s => {
                self.active_queries.entry(query).and_modify(|old| *old = s);
            }
        }
    }

    fn drop_shard(&mut self, query: Query, time: Duration) {
        match self.active_query(&query).drop_shard(time) {
            s if s.is_finished() => {
                self.queries_incomplete += 1;
                self.active_queries.remove(&query);
                self.past_queries.insert(query, s);
            }
            s => {
                self.active_queries.entry(query).and_modify(|old| *old = s);
            }
        }
    }

    fn log_events<E>(&mut self, events: E)
    where
        E: Iterator<Item = String>,
    {
        self.logs.extend(events);
    }

    fn update_time(&mut self, time: Duration) {
        self.time = time;
    }
}

#[cfg(test)]
mod test {
    use crate::simulation::{Query, QueryStatus, Status};
    use crate::simulation::{QueryId, RequestId};
    use im_rc::hashmap;
    use std::time::Duration;

    #[test]
    fn test_default() {
        let status = super::Status::default();
        assert_eq!(status.time(), Duration::new(0, 0));
        assert_eq!(status.queries_entered(), 0);
        assert_eq!(status.queries_finished(), 0);
        assert_eq!(status.queries_active(), 0);
        assert_eq!(status.queries_incomplete(), 0);
        assert_eq!(status.queries_incomplete(), 0);
        assert!(status.logs().collect::<Vec<_>>().is_empty());
    }

    fn secs(secs: u64) -> Duration {
        Duration::from_secs(secs)
    }

    #[test]
    fn test_pipeline() {
        let q1 = Query::new(QueryId(1), RequestId(11));
        let q2 = Query::new(QueryId(10), RequestId(13));
        let mut status = super::Status::default();
        status.enter_query(q1, secs(1));
        assert_eq!(status.queries_entered(), 1);
        status.enter_query(q2, secs(2));
        assert_eq!(status.queries_entered(), 2);
        assert_eq!(
            status.active_queries,
            hashmap! {
                q1 => QueryStatus::new(secs(1)),
                q2 => QueryStatus::new(secs(2))
            }
        );
        status.pick_up_query(q1, secs(3));
        assert_eq!(status.queries_entered(), 2);
        assert_eq!(
            status.active_queries,
            hashmap! {
                q1 => QueryStatus::new(secs(1)).pick_up(secs(3)),
                q2 => QueryStatus::new(secs(2))
            }
        );
        status.dispatch_query(q1, secs(3), 2);
        assert_eq!(status.queries_entered(), 2);
        assert_eq!(
            status.active_queries,
            hashmap! {
                q1 => QueryStatus::new(secs(1))
                    .pick_up(secs(3))
                    .dispatch(secs(3), 2),
                q2 => QueryStatus::new(secs(2))
            }
        );
        status.finish_shard(q1, secs(4));
        status.drop_shard(q1, secs(5));
        assert_eq!(status.queries_entered(), 2);
        assert_eq!(status.queries_active(), 1);
        assert_eq!(status.queries_finished(), 1);
        assert_eq!(status.queries_incomplete(), 1);
        assert_eq!(
            status.active().collect::<im_rc::HashMap<_, _>>(),
            hashmap![q2 => QueryStatus::new(secs(2))]
        );
        assert_eq!(
            status.past_queries,
            hashmap! {
                q1 => QueryStatus::new(secs(1))
                    .pick_up(secs(3))
                    .dispatch(secs(3), 2)
                    .finish_shard(secs(4))
                    .drop_shard(secs(5))
            }
        );
        status.pick_up_query(q2, secs(5));
        status.dispatch_query(q2, secs(6), 2);
        status.finish_shard(q2, secs(7));
        status.finish_shard(q2, secs(8));
        assert_eq!(status.queries_entered(), 2);
        assert_eq!(status.queries_active(), 0);
        assert_eq!(status.queries_finished(), 2);
        assert_eq!(status.queries_incomplete(), 1);
        assert!(status.active_queries.is_empty());
        assert_eq!(
            status.past_queries,
            hashmap! {
                q1 => QueryStatus::new(secs(1))
                    .pick_up(secs(3))
                    .dispatch(secs(3), 2)
                    .finish_shard(secs(4))
                    .drop_shard(secs(5)),
                q2 => QueryStatus::new(secs(2))
                    .pick_up(secs(5))
                    .dispatch(secs(6), 2)
                    .finish_shard(secs(7))
                    .finish_shard(secs(8))
            }
        );
        status.log_events(vec![String::from("Log1"), String::from("Log2")].into_iter());
        assert_eq!(
            status.logs().cloned().collect::<Vec<_>>(),
            vec![String::from("Log1"), String::from("Log2")]
        );
        status.log_events(vec![String::from("Log3"), String::from("Log4")].into_iter());
        assert_eq!(
            status.logs().cloned().collect::<Vec<_>>(),
            vec![
                String::from("Log1"),
                String::from("Log2"),
                String::from("Log3"),
                String::from("Log4"),
            ]
        );
    }
}
