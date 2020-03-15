use crate::{Query, QueryStatus};
use im_rc::HashMap;
use std::time::Duration;

/// Immutable version of `Status`.
pub struct Status {
    time: Duration,
    queries_incomplete: usize,
    active_queries: HashMap<Query, QueryStatus>,
    past_queries: HashMap<Query, QueryStatus>,
}

impl Default for Status {
    fn default() -> Self {
        Self {
            time: Duration::new(0, 0),
            queries_incomplete: 0,
            active_queries: HashMap::new(),
            past_queries: HashMap::new(),
        }
    }
}

impl Status {
    /// Time elapsed from the beginning of the simulation.
    pub fn time(&self) -> Duration {
        self.time
    }

    /// Returns how many queries have entered the main incoming queue.
    pub fn queries_entered(&self) -> usize {
        self.queries_active() + self.queries_finished()
    }

    /// Returns how many queries have finished processing.
    pub fn queries_finished(&self) -> usize {
        self.past_queries.len()
    }

    /// Active queries are these that have entered the system but have yet to finish, and have not
    /// been dropped.
    pub fn queries_active(&self) -> usize {
        self.active_queries.len()
    }

    /// Number of queries that have been finished but some shard requests have been dropped.
    pub fn queries_incomplete(&self) -> usize {
        self.queries_incomplete
    }

    fn active_query(&self, query: &Query) -> &QueryStatus {
        self.active_queries
            .get(&query)
            .expect("Request ID out of bounds")
    }

    fn alter<F>(&self, query: Query, f: F) -> HashMap<Query, QueryStatus>
    where
        F: FnOnce(QueryStatus) -> QueryStatus,
    {
        self.active_queries.alter(move |v| v.map(f), query)
    }

    /// Logs a new query that entered into the system at `time`.
    pub fn enter_query(&self, query: Query, time: Duration) -> Self {
        Self {
            time,
            active_queries: self.active_queries.clone()
                + HashMap::unit(query, QueryStatus::new(time)),
            past_queries: self.past_queries.clone(),
            ..*self
        }
    }

    /// Changes the status of `query` to picked up at `time`.
    pub fn pick_up_query(&self, query: Query, time: Duration) -> Self {
        Self {
            time,
            active_queries: self.alter(query, |s| s.pick_up(time)),
            past_queries: self.past_queries.clone(),
            ..*self
        }
    }

    /// Changes the status of `query` to dispatched to `num_shards` shards at `time`.
    pub fn dispatch_query(&self, query: Query, time: Duration, num_shards: usize) -> Self {
        Self {
            time,
            active_queries: self.alter(query, |s| s.dispatch(time, num_shards)),
            past_queries: self.past_queries.clone(),
            ..*self
        }
    }

    /// Records that one node has finished processing.
    pub fn finish_shard(&self, query: Query, time: Duration) -> Self {
        match self.active_query(&query).finish_shard(time) {
            s if s.is_finished() => {
                let queries_incomplete =
                    self.queries_incomplete + if s.num_dropped() > 0 { 1 } else { 0 };
                Self {
                    time,
                    active_queries: self.active_queries.without(&query),
                    past_queries: self.past_queries.clone() + HashMap::unit(query, s),
                    ..*self
                }
            }
            s => Self {
                time,
                active_queries: self.alter(query, |_| s),
                past_queries: self.past_queries.clone(),
                ..*self
            },
        }
    }

    /// Records a dropped shard request.
    pub fn drop_shard(&self, query: Query, time: Duration) -> Self {
        match self.active_query(&query).drop_shard(time) {
            s if s.is_finished() => {
                let queries_incomplete =
                    self.queries_incomplete + if s.num_dropped() > 0 { 1 } else { 0 };
                Self {
                    time,
                    active_queries: self.active_queries.without(&query),
                    past_queries: self.past_queries.clone() + HashMap::unit(query, s),
                    queries_incomplete,
                    ..*self
                }
            }
            s => Self {
                time,
                active_queries: self.alter(query, |_| s),
                past_queries: self.past_queries.clone(),
                ..*self
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{QueryId, RequestId};
    use im_rc::hashmap;

    #[test]
    fn test_default() {
        let status = Status::default();
        assert_eq!(status.time(), Duration::new(0, 0));
        assert_eq!(status.queries_entered(), 0);
        assert_eq!(status.queries_finished(), 0);
        assert_eq!(status.queries_active(), 0);
        assert_eq!(status.queries_incomplete(), 0);
        assert_eq!(status.queries_incomplete(), 0);
    }

    fn secs(secs: u64) -> Duration {
        Duration::from_secs(secs)
    }

    #[test]
    fn test_pipeline() {
        let q1 = Query::new(QueryId(1), RequestId(11));
        let q2 = Query::new(QueryId(10), RequestId(13));
        let status = Status::default();
        let status = status.enter_query(q1, secs(1));
        assert_eq!(status.queries_entered(), 1);
        let status = status.enter_query(q2, secs(2));
        assert_eq!(status.queries_entered(), 2);
        assert_eq!(
            status.active_queries,
            hashmap! {
                q1 => QueryStatus::new(secs(1)),
                q2 => QueryStatus::new(secs(2))
            }
        );
        let status = status.pick_up_query(q1, secs(3));
        assert_eq!(status.queries_entered(), 2);
        assert_eq!(
            status.active_queries,
            hashmap! {
                q1 => QueryStatus::new(secs(1)).pick_up(secs(3)),
                q2 => QueryStatus::new(secs(2))
            }
        );
        let status = status.dispatch_query(q1, secs(3), 2);
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
        let status = status.finish_shard(q1, secs(4));
        let status = status.drop_shard(q1, secs(5));
        assert_eq!(status.queries_entered(), 2);
        assert_eq!(status.queries_active(), 1);
        assert_eq!(status.queries_finished(), 1);
        assert_eq!(status.queries_incomplete(), 1);
        assert_eq!(
            status.active_queries,
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
        let status = status.pick_up_query(q2, secs(5));
        let status = status.dispatch_query(q2, secs(6), 2);
        let status = status.finish_shard(q2, secs(7));
        let status = status.finish_shard(q2, secs(8));
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
    }
}
