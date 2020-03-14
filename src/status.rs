use crate::{Query, QueryStatus};
use std::collections::BTreeMap;
use std::time::Duration;

/// Simulation status that can be printed/stored at any step of a simulation.
pub struct Status {
    time: Duration,
    queries_entered: usize,
    queries_finished: usize,
    queries_incomplete: usize,
    active_queries: BTreeMap<Query, QueryStatus>,
    past_queries: BTreeMap<Query, QueryStatus>,
}

impl Default for Status {
    fn default() -> Self {
        Self {
            time: Duration::new(0, 0),
            queries_entered: 0,
            queries_finished: 0,
            queries_incomplete: 0,
            active_queries: BTreeMap::new(),
            past_queries: BTreeMap::new(),
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
        self.queries_entered
    }

    /// Returns how many queries have finished processing.
    pub fn queries_finished(&self) -> usize {
        self.queries_finished
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

    fn active_query(&mut self, query: &Query) -> &mut QueryStatus {
        self.active_queries
            .get_mut(&query)
            .expect("Request ID out of bounds")
    }

    /// Logs a new query that entered into the system at `time`.
    pub fn enter_query(&mut self, query: Query, time: Duration) {
        self.queries_entered += 1;
        self.active_queries.insert(query, QueryStatus::new(time));
    }

    fn update<F>(status: &mut QueryStatus, action: F) -> &QueryStatus
    where
        F: Fn(&QueryStatus) -> QueryStatus,
    {
        *status = action(status);
        status
    }

    /// Changes the status of `query` to picked up at `time`.
    pub fn pick_up_query(&mut self, query: Query, time: Duration) {
        Self::update(self.active_query(&query), |s| s.pick_up(time));
    }

    /// Changes the status of `query` to dispatched to `num_shards` shards at `time`.
    pub fn dispatch_query(&mut self, query: Query, time: Duration, num_shards: usize) {
        Self::update(self.active_query(&query), |s| s.dispatch(time, num_shards));
    }

    /// Records that one node has finished processing.
    pub fn finish_shard(&mut self, query: Query, time: Duration) {
        if Self::update(self.active_query(&query), |s| s.finish_shard(time)).is_finished() {
            self.retire_query(query);
        }
    }

    /// Records a dropped shard request.
    pub fn drop_shard(&mut self, query: Query, time: Duration) {
        if Self::update(self.active_query(&query), |s| s.drop_shard(time)).is_finished() {
            self.retire_query(query);
        }
    }

    fn retire_query(&mut self, query: Query) {
        let status = self.active_queries.remove(&query).unwrap();
        if status.num_dropped() > 0 {
            self.queries_incomplete += 1;
        }
        self.queries_finished += 1;
        self.past_queries.insert(query, status);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{QueryId, RequestId};

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
        let mut status = Status::default();
        status.enter_query(q1, secs(1));
        assert_eq!(status.queries_entered(), 1);
        status.enter_query(q2, secs(2));
        assert_eq!(status.queries_entered(), 2);
        assert_eq!(
            status
                .active_queries
                .clone()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![
                (q1, QueryStatus::new(secs(1))),
                (q2, QueryStatus::new(secs(2)))
            ]
        );
        status.pick_up_query(q1, secs(3));
        assert_eq!(status.queries_entered(), 2);
        assert_eq!(
            status
                .active_queries
                .clone()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![
                (q1, QueryStatus::new(secs(1)).pick_up(secs(3))),
                (q2, QueryStatus::new(secs(2)))
            ]
        );
        status.dispatch_query(q1, secs(3), 2);
        assert_eq!(status.queries_entered(), 2);
        assert_eq!(
            status
                .active_queries
                .clone()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![
                (
                    q1,
                    QueryStatus::new(secs(1))
                        .pick_up(secs(3))
                        .dispatch(secs(3), 2)
                ),
                (q2, QueryStatus::new(secs(2)))
            ]
        );
        status.finish_shard(q1, secs(4));
        status.drop_shard(q1, secs(5));
        assert_eq!(status.queries_entered(), 2);
        assert_eq!(status.queries_active(), 1);
        assert_eq!(status.queries_finished(), 1);
        assert_eq!(status.queries_incomplete(), 1);
        assert_eq!(
            status
                .active_queries
                .clone()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![(q2, QueryStatus::new(secs(2)))]
        );
        assert_eq!(
            status.past_queries.clone().into_iter().collect::<Vec<_>>(),
            vec![(
                q1,
                QueryStatus::new(secs(1))
                    .pick_up(secs(3))
                    .dispatch(secs(3), 2)
                    .finish_shard(secs(4))
                    .drop_shard(secs(5))
            )]
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
            status.past_queries.clone().into_iter().collect::<Vec<_>>(),
            vec![
                (
                    q1,
                    QueryStatus::new(secs(1))
                        .pick_up(secs(3))
                        .dispatch(secs(3), 2)
                        .finish_shard(secs(4))
                        .drop_shard(secs(5))
                ),
                (
                    q2,
                    QueryStatus::new(secs(2))
                        .pick_up(secs(5))
                        .dispatch(secs(6), 2)
                        .finish_shard(secs(7))
                        .finish_shard(secs(8))
                )
            ]
        );
    }
}
