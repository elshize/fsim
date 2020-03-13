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

    /// Changes the status of `query` to picked up at `time`.
    pub fn pick_up_query(&mut self, query: Query, time: Duration) {
        self.active_query(&query).pick_up(time);
    }

    /// Changes the status of `query` to dispatched to `num_shards` shards at `time`.
    pub fn dispatch_query(&mut self, query: Query, time: Duration, num_shards: usize) {
        self.active_query(&query).dispatch(time, num_shards);
    }

    /// Records that one node has finished processing.
    pub fn finish_shard(&mut self, query: Query, time: Duration) {
        if self.active_query(&query).finish_shard(time).is_finished() {
            self.past_queries
                .insert(query, self.active_queries.remove(&query).unwrap());
        }
    }

    /// Records a dropped shard request.
    pub fn drop_shard(&mut self, query: Query, time: Duration) {
        if self.active_query(&query).drop_shard(time).is_finished() {
            self.past_queries
                .insert(query, self.active_queries.remove(&query).unwrap());
        }
    }

    fn retire_query(&mut self, query: Query) {
        match self.active_queries.remove(&query).unwrap() {
            status @ QueryStatus::Finished(_) => {
                self.past_queries.insert(query, status);
            }
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_default() {
        let status = Status::default();
        assert_eq!(status.time(), Duration::new(0, 0));
        assert_eq!(status.queries_entered(), 0);
        assert_eq!(status.queries_finished(), 0);
        assert_eq!(status.queries_active(), 0);
    }
}
