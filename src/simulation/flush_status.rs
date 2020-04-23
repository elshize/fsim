use crate::simulation::{Query, QueryStatus, Status};
use serde_json::{json, to_writer};
use std::collections::HashMap;
use std::io::Write;
use std::time::Duration;

#[derive(Clone)]
/// Implementation of [`Status`](../trait.Status.html) that flushes data:
/// It **does not** preserve history.
pub struct FlushStatus<W: Write, L: Write> {
    time: Duration,
    queries_incomplete: usize,
    active_queries: HashMap<Query, QueryStatus>,
    num_finished: usize,
    data_sink: W,
    log_sink: L,
}

impl<W: Write, L: Write> Drop for FlushStatus<W, L> {
    fn drop(&mut self) {
        for (query, status) in self.active_queries.iter() {
            write_query(&mut self.data_sink, *query, *status);
        }
    }
}

impl<W: Write, L: Write> FlushStatus<W, L> {
    /// Constructs a new flushing status with the given sink.
    pub fn new(data_sink: W, log_sink: L) -> Self {
        Self {
            time: Duration::new(0, 0),
            queries_incomplete: 0,
            active_queries: HashMap::new(),
            num_finished: 0,
            data_sink,
            log_sink,
        }
    }
}

impl Default for FlushStatus<std::io::Stdout, std::io::Stderr> {
    fn default() -> Self {
        Self {
            time: Duration::new(0, 0),
            queries_incomplete: 0,
            active_queries: HashMap::new(),
            num_finished: 0,
            data_sink: std::io::stdout(),
            log_sink: std::io::stderr(),
        }
    }
}

impl<W: Write, L: Write> Status for FlushStatus<W, L> {
    fn time(&self) -> Duration {
        self.time
    }

    fn queries_entered(&self) -> usize {
        self.queries_active() + self.queries_finished()
    }

    fn queries_finished(&self) -> usize {
        self.num_finished
    }

    fn queries_active(&self) -> usize {
        self.active_queries.len()
    }

    fn queries_incomplete(&self) -> usize {
        self.queries_incomplete
    }

    fn active<'a>(&'a self) -> Box<dyn Iterator<Item = (Query, QueryStatus)> + 'a> {
        Box::new(self.active_queries.iter().map(|(q, s)| (*q, *s)))
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
                self.num_finished += 1;
                write_query(&mut self.data_sink, query, s);
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
                self.num_finished += 1;
                write_query(&mut self.data_sink, query, s);
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
        for event in events {
            writeln!(self.data_sink, "{}", event).expect("Unable to write logs");
        }
    }

    fn update_time(&mut self, time: Duration) {
        self.time = time;
    }
}

fn write_query<W: Write>(mut sink: &mut W, query: Query, status: QueryStatus) {
    to_writer(
        &mut sink,
        &json! ({
            "query_id": query.id,
            "request_id": query.request,
            "status": status,
        }),
    )
    .expect("Unable to write query status");
    writeln!(sink).expect("Unable to write query status");
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::simulation::{Query, QueryStatus, Status};
    use crate::simulation::{QueryId, RequestId};
    use std::time::Duration;

    #[test]
    fn test_default() {
        let status = FlushStatus::default();
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
        let mut status = FlushStatus::default();
        status.enter_query(q1, secs(1));
        assert_eq!(status.queries_entered(), 1);
        status.enter_query(q2, secs(2));
        assert_eq!(status.queries_entered(), 2);
        assert_eq!(
            status.active_queries.iter().collect::<Vec<_>>(),
            vec![
                (&q1, &QueryStatus::new(secs(1))),
                (&q2, &QueryStatus::new(secs(2)))
            ]
        );
        status.pick_up_query(q1, secs(3));
        assert_eq!(status.queries_entered(), 2);
        assert_eq!(
            status.active_queries,
            vec![
                (q1, QueryStatus::new(secs(1)).pick_up(secs(3))),
                (q2, QueryStatus::new(secs(2)))
            ]
            .into_iter()
            .collect::<HashMap<_, _>>()
        );
        status.dispatch_query(q1, secs(3), 2);
        assert_eq!(status.queries_entered(), 2);
        assert_eq!(
            status.active_queries,
            vec![
                (
                    q1,
                    QueryStatus::new(secs(1))
                        .pick_up(secs(3))
                        .dispatch(secs(3), 2)
                ),
                (q2, QueryStatus::new(secs(2)))
            ]
            .into_iter()
            .collect::<HashMap<_, _>>()
        );
        status.finish_shard(q1, secs(4));
        status.drop_shard(q1, secs(5));
        assert_eq!(status.queries_entered(), 2);
        assert_eq!(status.queries_active(), 1);
        assert_eq!(status.queries_finished(), 1);
        assert_eq!(status.queries_incomplete(), 1);
        assert_eq!(
            status.active().collect::<HashMap<_, _>>(),
            vec![(q2, QueryStatus::new(secs(2)))]
                .into_iter()
                .collect::<HashMap<_, _>>()
        );
        assert_eq!(status.num_finished, 1);
        status.pick_up_query(q2, secs(5));
        status.dispatch_query(q2, secs(6), 2);
        status.finish_shard(q2, secs(7));
        status.finish_shard(q2, secs(8));
        assert_eq!(status.queries_entered(), 2);
        assert_eq!(status.queries_active(), 0);
        assert_eq!(status.queries_finished(), 2);
        assert_eq!(status.queries_incomplete(), 1);
        assert!(status.active_queries.is_empty());
        assert_eq!(status.num_finished, 2);
    }
}
