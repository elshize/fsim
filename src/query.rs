use crate::{QueryId, RequestId};
use std::cmp::Ordering;
use std::time::Duration;

/// Identifies a query passed along within a simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Query {
    /// Identifier of a certain query given at the input.
    /// This is used to retrieve information such as retrieval and selection times.
    pub id: QueryId,
    /// Identifier of a request, unique throughout the entire simulation.
    /// This is used to access information about a specific instance of a query request
    /// such as how many shards have completed processing.
    pub request: RequestId,
}

impl Query {
    /// Constructs a new query with the given query ID and request ID.
    pub fn new(id: QueryId, request: RequestId) -> Self {
        Self { id, request }
    }
}

impl PartialOrd for Query {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.request.partial_cmp(&other.request)
    }
}

impl Ord for Query {
    fn cmp(&self, other: &Self) -> Ordering {
        self.request.cmp(&other.request)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PickedUpQueryStatus {
    entry: Duration,
    broker: Duration,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DispatchedQueryStatus {
    entry: Duration,
    broker: Duration,
    dispatch: Duration,
    num_shards: usize,
    num_finished: usize,
    num_dropped: usize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FinishedQueryStatus {
    entry: Duration,
    broker: Duration,
    dispatch: Duration,
    finished: Duration,
    num_shards: usize,
    num_dropped: usize,
}

/// The current status of the query.
#[derive(Debug, Clone, Eq, PartialEq)]
#[allow(missing_docs)]
pub enum QueryStatus {
    New(Duration),
    PickedUp(PickedUpQueryStatus),
    Dispatched(DispatchedQueryStatus),
    Finished(FinishedQueryStatus),
}

impl QueryStatus {
    /// Constructs a new status of a query that arrived at `time` to the system.
    pub fn new(time: Duration) -> Self {
        Self::New(time)
    }

    /// Returns the time at which the query entered the incoming queue.
    pub fn entry_time(&self) -> Duration {
        use QueryStatus::*;
        match self {
            New(duration) => *duration,
            PickedUp(PickedUpQueryStatus { entry, .. })
            | Dispatched(DispatchedQueryStatus { entry, .. })
            | Finished(FinishedQueryStatus { entry, .. }) => *entry,
        }
    }

    /// Returns the time at which the query was picked up by a broker.
    pub fn pick_up_time(&self) -> Option<Duration> {
        use QueryStatus::*;
        match self {
            New(_) => None,
            PickedUp(PickedUpQueryStatus { broker, .. })
            | Dispatched(DispatchedQueryStatus { broker, .. })
            | Finished(FinishedQueryStatus { broker, .. }) => Some(*broker),
        }
    }

    /// Returns the time at which the query was dispatched to shard nodes.
    pub fn dispatch_time(&self) -> Option<Duration> {
        use QueryStatus::*;
        match self {
            New(_) | PickedUp(_) => None,
            Dispatched(DispatchedQueryStatus { dispatch, .. })
            | Finished(FinishedQueryStatus { dispatch, .. }) => Some(*dispatch),
        }
    }

    /// Returns the time at which the query was finished.
    pub fn finish_time(&self) -> Option<Duration> {
        use QueryStatus::*;
        match self {
            New(_) | PickedUp(_) | Dispatched(_) => None,
            Finished(FinishedQueryStatus { finished, .. }) => Some(*finished),
        }
    }

    /// Returns `true` if the query is in state `Finished`.
    pub fn is_finished(&self) -> bool {
        matches!(self, QueryStatus::Finished(_))
    }

    /// Records the fact that the query was picked up by a broker at `time`.
    pub fn pick_up(&mut self, time: Duration) -> &mut Self {
        match self {
            Self::New(entry) => {
                assert!(
                    *entry <= time,
                    "Entry time ({:?}) must be at or before pick up time ({:?})",
                    *entry,
                    time
                );
                *self = Self::PickedUp(PickedUpQueryStatus {
                    entry: *entry,
                    broker: time,
                });
                self
            }
            _ => panic!("Query in invalid state"),
        }
    }

    /// Records the fact that the query was dispatched by a broker at `time` to `num_shards`
    /// shards.
    pub fn dispatch(&mut self, time: Duration, num_shards: usize) -> &mut Self {
        match self {
            Self::PickedUp(PickedUpQueryStatus { entry, broker }) => {
                assert!(
                    *broker <= time,
                    "Pick-up time ({:?}) must be at or before dispatch time ({:?})",
                    *broker,
                    time
                );
                *self = Self::Dispatched(DispatchedQueryStatus {
                    entry: *entry,
                    broker: *broker,
                    dispatch: time,
                    num_shards,
                    num_finished: 0,
                    num_dropped: 0,
                });
                self
            }
            _ => panic!("Query in invalid state"),
        }
    }

    /// Records the fact that a shard have finished processing and sent results back to the
    /// broker by the time `time`.
    pub fn finish_shard(&mut self, time: Duration) -> &mut Self {
        match self {
            Self::Dispatched(DispatchedQueryStatus {
                entry,
                broker,
                dispatch,
                num_shards,
                num_finished,
                num_dropped,
            }) => {
                assert!(
                    *dispatch <= time,
                    "Dispatch time ({:?}) must be at or before finish time ({:?})",
                    *dispatch,
                    time
                );
                if *num_finished + *num_dropped + 1 == *num_shards {
                    *self = Self::Finished(FinishedQueryStatus {
                        entry: *entry,
                        broker: *broker,
                        dispatch: *dispatch,
                        finished: time,
                        num_shards: *num_shards,
                        num_dropped: *num_dropped,
                    });
                } else {
                    *self = Self::Dispatched(DispatchedQueryStatus {
                        entry: *entry,
                        broker: *broker,
                        dispatch: *dispatch,
                        num_shards: *num_shards,
                        num_dropped: *num_dropped,
                        num_finished: *num_finished + 1,
                    });
                };
                self
            }
            _ => panic!("Query in invalid state"),
        }
    }

    /// Records the fact that a shard request had to be dropped.
    pub fn drop_shard(&mut self, time: Duration) -> &mut Self {
        match self {
            Self::Dispatched(DispatchedQueryStatus {
                entry,
                broker,
                dispatch,
                num_shards,
                num_finished,
                num_dropped,
            }) => {
                assert!(
                    *dispatch <= time,
                    "Dispatch time ({:?}) must be at or before drop time ({:?})",
                    *dispatch,
                    time
                );
                if *num_finished + *num_dropped + 1 == *num_shards {
                    *self = Self::Finished(FinishedQueryStatus {
                        entry: *entry,
                        broker: *broker,
                        dispatch: *dispatch,
                        finished: time,
                        num_shards: *num_shards,
                        num_dropped: *num_dropped + 1,
                    });
                } else {
                    *self = Self::Dispatched(DispatchedQueryStatus {
                        entry: *entry,
                        broker: *broker,
                        dispatch: *dispatch,
                        num_shards: *num_shards,
                        num_dropped: *num_dropped + 1,
                        num_finished: *num_finished,
                    });
                };
                self
            }
            _ => panic!("Query in invalid state"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_new() {
        assert_eq!(
            Query::new(QueryId(1), RequestId(1)),
            Query {
                id: QueryId(1),
                request: RequestId(1)
            }
        );
    }

    #[test]
    fn test_query_status_correct_pipeline() {
        let mut query = QueryStatus::new(Duration::new(0, 1));
        assert_eq!(query, QueryStatus::New(Duration::new(0, 1)));
        assert_eq!(query.entry_time(), Duration::new(0, 1));
        assert_eq!(query.pick_up_time(), None);
        assert_eq!(query.dispatch_time(), None);
        assert_eq!(query.finish_time(), None);
        query.pick_up(Duration::new(0, 2));
        assert_eq!(
            query,
            QueryStatus::PickedUp(PickedUpQueryStatus {
                entry: Duration::new(0, 1),
                broker: Duration::new(0, 2)
            })
        );
        assert_eq!(query.entry_time(), Duration::new(0, 1));
        assert_eq!(query.pick_up_time(), Some(Duration::new(0, 2)));
        assert_eq!(query.dispatch_time(), None);
        assert_eq!(query.finish_time(), None);
        query.dispatch(Duration::new(0, 3), 2);
        assert_eq!(
            query,
            QueryStatus::Dispatched(DispatchedQueryStatus {
                entry: Duration::new(0, 1),
                broker: Duration::new(0, 2),
                dispatch: Duration::new(0, 3),
                num_shards: 2,
                num_finished: 0,
                num_dropped: 0,
            })
        );
        assert_eq!(query.entry_time(), Duration::new(0, 1));
        assert_eq!(query.pick_up_time(), Some(Duration::new(0, 2)));
        assert_eq!(query.dispatch_time(), Some(Duration::new(0, 3)));
        assert_eq!(query.finish_time(), None);
        query.finish_shard(Duration::new(0, 4));
        assert_eq!(
            query,
            QueryStatus::Dispatched(DispatchedQueryStatus {
                entry: Duration::new(0, 1),
                broker: Duration::new(0, 2),
                dispatch: Duration::new(0, 3),
                num_shards: 2,
                num_finished: 1,
                num_dropped: 0,
            })
        );
        assert_eq!(query.entry_time(), Duration::new(0, 1));
        assert_eq!(query.pick_up_time(), Some(Duration::new(0, 2)));
        assert_eq!(query.dispatch_time(), Some(Duration::new(0, 3)));
        assert_eq!(query.finish_time(), None);
        query.finish_shard(Duration::new(0, 5));
        assert_eq!(
            query,
            QueryStatus::Finished(FinishedQueryStatus {
                entry: Duration::new(0, 1),
                broker: Duration::new(0, 2),
                dispatch: Duration::new(0, 3),
                finished: Duration::new(0, 5),
                num_shards: 2,
                num_dropped: 0,
            })
        );
        assert_eq!(query.entry_time(), Duration::new(0, 1));
        assert_eq!(query.pick_up_time(), Some(Duration::new(0, 2)));
        assert_eq!(query.dispatch_time(), Some(Duration::new(0, 3)));
        assert_eq!(query.finish_time(), Some(Duration::new(0, 5)));
    }

    #[test]
    #[should_panic(expected = "Query in invalid state")]
    fn test_query_status_pick_up_again() {
        QueryStatus::PickedUp(PickedUpQueryStatus {
            entry: Duration::new(0, 1),
            broker: Duration::new(0, 2),
        })
        .pick_up(Duration::new(0, 2));
    }

    #[test]
    #[should_panic(expected = "Query in invalid state")]
    fn test_query_status_pick_up_dispatched() {
        QueryStatus::Dispatched(DispatchedQueryStatus {
            entry: Duration::new(0, 1),
            broker: Duration::new(0, 2),
            dispatch: Duration::new(0, 3),
            num_shards: 3,
            num_finished: 0,
            num_dropped: 0,
        })
        .pick_up(Duration::new(0, 2));
    }

    #[test]
    #[should_panic(expected = "Query in invalid state")]
    fn test_query_status_pick_up_finished() {
        QueryStatus::Finished(FinishedQueryStatus {
            entry: Duration::new(0, 1),
            broker: Duration::new(0, 2),
            dispatch: Duration::new(0, 3),
            finished: Duration::new(0, 5),
            num_shards: 3,
            num_dropped: 0,
        })
        .pick_up(Duration::new(0, 2));
    }

    #[test]
    #[should_panic(expected = "Query in invalid state")]
    fn test_query_status_dispatch_new() {
        QueryStatus::New(Duration::new(0, 1)).dispatch(Duration::new(0, 2), 3);
    }

    #[test]
    #[should_panic(expected = "Query in invalid state")]
    fn test_query_status_dispatch_again() {
        QueryStatus::Dispatched(DispatchedQueryStatus {
            entry: Duration::new(0, 1),
            broker: Duration::new(0, 2),
            dispatch: Duration::new(0, 3),
            num_shards: 3,
            num_finished: 0,
            num_dropped: 0,
        })
        .dispatch(Duration::new(0, 2), 3);
    }

    #[test]
    #[should_panic(expected = "Query in invalid state")]
    fn test_query_status_dispatch_finished() {
        QueryStatus::Finished(FinishedQueryStatus {
            entry: Duration::new(0, 1),
            broker: Duration::new(0, 2),
            dispatch: Duration::new(0, 3),
            finished: Duration::new(0, 5),
            num_shards: 3,
            num_dropped: 0,
        })
        .dispatch(Duration::new(0, 2), 3);
    }

    #[test]
    #[should_panic(expected = "Query in invalid state")]
    fn test_query_status_finish_new() {
        QueryStatus::New(Duration::new(0, 1)).finish_shard(Duration::new(0, 2));
    }

    #[test]
    #[should_panic(expected = "Query in invalid state")]
    fn test_query_status_finish_picked_up() {
        QueryStatus::PickedUp(PickedUpQueryStatus {
            entry: Duration::new(0, 1),
            broker: Duration::new(0, 2),
        })
        .finish_shard(Duration::new(0, 2));
    }

    #[test]
    #[should_panic(expected = "Query in invalid state")]
    fn test_query_status_finish_one_too_many_times() {
        QueryStatus::Finished(FinishedQueryStatus {
            entry: Duration::new(0, 1),
            broker: Duration::new(0, 2),
            dispatch: Duration::new(0, 3),
            finished: Duration::new(0, 5),
            num_shards: 3,
            num_dropped: 0,
        })
        .finish_shard(Duration::new(0, 6));
    }

    #[test]
    #[should_panic(expected = "Entry time (10ns) must be at or before pick up time (2ns)")]
    fn test_query_status_pick_up_in_past() {
        QueryStatus::New(Duration::new(0, 10)).pick_up(Duration::new(0, 2));
    }

    #[test]
    #[should_panic(expected = "Pick-up time (2ns) must be at or before dispatch time (1ns)")]
    fn test_query_status_dispatch_in_past() {
        QueryStatus::PickedUp(PickedUpQueryStatus {
            entry: Duration::new(0, 1),
            broker: Duration::new(0, 2),
        })
        .dispatch(Duration::new(0, 1), 2);
    }

    #[test]
    #[should_panic(expected = "Dispatch time (3ns) must be at or before finish time (1ns)")]
    fn test_query_status_finish_shard_in_past() {
        QueryStatus::Dispatched(DispatchedQueryStatus {
            entry: Duration::new(0, 1),
            broker: Duration::new(0, 2),
            dispatch: Duration::new(0, 3),
            num_shards: 3,
            num_finished: 0,
            num_dropped: 0,
        })
        .finish_shard(Duration::new(0, 1));
    }
}
