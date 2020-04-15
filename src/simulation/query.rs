use super::{QueryId, RequestId};
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::time::Duration;

/// Identifies a query passed along within a simulation.
///
/// A query is uniquely identified by [`request`](#structfield.request), which should be unique
/// throughout any simulation. In order to ensure correctness, partial order is not defined for any
/// two queries with a different query ID but the same request ID:
///
/// ```
/// # use fsim::simulation::{Query, QueryId, RequestId};
/// let lhs = Query::new(QueryId::from(0), RequestId::from(0));
/// let rhs = Query::new(QueryId::from(1), RequestId::from(0));
/// assert!(lhs.partial_cmp(&rhs).is_none());
/// ```
///
/// # Panics
///
/// To enable indexing by query in associative collections, `Eq` and `Ord` are defined but will
/// panic if the ordering or equality is not defined.
///
/// ```should_panic
/// # use fsim::simulation::{Query, QueryId, RequestId};
/// let lhs = Query::new(QueryId::from(0), RequestId::from(0));
/// let rhs = Query::new(QueryId::from(1), RequestId::from(0));
/// lhs.cmp(&rhs); // panics
/// ```
#[derive(Debug, Clone, Copy, Eq)]
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
    #[must_use]
    pub fn new(id: QueryId, request: RequestId) -> Self {
        Self { id, request }
    }
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}/{}", self.request, self.id)
    }
}

impl PartialEq for Query {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(&other) == Ordering::Equal
    }
}

impl PartialOrd for Query {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.request.cmp(&other.request) {
            Ordering::Equal => {
                if self.id == other.id {
                    Some(Ordering::Equal)
                } else {
                    None
                }
            }
            ord => Some(ord),
        }
    }
}

impl Ord for Query {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.partial_cmp(&other) {
            Some(cmp) => cmp,
            None => panic!("Cannot compare {:?} with {:?}", self, other),
        }
    }
}

impl Hash for Query {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.request.hash(state);
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct PickedUpQueryStatus {
    entry: Duration,
    broker: Duration,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DispatchedQueryStatus {
    entry: Duration,
    broker: Duration,
    dispatch: Duration,
    num_shards: usize,
    num_finished: usize,
    num_dropped: usize,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct FinishedQueryStatus {
    entry: Duration,
    broker: Duration,
    dispatch: Duration,
    finished: Duration,
    num_shards: usize,
    num_dropped: usize,
}

/// The current status of the query.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[allow(missing_docs)]
pub enum QueryStatus {
    New(Duration),
    PickedUp(PickedUpQueryStatus),
    Dispatched(DispatchedQueryStatus),
    Finished(FinishedQueryStatus),
}

impl QueryStatus {
    /// Constructs a new status of a query that arrived at `time` to the system.
    #[must_use]
    pub fn new(time: Duration) -> Self {
        Self::New(time)
    }

    /// Returns the time at which the query entered the incoming queue.
    #[must_use]
    pub fn entry_time(&self) -> Duration {
        use QueryStatus::{Dispatched, Finished, New, PickedUp};
        match self {
            New(duration) => *duration,
            PickedUp(PickedUpQueryStatus { entry, .. })
            | Dispatched(DispatchedQueryStatus { entry, .. })
            | Finished(FinishedQueryStatus { entry, .. }) => *entry,
        }
    }

    /// Returns the time at which the query was picked up by a broker.
    #[must_use]
    pub fn pick_up_time(&self) -> Option<Duration> {
        use QueryStatus::{Dispatched, Finished, New, PickedUp};
        match self {
            New(_) => None,
            PickedUp(PickedUpQueryStatus { broker, .. })
            | Dispatched(DispatchedQueryStatus { broker, .. })
            | Finished(FinishedQueryStatus { broker, .. }) => Some(*broker),
        }
    }

    /// Returns the time at which the query was dispatched to shard nodes.
    #[must_use]
    pub fn dispatch_time(&self) -> Option<Duration> {
        use QueryStatus::{Dispatched, Finished, New, PickedUp};
        match self {
            New(_) | PickedUp(_) => None,
            Dispatched(DispatchedQueryStatus { dispatch, .. })
            | Finished(FinishedQueryStatus { dispatch, .. }) => Some(*dispatch),
        }
    }

    /// Returns the time at which the query was finished.
    #[must_use]
    pub fn finish_time(&self) -> Option<Duration> {
        use QueryStatus::{Dispatched, Finished, New, PickedUp};
        match self {
            New(_) | PickedUp(_) | Dispatched(_) => None,
            Finished(FinishedQueryStatus { finished, .. }) => Some(*finished),
        }
    }

    /// Number of dispatch requests.
    #[must_use]
    pub fn num_dispatched(&self) -> usize {
        use QueryStatus::{Dispatched, Finished, New, PickedUp};
        match self {
            New(_) | PickedUp(_) => 0,
            Dispatched(DispatchedQueryStatus { num_shards, .. })
            | Finished(FinishedQueryStatus { num_shards, .. }) => *num_shards,
        }
    }

    /// Number of dispatch requests that were dropped.
    #[must_use]
    pub fn num_dropped(&self) -> usize {
        use QueryStatus::{Dispatched, Finished, New, PickedUp};
        match self {
            New(_) | PickedUp(_) => 0,
            Dispatched(DispatchedQueryStatus { num_dropped, .. })
            | Finished(FinishedQueryStatus { num_dropped, .. }) => *num_dropped,
        }
    }

    /// Returns `true` if the query is in state `Finished`.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        matches!(self, QueryStatus::Finished(_))
    }

    /// Records the fact that the query was picked up by a broker at `time`.
    #[must_use]
    pub fn pick_up(&self, time: Duration) -> Self {
        match self {
            Self::New(entry) => {
                assert!(
                    *entry <= time,
                    "Entry time ({:?}) must be at or before pick up time ({:?})",
                    entry,
                    time
                );
                Self::PickedUp(PickedUpQueryStatus {
                    entry: *entry,
                    broker: time,
                })
            }
            _ => panic!("Query in invalid state"),
        }
    }

    /// Records the fact that the query was dispatched by a broker at `time` to `num_shards`
    /// shards.
    #[must_use]
    pub fn dispatch(&self, time: Duration, num_shards: usize) -> Self {
        match self {
            Self::PickedUp(PickedUpQueryStatus { entry, broker }) => {
                assert!(
                    *broker <= time,
                    "Pick-up time ({:?}) must be at or before dispatch time ({:?})",
                    broker,
                    time
                );
                Self::Dispatched(DispatchedQueryStatus {
                    entry: *entry,
                    broker: *broker,
                    dispatch: time,
                    num_shards,
                    num_finished: 0,
                    num_dropped: 0,
                })
            }
            Self::Dispatched(status) => {
                assert!(status.dispatch <= time, "Dispatches must be done in order",);
                Self::Dispatched(DispatchedQueryStatus {
                    dispatch: time,
                    num_shards: status.num_shards + num_shards,
                    ..*status
                })
            }
            _ => panic!("Query in invalid state"),
        }
    }

    /// Records the fact that a shard have finished processing and sent results back to the
    /// broker by the time `time`.
    #[must_use]
    pub fn finish_shard(&self, time: Duration) -> Self {
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
                    Self::Finished(FinishedQueryStatus {
                        entry: *entry,
                        broker: *broker,
                        dispatch: *dispatch,
                        finished: time,
                        num_shards: *num_shards,
                        num_dropped: *num_dropped,
                    })
                } else {
                    Self::Dispatched(DispatchedQueryStatus {
                        entry: *entry,
                        broker: *broker,
                        dispatch: *dispatch,
                        num_shards: *num_shards,
                        num_dropped: *num_dropped,
                        num_finished: *num_finished + 1,
                    })
                }
            }
            _ => panic!("Query in invalid state"),
        }
    }

    /// Records the fact that a shard request had to be dropped.
    #[must_use]
    pub fn drop_shard(&self, time: Duration) -> Self {
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
                    Self::Finished(FinishedQueryStatus {
                        entry: *entry,
                        broker: *broker,
                        dispatch: *dispatch,
                        finished: time,
                        num_shards: *num_shards,
                        num_dropped: *num_dropped + 1,
                    })
                } else {
                    Self::Dispatched(DispatchedQueryStatus {
                        entry: *entry,
                        broker: *broker,
                        dispatch: *dispatch,
                        num_shards: *num_shards,
                        num_dropped: *num_dropped + 1,
                        num_finished: *num_finished,
                    })
                }
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
    fn test_ordering() {
        assert_eq!(
            Query::new(QueryId(1), RequestId(1)).partial_cmp(&Query::new(QueryId(1), RequestId(1))),
            Some(Ordering::Equal)
        );
        assert_eq!(
            Query::new(QueryId(1), RequestId(1)).partial_cmp(&Query::new(QueryId(1), RequestId(2))),
            Some(Ordering::Less)
        );
        assert_eq!(
            Query::new(QueryId(1), RequestId(1)).partial_cmp(&Query::new(QueryId(1), RequestId(0))),
            Some(Ordering::Greater)
        );
        assert!(Query::new(QueryId(0), RequestId(0))
            .partial_cmp(&Query::new(QueryId(1), RequestId(0)))
            .is_none());
        assert_eq!(
            Query::new(QueryId(1), RequestId(1)).cmp(&Query::new(QueryId(1), RequestId(1))),
            Ordering::Equal
        );
        assert_eq!(
            Query::new(QueryId(1), RequestId(1)).cmp(&Query::new(QueryId(1), RequestId(2))),
            Ordering::Less
        );
        assert_eq!(
            Query::new(QueryId(1), RequestId(1)).cmp(&Query::new(QueryId(1), RequestId(0))),
            Ordering::Greater
        );
    }

    #[test]
    #[should_panic]
    fn test_compare_invalid() {
        let _ = Query::new(QueryId(0), RequestId(0)) == Query::new(QueryId(1), RequestId(0));
    }

    #[test]
    fn test_query_status_correct_pipeline() {
        let mut query = QueryStatus::new(Duration::new(0, 1));
        assert_eq!(query, QueryStatus::New(Duration::new(0, 1)));
        assert_eq!(query.entry_time(), Duration::new(0, 1));
        assert_eq!(query.pick_up_time(), None);
        assert_eq!(query.dispatch_time(), None);
        assert_eq!(query.finish_time(), None);
        query = query.pick_up(Duration::new(0, 2));
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
        assert_eq!(query.num_dropped(), 0);
        query = query.dispatch(Duration::new(0, 3), 2);
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
        assert_eq!(query.num_dropped(), 0);
        query = query.drop_shard(Duration::new(0, 4));
        assert_eq!(
            query,
            QueryStatus::Dispatched(DispatchedQueryStatus {
                entry: Duration::new(0, 1),
                broker: Duration::new(0, 2),
                dispatch: Duration::new(0, 3),
                num_shards: 2,
                num_finished: 0,
                num_dropped: 1,
            })
        );
        assert_eq!(query.entry_time(), Duration::new(0, 1));
        assert_eq!(query.pick_up_time(), Some(Duration::new(0, 2)));
        assert_eq!(query.dispatch_time(), Some(Duration::new(0, 3)));
        assert_eq!(query.finish_time(), None);
        assert_eq!(query.num_dropped(), 1);
        query = query.finish_shard(Duration::new(0, 5));
        assert_eq!(
            query,
            QueryStatus::Finished(FinishedQueryStatus {
                entry: Duration::new(0, 1),
                broker: Duration::new(0, 2),
                dispatch: Duration::new(0, 3),
                finished: Duration::new(0, 5),
                num_shards: 2,
                num_dropped: 1,
            })
        );
        assert_eq!(query.entry_time(), Duration::new(0, 1));
        assert_eq!(query.pick_up_time(), Some(Duration::new(0, 2)));
        assert_eq!(query.dispatch_time(), Some(Duration::new(0, 3)));
        assert_eq!(query.finish_time(), Some(Duration::new(0, 5)));
        assert_eq!(query.num_dropped(), 1);
    }

    #[test]
    fn test_query_status_dispatch_again_correct() {
        let status = QueryStatus::Dispatched(DispatchedQueryStatus {
            entry: Duration::new(0, 1),
            broker: Duration::new(0, 2),
            dispatch: Duration::new(0, 3),
            num_shards: 3,
            num_finished: 0,
            num_dropped: 0,
        })
        .dispatch(Duration::new(0, 4), 3);
        assert_eq!(status.entry_time(), Duration::new(0, 1));
        assert_eq!(status.pick_up_time(), Some(Duration::new(0, 2)));
        assert_eq!(status.dispatch_time(), Some(Duration::new(0, 4)));
        assert_eq!(status.num_dispatched(), 6);
    }

    #[test]
    #[should_panic(expected = "Query in invalid state")]
    fn test_query_status_pick_up_again() {
        let _ = QueryStatus::PickedUp(PickedUpQueryStatus {
            entry: Duration::new(0, 1),
            broker: Duration::new(0, 2),
        })
        .pick_up(Duration::new(0, 2));
    }

    #[test]
    #[should_panic(expected = "Query in invalid state")]
    fn test_query_status_pick_up_dispatched() {
        let _ = QueryStatus::Dispatched(DispatchedQueryStatus {
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
        let _ = QueryStatus::Finished(FinishedQueryStatus {
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
        let _ = QueryStatus::New(Duration::new(0, 1)).dispatch(Duration::new(0, 2), 3);
    }

    #[test]
    #[should_panic(expected = "Dispatches must be done in order")]
    fn test_query_status_dispatch_again_out_of_order() {
        let _ = QueryStatus::Dispatched(DispatchedQueryStatus {
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
        let _ = QueryStatus::Finished(FinishedQueryStatus {
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
        let _ = QueryStatus::New(Duration::new(0, 1)).finish_shard(Duration::new(0, 2));
    }

    #[test]
    #[should_panic(expected = "Query in invalid state")]
    fn test_query_status_finish_picked_up() {
        let _ = QueryStatus::PickedUp(PickedUpQueryStatus {
            entry: Duration::new(0, 1),
            broker: Duration::new(0, 2),
        })
        .finish_shard(Duration::new(0, 2));
    }

    #[test]
    #[should_panic(expected = "Query in invalid state")]
    fn test_query_status_finish_one_too_many_times() {
        let _ = QueryStatus::Finished(FinishedQueryStatus {
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
    #[should_panic(expected = "Query in invalid state")]
    fn test_query_status_drop_new() {
        let _ = QueryStatus::New(Duration::new(0, 1)).drop_shard(Duration::new(0, 2));
    }

    #[test]
    #[should_panic(expected = "Query in invalid state")]
    fn test_query_status_drop_picked_up() {
        let _ = QueryStatus::PickedUp(PickedUpQueryStatus {
            entry: Duration::new(0, 1),
            broker: Duration::new(0, 2),
        })
        .drop_shard(Duration::new(0, 2));
    }

    #[test]
    #[should_panic(expected = "Query in invalid state")]
    fn test_query_status_drop_one_too_many_times() {
        let _ = QueryStatus::Finished(FinishedQueryStatus {
            entry: Duration::new(0, 1),
            broker: Duration::new(0, 2),
            dispatch: Duration::new(0, 3),
            finished: Duration::new(0, 5),
            num_shards: 3,
            num_dropped: 0,
        })
        .drop_shard(Duration::new(0, 6));
    }

    #[test]
    #[should_panic(expected = "Entry time (10ns) must be at or before pick up time (2ns)")]
    fn test_query_status_pick_up_in_past() {
        let _ = QueryStatus::New(Duration::new(0, 10)).pick_up(Duration::new(0, 2));
    }

    #[test]
    #[should_panic(expected = "Pick-up time (2ns) must be at or before dispatch time (1ns)")]
    fn test_query_status_dispatch_in_past() {
        let _ = QueryStatus::PickedUp(PickedUpQueryStatus {
            entry: Duration::new(0, 1),
            broker: Duration::new(0, 2),
        })
        .dispatch(Duration::new(0, 1), 2);
    }

    #[test]
    #[should_panic(expected = "Dispatch time (3ns) must be at or before finish time (1ns)")]
    fn test_query_status_finish_shard_in_past() {
        let _ = QueryStatus::Dispatched(DispatchedQueryStatus {
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
