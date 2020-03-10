use crate::{QueryId, RequestId};
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

/// The current status of the query.
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub enum QueryStatus {
    New(Duration),
    PickedUp {
        entry: Duration,
        broker: Duration,
    },
    Dispatched {
        entry: Duration,
        broker: Duration,
        dispatch: Duration,
        num_shards: usize,
        num_finished: usize,
    },
    Finished {
        entry: Duration,
        broker: Duration,
        dispatch: Duration,
        finished: Duration,
        num_shards: usize,
    },
}

impl QueryStatus {
    /// Constructs a new status of a query that arrived at `time` to the system.
    pub fn new(time: Duration) -> Self {
        Self::New(time)
    }
    /// Records the fact that the query was picked up by a broker at `time`.
    pub fn pick_up(&mut self, time: Duration) {
        match self {
            Self::New(entry) => {
                *self = Self::PickedUp {
                    entry: *entry,
                    broker: time,
                };
            }
            _ => panic!("Query in invalid state"),
        }
    }
    /// Records the fact that the query was dispatched by a broker at `time` to `num_shards`
    /// shards.
    pub fn dispatch(&mut self, time: Duration, num_shards: usize) {
        match self {
            Self::PickedUp { entry, broker } => {
                *self = Self::Dispatched {
                    entry: *entry,
                    broker: *broker,
                    dispatch: time,
                    num_shards,
                    num_finished: 0,
                };
            }
            _ => panic!("Query in invalid state"),
        }
    }
    /// Records the fact that all shards have finished processing and sent results back to the
    /// broker by the time `time`.
    pub fn finish_shard(&mut self, time: Duration) {
        match self {
            Self::Dispatched {
                entry,
                broker,
                dispatch,
                num_shards,
                num_finished,
                ..
            } => {
                if *num_finished + 1 == *num_shards {
                    *self = Self::Finished {
                        entry: *entry,
                        broker: *broker,
                        dispatch: *dispatch,
                        finished: time,
                        num_shards: *num_shards,
                    };
                } else {
                    *self = Self::Dispatched {
                        entry: *entry,
                        broker: *broker,
                        dispatch: *dispatch,
                        num_shards: *num_shards,
                        num_finished: *num_finished + 1,
                    };
                };
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

    //#[test]
    //fn test_id() {
    //    assert_eq!(Query::new(QueryId(1), RequestId(9)).id(), QueryId(1));
    //}

    //#[test]
    //fn test_request() {
    //    assert_eq!(Query::new(QueryId(1), RequestId(9)).request(), RequestId(1));
    //}
}
