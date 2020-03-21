//! Query routing. More docs to come...

#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::default_trait_access)]

use id_derive::Id;
use serde::{Deserialize, Serialize};
use std::cmp::{Ord, Ordering, PartialOrd};
use std::time::Duration;

mod query;
pub use query::{Query, QueryStatus};

mod process;
pub use process::Runnable;

mod query_generator;
pub use query_generator::{QueryGenerator, QueryGeneratorStage};

mod queue;
pub use queue::{PopResult, PushResult, Queue};

mod broker;
pub use broker::{Broker, BrokerStage};

mod node;
pub use node::{Node, NodeStage};

pub mod replica_selector;
pub mod routing_matrix;
mod shard_selector;

pub mod config;
pub use config::Config;

mod im_status;
pub use im_status::Status;

pub mod logger;

mod simulation;
pub use simulation::{Effect, QueryRoutingSimulation};

/// Shard ID.
#[derive(Id, Debug, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize, Copy, Clone, Hash)]
pub struct ShardId(usize);

/// Node ID.
#[derive(Id, Debug, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize, Copy, Clone, Hash)]
pub struct NodeId(usize);

/// Query ID.
#[derive(Id, Debug, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize, Copy, Clone, Hash)]
pub struct QueryId(usize);

/// Query request ID.
#[derive(Id, Debug, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize, Copy, Clone, Hash)]
pub struct RequestId(usize);

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize, Copy, Clone)]
/// Replica is identified by a node and a shard.
pub struct ReplicaId {
    /// Node ID.
    pub node: NodeId,
    /// Shard ID.
    pub shard: ShardId,
}

/// Represents a process being part of the simulation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Process {
    /// See [`QueryGenerator`](struct.QueryGenerator.html).
    QueryGenerator(QueryGeneratorStage),
    /// See [`Broker`](struct.Broker.html).
    Broker(BrokerStage),
    /// See [`Node`](struct.Node.html).
    Node {
        /// Node ID.
        id: NodeId,
        /// Stage to enter.
        stage: NodeStage,
    },
}

/// An event is simply a process to run and a time to run it on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    time: Duration,
    process: Process,
}

impl Event {
    fn new(time: Duration, process: Process) -> Self {
        Event { time, process }
    }
}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.time.partial_cmp(&other.time)
    }
}

impl Ord for Event {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time.cmp(&other.time)
    }
}

/// Request to route a query to a certain node to process a certain shard.
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct NodeRequest {
    id: NodeId,
    shard: ShardId,
}

#[cfg(test)]
mod test {
    use rand::distributions::Distribution;

    pub(crate) struct WrappingEchoDistribution<T> {
        size: T,
    }

    impl<T> WrappingEchoDistribution<T> {
        pub(crate) fn new(size: T) -> Self {
            Self { size }
        }
    }

    impl<T> Distribution<T> for WrappingEchoDistribution<T>
    where
        T: std::convert::TryFrom<u32> + std::ops::Rem<T, Output = T> + Copy,
        <T as std::convert::TryFrom<u32>>::Error: std::fmt::Debug,
    {
        fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> T {
            T::try_from(rng.next_u32()).unwrap() % self.size
        }
    }

    pub(crate) struct ToFloatDistribution<D>(pub(crate) D);

    impl<D> Distribution<f32> for ToFloatDistribution<D>
    where
        D: Distribution<u64>,
    {
        fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> f32 {
            self.0.sample(rng) as f32
        }
    }
}
