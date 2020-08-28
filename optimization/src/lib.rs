//! This module deals with assignment optimization problem.

#![warn(
    missing_docs,
    rust_2018_idioms,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::default_trait_access,
    clippy::cast_precision_loss
)]
#![deny(unsafe_code)]

mod array;
//mod distribution;
mod replica;
//mod routing;

//pub use distribution::AssignmentDistribution;
pub use replica::{AssignmentBuilder, Dimension, ReplicaAssignment};

/// Error type encompassing all optimization errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Two or more input parameters have inconsistent numbers of machines.
    #[error("Two or more input parameters have inconsistent numbers of machines.")]
    InconsistentMachineCount,
    /// Two or more input parameters have inconsistent numbers of shards.
    #[error("Two or more input parameters have inconsistent numbers of shards.")]
    InconsistentShardCount,
    /// Returned when attempted to perform a matrix operation with 0 shards.
    #[error("There must be at least one shard.")]
    ZeroShards,
    /// Returned when attempted to perform a matrix operation with 0 machines.
    #[error("There must be at least one machine.")]
    ZeroMachines,
    /// Greedy algorithm could not arrive at solution, problem could be infeasible.
    #[error("Greedy algorithm could not arrive at solution, problem could be infeasible.")]
    PossiblyInfeasible,
}

/// Result alias using [`Error`](enum.Error.html).
pub type Result<T> = std::result::Result<T, Error>;

array_wrapper!(ShardVolumes, f32, "Shard volumes vector.");
array_wrapper!(ShardLoads, f32, "Shard loads vector.");
array_wrapper!(ShardReplicas, u32, "Shard replica counts.");
array_wrapper!(ShardProbabilities, f32, "Shard probabilities.");
array_wrapper!(MachineCapacities, f32, "Machine capacities vector.");
array_wrapper!(MachineInhibitions, f32, "Machine inhibitions vector.");
