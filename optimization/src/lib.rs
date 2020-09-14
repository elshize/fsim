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
mod replica;
pub mod solver;
//mod routing;

//pub use distribution::AssignmentDistribution;
pub use replica::{AssignmentBuilder, AssignmentMethod, Dimension, ReplicaAssignment};

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
    /// Error occurred during execution of (I)LP solver.
    #[error("LP solver error: {0}")]
    Solver(#[from] solver::Error),
}

/// Result alias using [`Error`](enum.Error.html).
pub type Result<T> = std::result::Result<T, Error>;

array_wrapper!(ShardVolumes, f32, "Shard volumes vector.");
array_wrapper!(ShardLoads, f32, "Shard loads vector.");
array_wrapper!(ShardReplicas, u32, "Shard replica counts.");
array_wrapper!(ShardProbabilities, f32, "Shard probabilities.");
array_wrapper!(MachineCapacities, f32, "Machine capacities vector.");
array_wrapper!(MachineInhibitions, f32, "Machine inhibitions vector.");

/// Container grouping shard parameters.
pub struct ShardParams {
    /// Cost of running shards relative to other shards.
    pub loads: ShardLoads,
    /// Physical space used by shards.
    pub volumes: ShardVolumes,
    /// Number of replicas used for shards.
    pub replicas: ShardReplicas,
    /// Overall probability of shards being requested (all equal to 1.0 in exhaustive routing).
    pub probabilities: ShardProbabilities,
}

/// Container grouping machine parameters.
pub struct MachineParams {
    /// How much of shard volume fits in machines.
    pub capacities: MachineCapacities,
    /// Slowdown factor, relative to other shards. All equal 1 if machines considered identical.
    pub inhibitions: MachineInhibitions,
}
