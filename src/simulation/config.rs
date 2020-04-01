//! Anything related to loading up the simulation from configuration/input files.

use crate::simulation::ShardId;
use anyhow::{bail, ensure, Context};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Read};
use std::str::FromStr;

/// Type of time units used in the simulation.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum TimeUnit {
    /// Microseconds.
    Micro,
    /// Milliseconds.
    Milli,
    /// Nanoseconds.
    Nano,
    /// Seconds.
    Second,
}

impl Default for TimeUnit {
    fn default() -> Self {
        Self::Milli
    }
}

/// This stores shard to node assignments.
/// When setting up the simulation, any value greater than 0 will be taken as assignment.
/// When probabilities used in replica selection, weights will be used as probabilities.
#[derive(Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Assignment {
    weights: Vec<Vec<f32>>,
}

impl Assignment {
    /// Returns the number of shards in the assignment. Returns 0 if no weights available.
    pub fn num_shards(&self) -> usize {
        self.weights.get(0).map(Vec::len).unwrap_or(0)
    }
    /// Returns the number of nodes in the assignment. Returns 0 if no weights available.
    pub fn num_nodes(&self) -> usize {
        self.weights.len()
    }
    /// Assignment weights
    pub fn weights(&self) -> &[Vec<f32>] {
        &self.weights
    }
    /// Assignment weights
    pub fn weights_by_shard(&self) -> Vec<Vec<f32>> {
        let mut weights: Vec<Vec<f32>> = vec![vec![0_f32; self.num_nodes()]; self.num_shards()];
        for (sidx, shards) in weights.iter_mut().enumerate() {
            for (nidx, weight) in shards.iter_mut().enumerate() {
                *weight = self.weights[nidx][sidx];
            }
        }
        weights
    }
}

/// Describes normal distribution of time intervals between queries.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct QueryDistribution {
    /// Mean time interval.
    pub mean: f32,
    /// Standard deviation.
    pub std: f32,
}

/// Simulation configuration typically loaded from a file.
#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    /// Number of brokers.
    pub brokers: usize,
    /// Average interval between arriving queries.
    pub query_distribution: QueryDistribution,
    /// Shard to node assignment.
    pub assignment: Assignment,
    /// Random seed.
    pub seed: Option<u64>,
    /// Type of routing policy.
    #[serde(default)]
    pub routing: RoutingPolicy,
    /// Time unit. Any integer time will be interpreted as this unit.
    #[serde(default)]
    pub time_unit: TimeUnit,
    /// The number of threads that will be simulated per one shard node.
    pub cpus_per_node: usize,
}

impl Config {
    /// Load config from YAML file.
    ///
    /// # Example
    ///
    /// ```
    /// # use fsim::simulation::config::{TimeUnit, Config};
    /// # fn main() -> anyhow::Result<()> {
    /// let input = r#"
    /// time_unit: micro
    /// brokers: 2
    /// cpus_per_node: 2
    /// query_distribution:
    ///     mean: 10
    ///     std: 1
    /// assignment:
    ///     - [0, 1, 0]
    ///     - [1, 1, 0]
    ///     - [0, 1, 1]"#;
    /// let config = Config::from_yaml(std::io::Cursor::new(input))?;
    /// assert_eq!(config.time_unit, TimeUnit::Micro);
    /// assert_eq!(config.brokers, 2);
    /// assert_eq!(config.cpus_per_node, 2);
    /// assert_eq!(config.query_distribution.mean, 10.0);
    /// assert_eq!(config.query_distribution.std, 1.0);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Error will be returned either if the string/file cannot be parsed,
    /// or if assignment matrix is invalid, which happens if any of these are true:
    /// - matrix is empty,
    /// - rows are empty,
    /// - rows have unequal lengths, or
    /// - some weights are negative.
    pub fn from_yaml<R: Read>(reader: R) -> anyhow::Result<Self> {
        let config: Self = serde_yaml::from_reader(reader).context("Failed to parse config")?;
        config.verify()
    }

    fn verify(self) -> anyhow::Result<Self> {
        let nodes = match self.assignment.weights.get(0) {
            Some(row) => row.len(),
            None => bail!("Assignment matrix cannot be empty"),
        };
        ensure!(nodes > 0, "Assignment matrix rows cannot be empty");
        for row in &self.assignment.weights {
            ensure!(
                row.len() == nodes,
                "All rows in assignment matrix must be equal"
            );
            for &weight in row {
                ensure!(weight >= 0.0, "Weights must be non-negative ({})", weight);
            }
        }
        Ok(self)
    }
}

impl FromStr for Config {
    type Err = anyhow::Error;
    fn from_str(config: &str) -> Result<Self, Self::Err> {
        Config::from_yaml(Cursor::new(config))
    }
}

/// Type of routing policy.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RoutingPolicy {
    /// Probabilistic static routing: select a replica randomly based on routing probabilities.
    Static,
}

impl Default for RoutingPolicy {
    fn default() -> Self {
        Self::Static
    }
}

/// Query data.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Query {
    /// Shards that will be selected for this query. If `None`, then all shards selected.
    pub selected_shards: Option<Vec<ShardId>>,
    /// Shard selection time.
    #[serde(default)]
    pub selection_time: u64,
    /// A list of retrieval times in all shards. This is the reference time that later can
    /// be scaled in nodes by multiplying by a retrieval speed factor.
    /// By default, though, all nodes are identical in terms of processing power.
    pub retrieval_times: Vec<u64>,
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_config() -> anyhow::Result<()> {
        let input = r#"
brokers: 2
query_distribution:
    mean: 10
    std: 1
seed: 17
time_unit: milli
cpus_per_node: 2
assignment:
    - [0, 1, 0]
    - [1, 1, 0]
    - [0, 1, 1]"#;
        let config = Config::from_yaml(Cursor::new(input))?;
        assert_eq!(config.brokers, 2);
        assert_eq!(config.query_distribution.mean, 10.0);
        assert_eq!(config.query_distribution.std, 1.0);
        assert_eq!(config.time_unit, TimeUnit::Milli);
        assert_eq!(config.cpus_per_node, 2);
        assert_eq!(config.seed, Some(17));
        assert_eq!(config.routing, RoutingPolicy::Static);
        assert!(
            config.assignment.weights().partial_cmp(&vec![
                vec![0.0, 1.0, 0.0],
                vec![1.0, 1.0, 0.0],
                vec![0.0, 1.0, 1.0]
            ]) == Some(std::cmp::Ordering::Equal)
        );
        assert_eq!(config.assignment.num_shards(), 3);
        Ok(())
    }

    #[test]
    fn test_verify_fails_empty_assignment() {
        let input = r#"
brokers: 1
query_distribution:
    mean: 10
    std: 1
cpus_per_node: 1
assignment: []"#;
        assert_eq!(
            &format!("{}", Config::from_str(input).unwrap_err()),
            "Assignment matrix cannot be empty"
        );
    }

    #[test]
    fn test_verify_fails_empty_rows() {
        let input = r#"
brokers: 1
cpus_per_node: 1
query_distribution:
    mean: 10
    std: 1
assignment:
    - []
    - [1, 2]"#;
        assert_eq!(
            &format!("{}", Config::from_str(input).unwrap_err()),
            "Assignment matrix rows cannot be empty"
        );
    }

    #[test]
    fn test_verify_fails_rows_unequal() {
        let input = r#"
brokers: 1
cpus_per_node: 1
query_distribution:
    mean: 10
    std: 1
assignment:
    - [1]
    - [1, 0]"#;
        assert_eq!(
            &format!("{}", Config::from_str(input).unwrap_err()),
            "All rows in assignment matrix must be equal"
        );
    }

    #[test]
    fn test_verify_fails_negative_weight() {
        let input = r#"
brokers: 1
cpus_per_node: 1
query_distribution:
    mean: 10
    std: 1
assignment:
    - [1, 0]
    - [1, -2]"#;
        assert_eq!(
            format!("{}", Config::from_str(input).unwrap_err()),
            format!("Weights must be non-negative ({})", -2)
        );
    }

    #[test]
    fn test_query() -> anyhow::Result<()> {
        assert_eq!(
            Query {
                selected_shards: None,
                selection_time: 0,
                retrieval_times: vec![1, 2, 3, 4, 5],
            },
            serde_json::from_str(&r#"{"retrieval_times": [1, 2, 3, 4, 5]}"#)?
        );
        assert_eq!(
            Query {
                selected_shards: Some(vec![ShardId(0), ShardId(1), ShardId(3), ShardId(10)]),
                selection_time: 10,
                retrieval_times: vec![1, 2, 3, 4, 5],
            },
            serde_json::from_str(
                &r#"
                {
                    "retrieval_times": [1, 2, 3, 4, 5],
                    "selected_shards": [0, 1, 3, 10],
                    "selection_time": 10
                }"#
            )?
        );
        Ok(())
    }
}
