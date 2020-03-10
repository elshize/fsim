use crate::ShardId;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

/// Network architecture, shard and replica assignments.
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct Network {
    /// Each element describes a computational node with a list of shards it contains.
    pub nodes: Vec<Vec<ShardId>>,
}

impl Network {
    /// Load network from a JSON file.
    ///
    /// # Examples
    ///
    /// In JSON, a network is represented as an array of arrays.
    /// Each of the inner arrays contains shard IDs in its node.
    /// Nodes are indexed with consecutive numbers, such that `j[0]` is an array of shards
    /// in node 0, `j[1]` of shards in node 1, etc.
    ///
    /// ```
    /// # fn main() -> anyhow::Result<()> {
    /// # use fsim::{Network, ShardId};
    /// # use tempdir::TempDir;
    /// # let dir = TempDir::new("")?;
    /// # let file = dir.path().join("file");
    /// std::fs::write(&file, "[[0, 1, 2], [3, 5], [6]]");
    /// let network = Network::from_json_file(file)?;
    /// assert_eq!(
    ///     network.nodes,
    ///     vec![
    ///         vec![ShardId(0), ShardId(1), ShardId(2)],
    ///         vec![ShardId(3), ShardId(5)],
    ///         vec![ShardId(6)],
    ///     ]
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_json_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        Ok(serde_json::from_reader(BufReader::new(File::open(path)?))?)
    }
}
