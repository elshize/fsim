//! Assigns shard replicas to machines. Run `assign-replicas --help` for more information.

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

use optimization::{
    AssignmentBuilder, AssignmentMethod, AssignmentResult, Dimension, ReplicaAssignment, ShardLoads,
};

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::iter::repeat;
use std::path::{Path, PathBuf};

use color_eyre::eyre::{eyre, Error, Result};
use indicatif::ProgressBar;
use ndarray::Array1;
use ordered_float::OrderedFloat;
use rand::SeedableRng;
use rand_chacha::ChaChaRng;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(long, required_unless = "shard-loads")]
    query_times: Option<PathBuf>,

    #[structopt(long, required_unless = "query-times")]
    shard_loads: Option<PathBuf>,

    #[structopt(short, long)]
    num_machines: usize,

    #[structopt(short, long, default_value = "3")]
    replicas: u32,

    #[structopt(long)]
    shard_probabilities: Option<PathBuf>,
}

impl Opt {
    fn shard_loads(&self) -> Result<ShardLoads> {
        self.query_times.as_ref().map_or_else(
            || {
                read_shard_loads(
                    self.shard_loads
                        .as_ref()
                        .expect("either query times or shard loads must be present"),
                )
            },
            |q| calc_shard_loads(q),
        )
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
struct QueryTime {
    shard_id: u64,
    time: u64,
}

#[derive(Debug)]
struct Average {
    sum: u64,
    count: u64,
}

impl Default for Average {
    fn default() -> Self {
        Self { sum: 0, count: 0 }
    }
}

#[derive(Debug, Serialize)]
struct Node {
    node_id: usize,
    shards: Vec<usize>,
}

fn median_from_sorted(values: &[f32]) -> f32 {
    let mid = values.get(values.len() / 2).copied().unwrap_or(0.0);
    let mid_prev = values.get((values.len() - 1) / 2).copied().unwrap_or(0.0);
    (mid + mid_prev) / 2.0
}

fn assignment_result(
    assignment: &ReplicaAssignment,
    shard_loads: &ShardLoads,
    replicas: u32,
) -> AssignmentResult {
    let nodes = assignment
        .0
        .genrows()
        .into_iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .filter_map(|(idx, a)| if *a { Some(idx) } else { None })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let counts: Vec<_> = assignment
        .0
        .genrows()
        .into_iter()
        .map(|row| row.map(|&a| a as usize).sum())
        .collect();
    let loads = assignment
        .0
        .genrows()
        .into_iter()
        .map(|row| {
            row.iter()
                .zip(shard_loads.iter())
                .filter_map(|(&a, load)| if a { Some(*load) } else { None })
                .sum::<f32>()
                / (replicas as f32)
        })
        .collect::<Vec<_>>();
    let weights = assignment
        .0
        .genrows()
        .into_iter()
        .map(|row| {
            row.iter()
                .zip(shard_loads.iter())
                .map(|(&a, load)| if a { *load } else { 0.0 })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut sorted_loads = loads.clone();
    sorted_loads.sort_by_key(|&l| OrderedFloat(l));
    AssignmentResult {
        loads,
        counts,
        max_load: sorted_loads.last().copied().unwrap_or(0.0),
        min_load: sorted_loads.first().copied().unwrap_or(0.0),
        med_load: median_from_sorted(&sorted_loads),
        avg_load: match sorted_loads.len() {
            0 => 0.0,
            len => sorted_loads.iter().sum::<f32>() / len as f32,
        },
        cv: cv(&sorted_loads),
        nodes,
        weights,
    }
}

fn calc_shard_loads(query_times: &Path) -> Result<ShardLoads> {
    let file = File::open(query_times)?;
    let mut stats: HashMap<u64, Average> = HashMap::new();
    for row in Deserializer::from_reader(file).into_iter::<QueryTime>() {
        let QueryTime { shard_id, time } = row?;
        let avg = stats.entry(shard_id).or_insert_with(Average::default);
        avg.count += 1;
        avg.sum += time;
    }
    let num_shards = stats.len();
    let vec: Result<Vec<_>> = (0..num_shards as u64)
        .map(|s| {
            stats
                .get(&s)
                .map(|&Average { sum, count }| sum as f32 / count as f32)
                .ok_or_else(|| eyre!("Missing shard {} from query times file", s))
        })
        .collect();
    Ok(Array1::from(vec?).into())
}

fn read_shard_loads(input: &Path) -> Result<ShardLoads> {
    let file = File::open(input)?;
    let loads: Result<Vec<_>> = BufReader::new(file)
        .lines()
        .map(|l| {
            l.map_err(Error::from)
                .and_then(|l| l.parse::<f32>().map_err(Error::from))
        })
        .collect();
    Ok(Array1::from(loads?).into())
}

fn cv(values: &[f32]) -> f32 {
    let mean = values.iter().sum::<f32>() / values.len() as f32;
    let stddev =
        (values.iter().map(|v| (v - mean).powi(2)).sum::<f32>() / values.len() as f32).sqrt();
    stddev / mean
}

fn assign(opt: &Opt) -> Result<()> {
    let shard_loads = opt.shard_loads()?;
    let num_shards = shard_loads.len();
    let mut builder = AssignmentBuilder::new(Dimension {
        num_shards,
        num_machines: opt.num_machines,
    });
    let progress_bar = ProgressBar::new_spinner();
    builder
        .loads(shard_loads.clone())
        .replicas(repeat(opt.replicas).take(num_shards).collect())
        .progress_bar(progress_bar);
    if let Some(probabilities) = &opt.shard_probabilities {
        let probabilities: Vec<f32> = serde_json::from_reader(File::open(probabilities)?)?;
        builder.probabilities(probabilities.into_iter().collect());
    }
    let mut rng = ChaChaRng::from_entropy();
    let (assignment, _) = builder.assign(
        &mut rng,
        AssignmentMethod::Approximate {
            population: 100_000,
            iterations: 1_000_000,
        },
    )?;
    println!(
        "{}",
        &serde_json::to_string(&assignment_result(&assignment, &shard_loads, opt.replicas))?
    );
    Ok(())
}

fn main() {
    if let Err(err) = assign(&Opt::from_args()) {
        eprintln!("{}", err);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_calc_shard_loads() -> Result<()> {
        let tmp = TempDir::new()?;
        let input_file = tmp.path().join("query-times.jl");
        fs::write(
            &input_file,
            r#"{"sharding":"random","ordering":"bp","shard_id":0,"query_id":50,"time":1000}
{"sharding":"random","ordering":"bp","shard_id":0,"query_id":50,"time":3000}"#,
        )?;
        let loads = calc_shard_loads(&input_file)?;
        assert_eq!(loads.vec().first().unwrap(), &2000.0);

        fs::write(&input_file, r#"{}"#)?;
        assert!(calc_shard_loads(&input_file).is_err());

        fs::write(
            &input_file,
            r#"{"sharding":"random","ordering":"bp","shard_id":1,"query_id":50,"time":4427}"#,
        )?;
        assert!(calc_shard_loads(&input_file).is_err());
        Ok(())
    }
}
