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

use optimization::{AssignmentBuilder, AssignmentMethod, Dimension, ReplicaAssignment, ShardLoads};

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::iter::repeat;
use std::path::{Path, PathBuf};

use anyhow::{Error, Result};
use indicatif::ProgressBar;
use ndarray::Array1;
use ordered_float::OrderedFloat;
use rand::SeedableRng;
use rand_chacha::ChaChaRng;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use structopt::StructOpt;

/// For testing: CW09B+selective+natural
#[allow(clippy::all, clippy::pedantic)]
pub const SHARD_LOADS: [f32; 128] = [
    2769.4465, 2563.0969, 1658.6009, 1907.1591, 4095.8377, 2595.4211, 1902.8827, 1871.9019,
    2811.6678, 1330.1244, 2173.1014, 1736.8571, 1910.7388, 3055.4968, 2306.2665, 3043.5685,
    2606.7585, 1864.2502, 1463.707, 5827.256, 2743.4234, 3540.5672, 1845.7381, 3852.1842,
    1393.0152, 1712.4744, 3049.0203, 2669.7383, 2727.4197, 1815.9953, 2097.3796, 4033.9158,
    1670.9638, 2398.5224, 2727.7015, 1681.0979, 1143.6434, 3077.4799, 1934.5515, 2077.0641,
    3472.3239, 3767.8709, 1996.7776, 2122.1478, 2598.968, 2673.3812, 1370.5703, 2553.3114,
    3621.8528, 3332.3947, 3298.2937, 2321.8184, 1651.1206, 1599.3494, 2775.6897, 933.9928,
    2308.0888, 2967.4318, 2489.4295, 2660.5304, 3875.0388, 3459.9756, 2933.5879, 2302.4254,
    2456.0884, 2866.4099, 2222.7688, 2949.4361, 772.0176, 3036.9805, 734.1402, 2389.1279, 931.8882,
    2351.7616, 3564.0863, 2805.4057, 2414.5001, 3401.5756, 2094.716, 2065.8288, 3419.5094, 2417.12,
    2622.0397, 1560.3342, 1112.5524, 1140.2073, 2929.6292, 2854.6724, 2502.3643, 2123.6761,
    1784.0063, 1504.3259, 3160.18, 2189.8444, 2562.0229, 559.5894, 782.1046, 3110.4198, 2694.6023,
    2192.6533, 2190.0165, 3324.3018, 2680.1467, 2323.2137, 2491.8795, 3858.0141, 1483.7859,
    2896.5784, 3752.6631, 1996.6473, 2060.7296, 4128.9895, 1560.5123, 2694.7296, 2868.7403,
    2571.9589, 1845.6197, 3914.823, 3462.8996, 1884.551, 2930.887, 3339.6855, 1777.2353, 3382.8745,
    1000.3053, 4795.2733, 2168.9426, 3044.6332,
];

/// For testing: CW09B+selective+natural
#[allow(clippy::all, clippy::pedantic)]
pub const SHARD_VOLUMES: [f32; 128] = [
    22.2293262, 12.6619292, 9.9792899, 7.4227925, 35.9115986, 16.9188487, 12.718849, 9.8888731,
    27.6115509, 11.1012769, 29.3419464, 9.2526493, 10.9776041, 31.5410774, 17.0611388, 21.2793979,
    19.5846393, 10.1899121, 9.5617223, 77.8259615, 22.5645708, 44.5621033, 9.1685526, 28.5377459,
    7.7961363, 7.8787045, 15.6376474, 13.5616299, 20.7264479, 9.2617893, 13.2923171, 38.9188564,
    9.1492336, 20.7700482, 20.0522159, 7.1870716, 3.6720468, 20.2183387, 13.4836079, 21.5389323,
    23.1554164, 31.5159969, 10.4453579, 15.6147215, 19.9352025, 18.8024051, 5.2953479, 21.3048384,
    30.890757, 17.7598981, 30.1640823, 12.5522911, 9.9624533, 8.6006635, 24.4348158, 4.0249707,
    13.9640678, 19.9929309, 16.0116179, 21.3256179, 34.1877979, 24.0736217, 24.9283799, 16.4821927,
    25.7021179, 23.7681685, 17.8688572, 22.3077097, 3.8391571, 23.1142085, 13.5761679, 14.8221898,
    3.3282193, 17.6892673, 36.9519875, 20.1374695, 12.2232552, 24.2528947, 14.9436307, 14.8295564,
    37.9661578, 15.0037191, 18.7673903, 6.1808815, 7.3814802, 8.4884903, 15.6926281, 16.2914561,
    19.7665783, 15.3217833, 22.558547, 7.5832559, 33.4387182, 14.7449038, 12.2888097, 2.0108969,
    1.6852314, 17.7036442, 15.576595, 17.8193341, 20.4416948, 22.2861352, 15.4434949, 10.7370789,
    22.1935833, 31.5384887, 5.156956, 26.392024, 23.8591493, 7.499831, 9.3541704, 37.9407141,
    11.7599145, 24.7952155, 24.5642507, 19.2584626, 11.7979558, 28.4853907, 34.2552787, 17.240649,
    26.3187414, 27.1509591, 9.4575177, 20.5271572, 5.6577814, 43.6696296, 12.5498856, 22.7881003,
];

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(long, required_unless = "shard-loads")]
    query_times: Option<PathBuf>,

    #[structopt(long, required_unless = "query-times")]
    shard_loads: Option<PathBuf>,

    #[structopt(short, long)]
    num_machines: usize,
}

impl Opt {
    fn shard_loads(&self) -> Result<ShardLoads> {
        if let Some(query_times_file) = &self.query_times {
            calc_shard_loads(&query_times_file)
        } else {
            read_shard_loads(self.shard_loads.as_ref().unwrap())
        }
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
struct AssignmentResult {
    max_load: f32,
    min_load: f32,
    avg_load: f32,
    med_load: f32,
    cv: f32,
    loads: Vec<f32>,
    counts: Vec<usize>,
}

fn median_from_sorted(values: &[f32]) -> f32 {
    let mid = values.get(values.len() / 2).copied().unwrap_or(0.0);
    let mid_prev = values.get((values.len() - 1) / 2).copied().unwrap_or(0.0);
    (mid + mid_prev) / 2.0
}

impl AssignmentResult {
    fn new(assignment: &ReplicaAssignment, shard_loads: &ShardLoads) -> Self {
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
                    / 3.0
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
        }
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
                .ok_or_else(|| anyhow::anyhow!("Missing shard {} from query times file", s))
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
        .replicas(repeat(3).take(num_shards).collect())
        .progress_bar(progress_bar);
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
        &serde_json::to_string(&AssignmentResult::new(&assignment, &shard_loads))?
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
