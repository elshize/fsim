//! Produce random shard partitioning from a text file with document titles.

#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::default_trait_access)]

use indicatif::ProgressIterator;
use itertools::Itertools;
use rand::{seq::SliceRandom, Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use structopt::StructOpt;

/// Produce random shard partitioning from a text file with document titles.
#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short, long)]
    /// Input file. Contains document titles (TREC IDs) in separate lines.
    input: PathBuf,

    #[structopt(short, long)]
    /// Directory where to write the produced files.
    output_dir: PathBuf,

    #[structopt(long)]
    /// Number of shards
    shards: usize,

    #[structopt(long)]
    /// Random seed.
    seed: Option<u64>,
}

fn count_lines(path: &Path) -> anyhow::Result<usize> {
    let mut count: usize = 0;
    for byte in BufReader::new(File::open(path)?).bytes() {
        if byte? == b'\n' {
            count += 1;
        }
    }
    Ok(count)
}

/// Given a shuffled range `0..ids.len()` and desired number of shards,
/// returns an iterator of shard assignments for consecutive documents, i.e.,
/// the first element is the shard ID of document at index 0, the second is the shard
/// of document 1, and so on.
fn assignment<'a>(ids: &'a [usize], num_shards: usize) -> impl Iterator<Item = usize> + 'a {
    let shard_size = (ids.len() + num_shards - 1) / num_shards;
    ids.chunks(shard_size)
        .into_iter()
        .enumerate()
        .map(|(shard, documents)| {
            let mut documents: Vec<_> = documents.iter().map(|d| (d, shard)).collect();
            documents.sort();
            documents
        })
        .kmerge()
        .map(|(_, s)| s)
}

fn prepare_output(dir: &Path) -> anyhow::Result<()> {
    if dir.exists() {
        if dir.is_dir() {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Cannot create {} -- it exists and is a file.",
                dir.display()
            ))
        }
    } else {
        fs::create_dir_all(dir)?;
        Ok(())
    }
}

fn init_seed(opt: &Opt) -> anyhow::Result<u64> {
    let seed = opt.seed.unwrap_or_else(|| rand::thread_rng().gen::<u64>());
    fs::write(opt.output_dir.join("seed"), seed.to_string())?;
    Ok(seed)
}

fn shuffled_ids(seed: u64, num_documents: usize) -> Vec<usize> {
    let mut ids: Vec<_> = (0..num_documents).collect();
    let mut rng = ChaChaRng::seed_from_u64(seed);
    ids.shuffle(&mut rng);
    ids
}

fn write_shards<R: BufRead, A: Iterator<Item = usize>>(
    num_shards: usize,
    input: R,
    assignment: A,
    output_dir: &Path,
) -> anyhow::Result<()> {
    let outputs: Result<Vec<_>, _> = (0..num_shards)
        .map(|idx| File::create(output_dir.join(idx.to_string())))
        .collect();
    let mut outputs = outputs?;
    for (document, shard) in input.lines().zip(assignment).progress() {
        write!(&mut outputs[shard], "{}", document?)?;
    }
    Ok(())
}

fn run(opt: Opt) -> anyhow::Result<()> {
    prepare_output(&opt.output_dir)?;
    let ids = shuffled_ids(init_seed(&opt)?, count_lines(&opt.input)?);
    let input = BufReader::new(File::open(&opt.input)?);
    write_shards(
        opt.shards,
        input,
        assignment(&ids, opt.shards),
        &opt.output_dir,
    )?;
    Ok(())
}

fn main() {
    if let Err(err) = run(Opt::from_args()) {
        println!("{}", err);
    }
}
