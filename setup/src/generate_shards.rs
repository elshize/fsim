//! Produce random or URL shard partitioning from a text file with document titles.

#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::default_trait_access)]

use indicatif::ProgressBar;
use indicatif::ProgressIterator;
use itertools::Itertools;
use rand::{seq::SliceRandom, Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use structopt::StructOpt;

/// Produce random or URL shard partitioning from a text file with document titles.
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

    #[structopt(long, conflicts_with = "url")]
    /// Random seed.
    seed: Option<u64>,

    #[structopt(long, conflicts_with = "seed")]
    /// Partition by URL.
    url: Option<PathBuf>,

    #[structopt(long)]
    /// Do not print progress.
    silent: bool,
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
fn assignment(ids: &[usize], num_shards: usize) -> impl Iterator<Item = usize> + '_ {
    let shard_size = (ids.len() + num_shards - 1) / num_shards;
    ids.chunks(shard_size)
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
    silent: bool,
) -> anyhow::Result<()> {
    let outputs: Result<Vec<_>, _> = (0..num_shards)
        .map(|idx| File::create(output_dir.join(idx.to_string())))
        .collect();
    let mut outputs = outputs?;
    let len = assignment.size_hint().0 as u64;
    let progress = if silent {
        ProgressBar::hidden()
    } else {
        ProgressBar::new(len)
    };
    for (document, shard) in input.lines().zip(assignment).progress_with(progress) {
        writeln!(&mut outputs[shard], "{}", document?)?;
    }
    Ok(())
}

fn run(opt: Opt) -> anyhow::Result<()> {
    prepare_output(&opt.output_dir)?;
    let permutation: Vec<_> = if let Some(url_path) = opt.url {
        let indexed_lines: Result<Vec<(String, usize)>, _> = BufReader::new(File::open(&url_path)?)
            .split(b'\n')
            .map(|buf| buf.map(|b| String::from_utf8_lossy(&b).to_string()))
            .enumerate()
            .map(|(idx, line)| line.map(|l| (l, idx)))
            .collect();
        let mut indexed_lines = indexed_lines?;
        indexed_lines.sort();
        indexed_lines.into_iter().map(|(_, doc)| doc).collect()
    } else {
        shuffled_ids(init_seed(&opt)?, count_lines(&opt.input)?)
    };
    let input = BufReader::new(File::open(&opt.input)?);
    write_shards(
        opt.shards,
        input,
        assignment(&permutation, opt.shards),
        &opt.output_dir,
        opt.silent,
    )?;
    Ok(())
}

fn main() {
    if let Err(err) = run(Opt::from_args()) {
        println!("{}", err);
    }
}

#[cfg(test)]
mod test {
    use super::{run, Opt};
    use itertools::Itertools;
    use proptest::prelude::*;
    use rand::{seq::SliceRandom, SeedableRng};
    use rand_chacha::ChaChaRng;
    use std::collections::BTreeSet;
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    use tempfile::TempDir;

    proptest! {
        #[test]
        fn test_by_url(seed: u64) {
            let temp = TempDir::new().unwrap();
            let urls_path = temp.path().join("urls");
            let docs_path = temp.path().join("docs");
            let mut data: Vec<(String, String)> = (0..=9)
                .map(|n| (format!("U{}", n), format!("D{}", n)))
                .collect();
            let mut rng = ChaChaRng::seed_from_u64(seed);
            data.shuffle(&mut rng);
            let (urls, docs): (Vec<String>, Vec<String>) = data.into_iter().unzip();
            std::fs::write(&urls_path, urls.join("\n"))?;
            std::fs::write(&docs_path, docs.join("\n"))?;

            run(Opt {
                input: docs_path,
                output_dir: temp.path().into(),
                shards: 3,
                seed: None,
                url: Some(urls_path),
                silent: true,
            }).unwrap();

            for (shard, docs) in (0..=9).chunks(4).into_iter().enumerate() {
                let shard_path = temp.path().join(shard.to_string());
                let actual: Result<BTreeSet<_>, _> =
                    BufReader::new(File::open(&shard_path)?).lines().collect();
                let expected: BTreeSet<_> = docs.map(|d| format!("D{}", d)).collect();
                assert_eq!(actual.unwrap(), expected);
            }
        }
    }
}
