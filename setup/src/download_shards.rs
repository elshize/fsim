//! Download shard partitions.

#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::default_trait_access)]

use setup::{download_file, Collection};

use std::path::PathBuf;

use structopt::StructOpt;
use url::Url;

const URL: &str = "http://boston.lti.cs.cmu.edu/appendices/CIKM2016-Dai/";

/// Downloads shard partitioning from
/// <http://boston.lti.cs.cmu.edu/appendices/CIKM2016-Dai/>
/// and renames the files to start from 0.
#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short, long, possible_values = &["gov2", "cw09b"], conflicts_with = "url")]
    /// Use default URL for one of the known collections.
    collection: Collection,

    #[structopt(short, long)]
    /// Directory where to write the downloaded files.
    output_dir: PathBuf,
}

impl Opt {
    /// Resolves the URL.
    fn url(&self) -> Url {
        let dir = format!("{}-qkld-qinit/", self.collection);
        Url::parse(URL).unwrap().join(&dir).unwrap()
    }
}

fn run(opt: &Opt) -> anyhow::Result<()> {
    for shard in 0..opt.collection.num_shards() {
        let src = opt.url().join(&(shard + 1).to_string())?;
        let dst = opt.output_dir.join(shard.to_string());
        eprintln!("{} ==> {}", src.as_str(), dst.display());
        download_file(&src, dst)?;
    }
    Ok(())
}

fn main() {
    if let Err(err) = run(&Opt::from_args()) {
        println!("{}", err);
    }
}
