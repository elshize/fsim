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

use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use structopt::StructOpt;
use url::Url;

const URL: &str = "http://boston.lti.cs.cmu.edu/appendices/CIKM2016-Dai/";

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Collection {
    Gov2,
    Clueweb09b,
}

impl FromStr for Collection {
    type Err = String;
    fn from_str(collection: &str) -> Result<Self, Self::Err> {
        match collection {
            "gov2" | "Gov2" | "GOV2" => Ok(Self::Gov2),
            "cw09b" | "CW09B" => Ok(Self::Clueweb09b),
            _ => Err(format!("Unknown collection: {}", collection)),
        }
    }
}

impl fmt::Display for Collection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Gov2 => write!(f, "GOV2"),
            Self::Clueweb09b => write!(f, "CW09B"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Clustering {
    KldRand,
    QkldRand,
    QkldQinit,
}

impl FromStr for Clustering {
    type Err = String;
    fn from_str(clustering: &str) -> Result<Self, Self::Err> {
        match clustering {
            "kld-rand" => Ok(Self::KldRand),
            "qkld-rand" => Ok(Self::QkldRand),
            "qkld-qinit" => Ok(Self::QkldQinit),
            _ => Err(format!("Unknown clustering: {}", clustering)),
        }
    }
}

impl fmt::Display for Clustering {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::KldRand => write!(f, "KLD-Rand"),
            Self::QkldRand => write!(f, "QKLD-Rand"),
            Self::QkldQinit => write!(f, "QKLD-QInit"),
        }
    }
}

/// Downloads shard partitioning from
/// [`http://boston.lti.cs.cmu.edu/appendices/CIKM2016-Dai/`](http://boston.lti.cs.cmu.edu/appendices/CIKM2016-Dai/)
#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short, long, possible_values = &["gov2", "cw09b"], conflicts_with = "url")]
    /// Use default URL for one of the known collections.
    collection: Collection,

    #[structopt(
        long,
        conflicts_with = "url",
        default_value = "qkld-rand",
        possible_values = &["kld-rand", "qkld-rand", "qkld-qinit"]
    )]
    /// Type of clustering.
    clustering: Clustering,

    #[structopt(short, long)]
    /// Directory where to write the downloaded files.
    output_dir: PathBuf,
}

impl Opt {
    /// Resolves the URL.
    fn url(&self) -> Url {
        let dir = format!("{}-{}/", self.collection, self.clustering);
        Url::parse(URL).unwrap().join(&dir).unwrap()
    }
    /// Number of shards for the requested collection.
    fn num_shards(&self) -> usize {
        match self.collection {
            Collection::Gov2 => 190,
            Collection::Clueweb09b => 128,
        }
    }
}

// fn download_text(url: Url) -> anyhow::Result<String> {
//     Ok(reqwest::blocking::get(url)?.text()?)
// }

fn download_file<P: AsRef<Path>>(src: Url, dest: P) -> anyhow::Result<()> {
    std::fs::write(dest, reqwest::blocking::get(src)?.text()?)?;
    Ok(())
}

fn run(opt: Opt) -> anyhow::Result<()> {
    for shard in 0..opt.num_shards() {
        let src = opt.url().join(&(shard + 1).to_string())?;
        let dst = opt.output_dir.join(shard.to_string());
        eprintln!("{} ==> {}", src.as_str(), dst.display());
        download_file(src, dst)?;
    }
    Ok(())
}

fn main() {
    if let Err(err) = run(Opt::from_args()) {
        println!("{}", err);
    }
}
