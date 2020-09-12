//! Download shard probabilities.

#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::default_trait_access)]

use setup::Collection;

use anyhow::anyhow;
use structopt::StructOpt;
use url::Url;

const URL: &str = "http://boston.lti.cs.cmu.edu/appendices/SIGIR2017-Zhuyun-Dai/"; //cwb/trec_full.shardlist";

/// Downloads shard selections from
/// <http://boston.lti.cs.cmu.edu/appendices/SIGIR2017-Zhuyun-Dai/cwb/>
/// and computes the shard probabilities.
#[derive(Debug, StructOpt)]
struct Opt {
    /// Use default URL for one of the known collections.
    #[structopt(short, long, possible_values = &["gov2", "cw09b"])]
    collection: Collection,

    /// Number of selected shards for each query.
    #[structopt(short, long)]
    num_selected_shards: usize,
}

impl Opt {
    /// Resolves the URL.
    fn url(&self) -> Url {
        let dir = match self.collection {
            Collection::Gov2 => "gov2",
            Collection::Clueweb09b => "cwb",
        };
        Url::parse(URL)
            .unwrap()
            .join(&format!("{}/trec_fast.shardlist", dir))
            .unwrap()
    }
}

fn run(opt: &Opt) -> anyhow::Result<()> {
    let selections = attohttpc::get(opt.url()).send()?.text()?;
    let mut counts = vec![1_usize; opt.collection.num_shards()];
    for selection in selections.lines() {
        for shard in selection
            .split_whitespace()
            .into_iter()
            .skip(1) // skips the query ID
            .take(opt.num_selected_shards)
        {
            let shard: usize = shard
                .parse()
                .map_err(|e| anyhow!("Cannot parse to int: {} ({})", shard, e))?;
            let counter = counts.get_mut(shard - 1).ok_or_else(|| {
                anyhow!(
                    "Expected only {} shards but got shard #{}",
                    opt.collection.num_shards(),
                    shard
                )
            })?;
            *counter += 1;
        }
    }
    for count in counts {
        println!("{}", count as f32 / 200.0);
    }
    Ok(())
}

fn main() {
    if let Err(err) = run(&Opt::from_args()) {
        println!("{}", err);
    }
}
