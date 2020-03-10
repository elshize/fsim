//! This program starts a simulation. TODO: More docs to come.

#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::default_trait_access)]

use anyhow::{Context, Result};
use fern::colors::{Color, ColoredLevelConfig};
use fsim::{config::Config, QueryRoutingSimulation};
use std::fs::File;
use std::io::{stdin, BufRead, BufReader};
use std::path::PathBuf;
use structopt::StructOpt;

fn setup_logger(opt: &Opt) -> Result<(), fern::InitError> {
    let colors = ColoredLevelConfig::new()
        .error(Color::Red)
        .warn(Color::Yellow)
        .info(Color::White)
        .debug(Color::White)
        .trace(Color::BrightBlack);
    let mut dispatch = fern::Dispatch::new().format(move |out, message, record| {
        out.finish(format_args!(
            "[{}] {}",
            colors.color(record.level()),
            message
        ))
    });
    dispatch = if opt.trace {
        dispatch.level(log::LevelFilter::Trace)
    } else {
        dispatch.level(log::LevelFilter::Debug)
    };
    dispatch.chain(std::io::stdout()).apply()?;
    Ok(())
}

#[derive(Debug, StructOpt)]
struct Opt {
    /// Simulation configuration file.
    #[structopt(short, long)]
    config: PathBuf,
    /// Simulation configuration file.
    queries: Option<PathBuf>,
    #[structopt(short, long)]
    /// Print trace messages.
    trace: bool,
}

struct Input<'a>(Box<dyn BufRead + 'a>);

impl<'a> Input<'a> {
    fn new(path: Option<PathBuf>) -> Result<Self> {
        Ok(match path {
            Some(p) => Input(Box::new(BufReader::new(File::open(p)?))),
            None => Input(Box::new(BufReader::new(stdin()))),
        })
    }
    fn reader(&mut self) -> &mut Box<dyn BufRead + 'a> {
        &mut self.0
    }
}

fn run(opt: Opt) -> Result<()> {
    setup_logger(&opt).context("Could not set up logger")?;
    let config = Config::from_yaml(File::open(opt.config)?)?;
    println!("{:?}", config);
    let queries: Result<Vec<fsim::config::Query>> =
        serde_json::Deserializer::from_reader(Input::new(opt.queries)?.reader())
            .into_iter()
            .map(|elem| elem.context("Failed to parse query"))
            .collect();
    let mut sim = QueryRoutingSimulation::from_config(config, queries?);
    sim.run_until(std::time::Duration::from_millis(10));
    Ok(())
}

fn main() {
    if let Some(err) = run(Opt::from_args()).err() {
        log::error!("{}", err);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::write;
    use tempdir::TempDir;

    #[test]
    fn test_input_from_file() -> Result<()> {
        let dir = TempDir::new("fsim")?;
        let file_path = dir.path().join("queries");
        write(&file_path, "INPUT")?;
        let mut input = Input::new(Some(file_path))?;
        let mut buf: Vec<u8> = Vec::new();
        input.reader().read_to_end(&mut buf)?;
        assert_eq!(&String::from_utf8(buf).unwrap(), "INPUT");
        Ok(())
    }
}
