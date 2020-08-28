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

mod tui;

use crate::tui::{ActivePaneAction, App, GlobalAction, KeyBindings, NavigationAction};
use ::tui::backend::TermionBackend;
use ::tui::Terminal;
use anyhow::{Context, Result};
use simulation::{
    config::{self, Config},
    logger::LoggerBuilder,
    FlushingProgression, ReversibleProgression, Simulation,
};
use std::fs::File;
use std::io;
use std::io::Write;
use std::io::{stdin, BufRead, BufReader};
use std::path::PathBuf;
use structopt::StructOpt;
use strum::IntoEnumIterator;
use termion::event::Key;
use termion::raw::IntoRawMode;

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, StructOpt)]
struct Opt {
    /// Simulation configuration file.
    #[structopt(short, long, required_unless("key-bindings"))]
    config: Option<PathBuf>,

    /// Simulation configuration file.
    queries: Option<PathBuf>,

    /// Print trace messages.
    #[structopt(long, conflicts_with("debug"))]
    trace: bool,

    /// Print debug messages.
    #[structopt(long, conflicts_with("trace"))]
    debug: bool,

    /// Print logs to a file.
    #[structopt(long)]
    log_file: Option<PathBuf>,

    /// Print key bindings.
    #[structopt(long)]
    key_bindings: bool,

    /// Print out up to this many log entries at a time.
    #[structopt(long, default_value = "1000")]
    log_history_size: usize,

    /// Only run simulation for a given time and reports the data.
    #[structopt(long, requires = "time")]
    no_ui: bool,

    /// Running time of the simulation in units defined in the config.
    #[structopt(long)]
    time: Option<u64>,
}

struct Input<'a>(Box<dyn BufRead + 'a>);

impl<'a> Input<'a> {
    fn new(path: Option<PathBuf>) -> Result<Self> {
        Ok(match path {
            Some(p) => Input(Box::new(BufReader::new(File::open(p)?))),
            None => Input(Box::new(BufReader::new(stdin()))),
        })
    }
    fn reader(&mut self) -> &mut dyn BufRead {
        &mut self.0
    }
}

fn format_key(key: Key) -> String {
    match key {
        Key::Char('\n') => "Enter".to_string(),
        Key::Char(c) => format!("{}", c),
        Key::Ctrl(c) => format!("Ctrl+{}", c),
        _ => format!("{:?}", key),
    }
}

fn print_keybindings(keys: &KeyBindings) -> Result<()> {
    println!("--- Global ---");
    for action in GlobalAction::iter() {
        println!("{:?}", action);
        for key in keys.global_bindings(action) {
            println!("\t{}", format_key(key));
        }
    }
    println!("--- Navigation Mode ---");
    for action in NavigationAction::iter() {
        println!("{:?}", action);
        for key in keys.navigation_bindings(action) {
            println!("\t{}", format_key(key));
        }
    }
    println!("\n--- Active Pane Mode ---");
    for action in ActivePaneAction::iter() {
        println!("{:?}", action);
        for key in keys.active_pane_bindings(action) {
            println!("\t{}", format_key(key));
        }
    }
    Ok(())
}

fn run_no_ui(config: &Config, queries: Vec<config::Query>, time: u64) -> Result<()> {
    let stdout = io::stdout();
    let stderr = io::stderr();
    let mut sim = Simulation::new(
        &config,
        queries,
        FlushingProgression::new(stdout.lock(), stderr.lock()),
    );
    let length = sim.duration(time);
    println!("{:?}", length);
    sim.run_until(length);
    Ok(())
}

fn run_ui(config: &Config, queries: Vec<config::Query>, log_history_size: usize) -> Result<()> {
    let app = App::new(Simulation::new(
        &config,
        queries,
        ReversibleProgression::default(),
    ))
    .with_log_history_size(log_history_size);
    write!(io::stdout().into_raw_mode()?, "{}", termion::clear::All).unwrap();
    let stdout = io::stdout().into_raw_mode()?;
    let backend = TermionBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.hide_cursor()?;
    app.event_loop(&KeyBindings::default(), &mut terminal)?;
    terminal.show_cursor()?;
    Ok(())
}

fn run(opt: Opt) -> Result<()> {
    if opt.key_bindings {
        return print_keybindings(&KeyBindings::default());
    }

    LoggerBuilder::default()
        .level(if opt.trace {
            log::LevelFilter::Trace
        } else if opt.debug {
            log::LevelFilter::Debug
        } else {
            log::LevelFilter::Info
        })
        .target("fsim")
        .init()?;

    let config = Config::from_yaml(File::open(opt.config.unwrap())?)?;

    let queries: Result<Vec<config::Query>> =
        serde_json::Deserializer::from_reader(Input::new(opt.queries)?.reader())
            .into_iter()
            .map(|elem| elem.context("Failed to parse query"))
            .collect();

    if opt.no_ui {
        run_no_ui(&config, queries?, opt.time.unwrap())
    } else {
        run_ui(&config, queries?, opt.log_history_size)
    }
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
    use tempfile::TempDir;

    #[test]
    fn test_input_from_file() -> Result<()> {
        let dir = TempDir::new()?;
        let file_path = dir.path().join("queries");
        write(&file_path, "INPUT")?;
        let mut input = Input::new(Some(file_path))?;
        let mut buf: Vec<u8> = Vec::new();
        input.reader().read_to_end(&mut buf)?;
        assert_eq!(&String::from_utf8(buf).unwrap(), "INPUT");
        Ok(())
    }
}
