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
use event::{Event, Events};
use fsim::{config::Config, QueryRoutingSimulation};
use std::fs::File;
use std::io;
use std::io::{stdin, BufRead, BufReader};
use std::path::PathBuf;
use std::time::Duration;
use structopt::StructOpt;
use termion::event::Key;
use termion::raw::IntoRawMode;
use tui::backend::TermionBackend;
use tui::Terminal;

mod app;
use app::{App, Component};
mod event;
mod ui;

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
    #[structopt(long)]
    /// Print logs to a file.
    log_file: Option<PathBuf>,
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
    fsim::logger::LoggerBuilder::default()
        .level(if opt.trace {
            log::LevelFilter::Trace
        } else {
            log::LevelFilter::Debug
        })
        .target("fsim")
        .init()?;
    let config = Config::from_yaml(File::open(opt.config)?)?;
    let queries: Result<Vec<fsim::config::Query>> =
        serde_json::Deserializer::from_reader(Input::new(opt.queries)?.reader())
            .into_iter()
            .map(|elem| elem.context("Failed to parse query"))
            .collect();
    let mut app = App::new(QueryRoutingSimulation::from_config(config, queries?));
    let events = Events::with_config(event::Config {
        tick_rate: Duration::from_millis(200),
        exit_key: Key::Null,
    });

    use std::io::Write;
    write!(io::stdout().into_raw_mode()?, "{}", termion::clear::All).unwrap();

    let stdout = io::stdout().into_raw_mode()?;
    let backend = TermionBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.hide_cursor()?;

    loop {
        terminal.draw(|mut f| ui::draw(&mut f, &app))?;

        app.focus = match app.focus.take() {
            None => match events.next()? {
                Event::Input(key) => match key {
                    Key::Ctrl('c') => {
                        break;
                    }
                    Key::Left | Key::Char('h') | Key::Char(',') => {
                        app.prev();
                        None
                    }
                    Key::Right | Key::Char('l') | Key::Char('.') => {
                        app.next();
                        None
                    }
                    Key::Char('A') => {
                        let mut queries: Vec<_> = app.sim.status().active().cloned().collect();
                        queries.sort_by_key(|q| q.0.request);
                        Some(Component::Active {
                            queries,
                            selected: 0,
                        })
                    }
                    Key::Char('L') => Some(Component::Logs),
                    _ => None,
                },
                Event::Tick => None,
            },
            Some(Component::Active { queries, selected }) => match events.next()? {
                Event::Input(key) => match key {
                    Key::Ctrl('c') => {
                        break;
                    }
                    Key::Up | Key::Char('k') => Some(Component::Active {
                        queries,
                        selected: selected.checked_sub(1).unwrap_or(0),
                    }),
                    Key::Down | Key::Char('j') => Some(Component::Active {
                        queries,
                        selected: std::cmp::min(
                            selected + 1,
                            app.sim
                                .status()
                                .queries_active()
                                .checked_sub(1)
                                .unwrap_or(0),
                        ),
                    }),
                    Key::Char('\n') => Some(Component::ActiveDetails { queries, selected }),
                    Key::Esc | Key::Char('d') => None,
                    _ => Some(Component::Active { queries, selected }),
                },
                Event::Tick => Some(Component::Active { queries, selected }),
            },
            Some(Component::ActiveDetails { queries, selected }) => match events.next()? {
                Event::Input(key) => match key {
                    Key::Ctrl('c') => {
                        break;
                    }
                    Key::Esc | Key::Char('d') => Some(Component::Active { queries, selected }),
                    _ => Some(Component::ActiveDetails { queries, selected }),
                },
                _ => Some(Component::ActiveDetails { queries, selected }),
            },
            Some(Component::Logs) => match events.next()? {
                Event::Input(key) => match key {
                    Key::Ctrl('c') => {
                        break;
                    }
                    Key::Esc | Key::Char('d') => None,
                    _ => Some(Component::Logs),
                },
                _ => Some(Component::Logs),
            },
        }
    }
    terminal.show_cursor()?;
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
