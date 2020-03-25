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
use std::cell::RefCell;
use std::fs::File;
use std::io;
use std::io::{stdin, BufRead, BufReader};
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;
use structopt::StructOpt;
use strum::IntoEnumIterator;
use termion::event::Key;
use termion::raw::IntoRawMode;
use tui::backend::TermionBackend;
use tui::Terminal;

mod app;
use app::{App, Mode, View, Window};
mod event;
mod keys;
mod ui;
use keys::{ActivePaneAction, GlobalAction, KeyBindings, KeyHandler, NavigationAction};

#[derive(Debug, StructOpt)]
struct Opt {
    /// Simulation configuration file.
    #[structopt(short, long, required_unless("key-bindings"))]
    config: Option<PathBuf>,
    /// Simulation configuration file.
    queries: Option<PathBuf>,
    #[structopt(long, conflicts_with("debug"))]
    /// Print trace messages.
    trace: bool,
    #[structopt(long, conflicts_with("trace"))]
    /// Print debug messages.
    debug: bool,
    #[structopt(long)]
    /// Print logs to a file.
    log_file: Option<PathBuf>,
    #[structopt(long)]
    /// Print key bindings.
    key_bindings: bool,
    #[structopt(long, default_value = "1000")]
    /// Print out up to this many log entries at a time.
    log_history_size: usize,
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

fn format_key(key: Key) -> String {
    match key {
        Key::Char('\n') => format!("Enter"),
        Key::Char(c) => format!("{}", c),
        Key::Ctrl(c) => format!("Ctrl+{}", c),
        _ => format!("{:?}", key),
    }
}

fn run(opt: Opt) -> Result<()> {
    use Mode::*;

    if opt.key_bindings {
        let keys = KeyBindings::default();
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
        return Ok(());
    }

    fsim::logger::LoggerBuilder::default()
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
    let queries: Result<Vec<fsim::config::Query>> =
        serde_json::Deserializer::from_reader(Input::new(opt.queries)?.reader())
            .into_iter()
            .map(|elem| elem.context("Failed to parse query"))
            .collect();
    let app = Rc::new(RefCell::new(
        App::new(QueryRoutingSimulation::from_config(config, queries?))
            .with_log_history_size(opt.log_history_size),
    ));
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

    let key_handler = KeyHandler::new(Rc::clone(&app));

    loop {
        terminal.draw(|mut f| app.borrow_mut().draw(&mut f))?;

        app.borrow_mut().window = match events.next()? {
            Event::Input(key) => match key_handler.global_action(key) {
                Ok(GlobalAction::Exit) => {
                    break;
                }
                Ok(GlobalAction::NextStep) => {
                    app.borrow_mut().next_step();
                    app.borrow().window
                }
                Ok(GlobalAction::PrevStep) => {
                    app.borrow_mut().prev_step();
                    app.borrow().window
                }
                Ok(GlobalAction::NextSecond) => {
                    app.borrow_mut().next_second();
                    app.borrow().window
                }
                Ok(GlobalAction::PrevSecond) => {
                    app.borrow_mut().prev_second();
                    app.borrow().window
                }
                Err(key) => match app.borrow().window {
                    Window::Main(view, Navigation) => key_handler.handle_navigation(view, key),
                    Window::Main(view, ActivePane) => key_handler.handle_active_pane(view, key),
                    Window::Maximized(view) => key_handler.handle_maximized(view, key),
                },
            },
            Event::Tick => app.borrow().window,
        };
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
