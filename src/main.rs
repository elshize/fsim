//! This program starts a simulation. TODO: More docs to come.

#![feature(or_patterns)]
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
use keys::{ActivePaneAction, KeyBindings, KeyHandler, NavigationAction};

#[derive(Debug, StructOpt)]
struct Opt {
    /// Simulation configuration file.
    #[structopt(short, long, required_unless("key-bindings"))]
    config: Option<PathBuf>,
    /// Simulation configuration file.
    queries: Option<PathBuf>,
    #[structopt(short, long)]
    /// Print trace messages.
    trace: bool,
    #[structopt(long)]
    /// Print logs to a file.
    log_file: Option<PathBuf>,
    #[structopt(long)]
    /// Print key bindings.
    key_bindings: bool,
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
        } else {
            log::LevelFilter::Debug
        })
        .target("fsim")
        .init()?;
    let config = Config::from_yaml(File::open(opt.config.unwrap())?)?;
    let queries: Result<Vec<fsim::config::Query>> =
        serde_json::Deserializer::from_reader(Input::new(opt.queries)?.reader())
            .into_iter()
            .map(|elem| elem.context("Failed to parse query"))
            .collect();
    let app = Rc::new(RefCell::new(App::new(QueryRoutingSimulation::from_config(
        config, queries?,
    ))));
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
            Event::Input(Key::Ctrl('c')) => {
                break;
            }
            Event::Input(Key::Char('>')) => {
                app.borrow_mut().next();
                app.borrow().window
            }
            Event::Input(Key::Char('<')) => {
                app.borrow_mut().prev();
                app.borrow().window
            }
            Event::Input(key) => match app.borrow().window {
                Window::Main(view, Navigation) => key_handler.handle_navigation(view, key),
                Window::Main(view, ActivePane) => key_handler.handle_active_pane(view, key),
                Window::Maximized(view) => key_handler.handle_maximized(view, key),
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
