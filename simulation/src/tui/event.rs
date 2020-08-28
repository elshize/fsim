//! Handles TUI events.

#![cfg_attr(tarpaulin, skip)]

use std::collections::HashSet;
use std::io;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use termion::event::Key;
use termion::input::TermRead;

/// TUI event.
#[derive(Debug, Copy, Clone)]
pub enum Event {
    /// Key input has been passed.
    Input(Key),
    /// No input, only timer tick.
    Tick,
}

/// Construct new event server.
pub fn init_event_receiver(tick_rate: Duration, exit_keys: HashSet<Key>) -> mpsc::Receiver<Event> {
    let (tx, rx) = mpsc::channel();
    {
        let tx = tx.clone();
        thread::spawn(move || {
            let stdin = io::stdin();
            for evt in stdin.keys() {
                if let Ok(key) = evt {
                    if tx.send(Event::Input(key)).is_err() {
                        return;
                    }
                    if exit_keys.contains(&key) {
                        return;
                    }
                }
            }
        })
    };
    thread::spawn(move || loop {
        tx.send(Event::Tick).unwrap();
        thread::sleep(tick_rate);
    });
    rx
}