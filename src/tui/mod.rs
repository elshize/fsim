//! Code related to the TUI application.

mod app;
mod event;
mod keys;
mod ui;

pub use app::App;
pub use keys::{ActivePaneAction, GlobalAction, KeyBindings, NavigationAction};
