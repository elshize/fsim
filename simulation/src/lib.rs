#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::default_trait_access)]

//! General purpose simulation library that provides the mechanisms such as: scheduler, state,
//! queues, etc.
//!
//! # Usage
//!
//! To define a simulation, you must define the following inside [`simulation!`] macro:
//! - a trait that all event types will implement to ensure no other event types can be used,
//! - a simulation structure holding at least the following three fields: state, scheduler, and components.
//! See the example below.
//!
//! # Examples
//!
//! ```
//! # use simulation::{simulation, State, Scheduler, Components};
//!
//! #[derive(Debug)]
//! enum EventA {
//!     CaseA,
//!     CaseB,
//! }
//! #[derive(Debug)]
//! enum EventB {
//!     CaseA,
//!     CaseB,
//! }
//! simulation! {
//!     #[events(EventA, EventB)]
//!     trait ValidEvent {}
//!
//!     #[simulation]
//!     struct Simulation {
//!         state: State,
//!         scheduler: Scheduler,
//!         components: Components,
//!     }
//! }
//!
//! fn main() {
//!     let mut sim = Simulation::default();
//!     sim.step();
//! }
//! ```

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

pub use simulation_derive::simulation;

/// Simulation clock.
pub type Clock = Rc<Cell<Duration>>;

pub use component::{Component, ComponentId, Components};
pub use scheduler::{ClockRef, EventEntry, Scheduler, Time};
pub use state::{Key, QueueId, State};

use queue::Queue;

mod component;
mod queue;
mod scheduler;
mod state;
