#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::default_trait_access)]

//! This is a general purpose simulation that provides the mechanisms such as: scheduler, state,
//! queues, etc.

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

pub use sim_derive::simulation;

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

// pub trait Simulation: Defult {
//     type Event;

//     #[must_use]
//     fn add_queue<V: 'static>(&mut self) -> QueueId<V>;

//     #[must_use]
//     fn add_bounded_queue<V: 'static>(&mut self, capacity: usize) -> QueueId<V>;

//     fn add_component<E: Event + 'static, C: Component<Event = E> + 'static>(
//         &mut self,
//         component: C,
//     ) -> ComponentId<E>;

//     fn schedule<E: 'static>(&mut self, time: Duration, component: ComponentId<E>, event: E);
// }
