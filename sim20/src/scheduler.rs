use std::any::{Any, TypeId};
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};
use std::time::Duration;

use super::ComponentId;

/// Entry type stored in the scheduler, including the event value, component ID, and the time when
/// it is supposed to occur.
#[derive(Debug)]
pub struct EventEntry {
    time: Reverse<Duration>,
    component: usize,
    inner: Box<dyn Any>,
    event_type: TypeId,
}

impl PartialEq for EventEntry {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}

impl Eq for EventEntry {}

impl PartialOrd for EventEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.time.partial_cmp(&other.time)
    }
}

impl Ord for EventEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time.cmp(&other.time)
    }
}

/// Scheduler is used to keep the current time and information about the upcoming events.
pub struct Scheduler {
    events: HashMap<TypeId, BinaryHeap<EventEntry>>,
    clock: Duration,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self {
            events: HashMap::new(),
            clock: Duration::default(),
        }
    }
}

impl Scheduler {
    /// Schedules `event` to be executed for `component` at `self.time() + time`.
    pub fn schedule<E: 'static>(&mut self, time: Duration, component: ComponentId<E>, event: E) {
        let time = self.time() + time;
        self.events
            .entry(TypeId::of::<E>())
            .or_default()
            .push(EventEntry {
                time: Reverse(time),
                component: component.id,
                inner: Box::new(event),
                event_type: TypeId::of::<E>(),
            });
    }

    /// Returns the current simulation time.
    pub fn time(&self) -> Duration {
        self.clock
    }

    /// Removes and returns the next scheduled event or `None` if none are left.
    pub fn pop(&mut self) -> Option<EventEntry> {
        let tid = self
            .events
            .iter()
            .filter_map(|(t, h)| h.peek().map(|e| (t, e.time)))
            .max_by_key(|(_, time)| *time)
            .map(|(tid, _)| *tid);
        tid.and_then(|t| self.events.get_mut(&t).unwrap().pop())
            .map(|e| {
                self.clock = e.time.0;
                e
            })
    }
}
