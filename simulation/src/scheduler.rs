use std::any::{Any, TypeId};
use std::cell::Cell;
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};
use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;

use crate::{Clock, ComponentId};

/// Entry type stored in the scheduler, including the event value, component ID, and the time when
/// it is supposed to occur.
#[derive(Debug)]
pub struct EventEntry {
    time: Reverse<Duration>,
    component: usize,
    inner: Box<dyn Any>,
    event_type: TypeId,
    state_hash: u64,
}

impl EventEntry {
    /// Tries to downcast the event entry to one holding an event of type `E`.
    /// If fails, returns `None`.
    #[must_use]
    pub fn downcast<E: fmt::Debug + 'static>(&self) -> Option<EventEntryTyped<'_, E>> {
        if self.event_type == TypeId::of::<E>() {
            let event = self.inner.downcast_ref::<E>().unwrap();
            Some(EventEntryTyped {
                time: self.time.0,
                component_id: ComponentId {
                    id: self.component,
                    state_hash: self.state_hash,
                    _marker: PhantomData,
                },
                component_idx: self.component,
                event,
            })
        } else {
            None
        }
    }
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

#[derive(Debug)]
pub struct EventEntryTyped<'e, E: fmt::Debug> {
    pub time: Duration,
    pub component_id: ComponentId<E>,
    pub component_idx: usize,
    pub event: &'e E,
}

/// Trait implemented by objects maintaining the current simulation time.
pub trait Time {
    /// Return the current simulation time.
    fn time(&self) -> Duration;
}

/// This struct has only immutable access to the simulation clock exposed.
pub struct ClockRef {
    clock: Clock,
}

impl From<Clock> for ClockRef {
    fn from(clock: Clock) -> Self {
        Self { clock }
    }
}

impl ClockRef {
    /// Return the current simulation time.
    #[must_use]
    pub fn time(&self) -> Duration {
        self.clock.get()
    }
}

/// Scheduler is used to keep the current time and information about the upcoming events.
pub struct Scheduler {
    events: HashMap<TypeId, BinaryHeap<EventEntry>>,
    clock: Clock,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self {
            events: HashMap::new(),
            clock: Rc::new(Cell::new(Duration::default())),
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
                state_hash: component.state_hash,
            });
    }

    /// Schedules `event` to be executed for `component` at `self.time()`.
    pub fn schedule_immediately<E: 'static>(&mut self, component: ComponentId<E>, event: E) {
        self.schedule(Duration::default(), component, event);
    }

    /// Returns the current simulation time.
    #[must_use]
    pub fn time(&self) -> Duration {
        self.clock.get()
    }

    /// Returns a structure with immutable access to the simulation time.
    #[must_use]
    pub fn clock(&self) -> ClockRef {
        ClockRef {
            clock: Rc::clone(&self.clock),
        }
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
                self.clock.replace(e.time.0);
                e
            })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_event_entry_downcast() {
        let entry = EventEntry {
            time: Reverse(Duration::from_secs(1)),
            component: 2,
            event_type: TypeId::of::<String>(),
            state_hash: 17,
            inner: Box::new(String::from("inner")),
        };
        assert!(entry.downcast::<String>().is_some());
        assert!(entry.downcast::<i32>().is_none());
    }

    #[test]
    fn test_event_entry_cmp() {
        let make_entry = || EventEntry {
            time: Reverse(Duration::from_secs(1)),
            component: 2,
            event_type: TypeId::of::<String>(),
            state_hash: 17,
            inner: Box::new(String::from("inner")),
        };
        assert_eq!(
            EventEntry {
                time: Reverse(Duration::from_secs(1)),
                ..make_entry()
            },
            EventEntry {
                time: Reverse(Duration::from_secs(1)),
                ..make_entry()
            }
        );
        assert_eq!(
            EventEntry {
                time: Reverse(Duration::from_secs(0)),
                ..make_entry()
            }
            .cmp(&EventEntry {
                time: Reverse(Duration::from_secs(1)),
                ..make_entry()
            }),
            Ordering::Greater
        );
        assert_eq!(
            EventEntry {
                time: Reverse(Duration::from_secs(2)),
                ..make_entry()
            }
            .cmp(&EventEntry {
                time: Reverse(Duration::from_secs(1)),
                ..make_entry()
            }),
            Ordering::Less
        );
    }

    #[derive(Debug, Clone, Eq, PartialEq)]
    struct EventA;
    #[derive(Debug, Clone, Eq, PartialEq)]
    struct EventB;

    #[test]
    fn test_scheduler() {
        let mut scheduler = Scheduler::default();
        assert_eq!(scheduler.time(), Duration::new(0, 0));
        assert!(scheduler.events.is_empty());

        let component_a = ComponentId::<EventA> {
            id: 0,
            state_hash: 17,
            _marker: PhantomData,
        };
        let component_b = ComponentId::<EventB> {
            id: 1,
            state_hash: 17,
            _marker: PhantomData,
        };

        scheduler.schedule(Duration::from_secs(1), component_a, EventA);
        scheduler.schedule(Duration::from_secs(0), component_b, EventB);
        scheduler.schedule(Duration::from_secs(2), component_b, EventB);

        assert_eq!(scheduler.time(), Duration::from_secs(0));

        let entry = scheduler.pop().unwrap();
        assert_eq!(entry.event_type, TypeId::of::<EventB>());
        let entry = entry.downcast::<EventB>().unwrap();
        assert_eq!(entry.time, Duration::from_secs(0));
        assert_eq!(entry.component_idx, 1);
        assert_eq!(entry.component_id, component_b);
        assert_eq!(entry.event, &EventB);

        assert_eq!(scheduler.time(), Duration::from_secs(0));

        let entry = scheduler.pop().unwrap();
        assert_eq!(entry.event_type, TypeId::of::<EventA>());
        let entry = entry.downcast::<EventA>().unwrap();
        assert_eq!(entry.time, Duration::from_secs(1));
        assert_eq!(entry.component_idx, 0);
        assert_eq!(entry.component_id, component_a);
        assert_eq!(entry.event, &EventA);

        assert_eq!(scheduler.time(), Duration::from_secs(1));

        let entry = scheduler.pop().unwrap();
        assert_eq!(entry.event_type, TypeId::of::<EventB>());
        let entry = entry.downcast::<EventB>().unwrap();
        assert_eq!(entry.time, Duration::from_secs(2));
        assert_eq!(entry.component_idx, 1);
        assert_eq!(entry.component_id, component_b);
        assert_eq!(entry.event, &EventB);

        assert_eq!(scheduler.time(), Duration::from_secs(2));

        assert!(scheduler.pop().is_none());
    }
}
