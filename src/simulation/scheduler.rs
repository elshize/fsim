use super::{Event, Process};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Duration;

/// Event scheduler. Stores any future events in a priority queue.
pub struct Scheduler {
    /// Queue of the future events that will be processed in order of time.
    scheduled_events: BinaryHeap<Reverse<Event>>,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self {
            scheduled_events: BinaryHeap::new(),
        }
    }
}

impl Scheduler {
    /// Schedule `process` to be executed at `time`.
    pub fn schedule(&mut self, time: Duration, process: Process) {
        self.scheduled_events
            .push(Reverse(Event::new(time, process)));
    }
    /// Returns the number of events in the queue.
    pub fn len(&self) -> usize {
        self.scheduled_events.len()
    }
    /// Answers whether the event queue is empty.
    pub fn is_empty(&self) -> bool {
        self.scheduled_events.is_empty()
    }
    /// Returns, and removes from the queue, the next event to be processed.
    pub fn pop(&mut self) -> Option<Reverse<Event>> {
        self.scheduled_events.pop()
    }
}
