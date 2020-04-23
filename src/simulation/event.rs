use super::Process;
use std::cmp::Ordering;
use std::time::Duration;

/// An event is simply a process to run and a time to run it on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    /// Event's time.
    pub time: Duration,
    /// Process to execute at `time`.
    pub process: Process,
}

impl Event {
    /// Constructs a new event.
    pub fn new(time: Duration, process: Process) -> Self {
        Event { time, process }
    }
}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.time.partial_cmp(&other.time)
    }
}

impl Ord for Event {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time.cmp(&other.time)
    }
}
