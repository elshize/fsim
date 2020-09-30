use std::cmp::Ordering;
use std::time::Duration;

/// An event is simply a process to run and a time to run it on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event<P> {
    /// Event's time.
    pub time: Duration,
    /// Process to execute at `time`.
    pub process: P,
}

impl<P> Event<P> {
    /// Constructs a new event.
    #[must_use]
    pub fn new(time: Duration, process: P) -> Self {
        Event { time, process }
    }
}

impl<P: PartialEq + Eq> PartialOrd for Event<P> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.time.partial_cmp(&other.time)
    }
}

impl<P: PartialEq + Eq> Ord for Event<P> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time.cmp(&other.time)
    }
}

trait EventHandler<P> {
    fn handle_log(event: &Event<P>);
}
