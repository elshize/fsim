use super::{FlushStatus, ImStatus, RunResult, Status};
use std::io::Write;

/// This is what drives the progression of the simulation.
pub trait Progression {
    /// Type holding simulation status.
    type Status: Status;

    /// Returns the current simulation status.
    fn status(&self) -> &Self::Status;

    /// Returns the current simulation status as mutable reference.
    fn status_mut(&mut self) -> &mut Self::Status;

    /// Current step.
    fn step(&self) -> usize;

    /// Next step in the simulation. Returns `Incomplete` if no future statuses are available and
    /// one needs to be generated.
    fn next(&mut self) -> RunResult {
        RunResult::Incomplete
    }
}

/// Simulation progression that can go back and forth in its history.
pub struct ReversibleProgression {
    past: Vec<ImStatus>,
    future: Vec<ImStatus>,
}

impl<'a> Default for ReversibleProgression {
    fn default() -> Self {
        Self {
            past: vec![ImStatus::default()],
            future: vec![],
        }
    }
}

impl Progression for ReversibleProgression {
    type Status = ImStatus;

    fn status(&self) -> &Self::Status {
        self.past.iter().last().unwrap()
    }

    fn status_mut(&mut self) -> &mut Self::Status {
        assert!(self.future.is_empty()); // TODO
        let new_status = self.status().clone();
        self.past.push(new_status);
        self.past.iter_mut().last().unwrap()
    }

    fn step(&self) -> usize {
        self.past.len()
    }

    fn next(&mut self) -> RunResult {
        if let Some(next) = self.future.pop() {
            self.past.push(next);
            RunResult::Complete
        } else {
            RunResult::Incomplete
        }
    }
}

impl ReversibleProgression {
    /// Go one step back in history.
    ///
    /// # Errors
    ///
    /// It returns an error if the current status it the initial one.
    pub fn prev(&mut self) -> Result<(), ()> {
        if self.past.len() > 1 {
            self.future.push(self.past.pop().unwrap());
            Ok(())
        } else {
            Err(())
        }
    }

    /// Gets all past statuses.
    #[must_use]
    pub fn past(&self) -> Vec<&<ReversibleProgression as Progression>::Status> {
        self.past.iter().rev().collect()
    }
}

/// Simulation progression that continuously prints out data to a writer.
pub struct FlushingProgression<W: Write, L: Write> {
    status: FlushStatus<W, L>,
    step: usize,
}

impl<W: Write, L: Write> FlushingProgression<W, L> {
    /// Constructs a new progression with the given sinks.
    pub fn new(data_sink: W, log_sink: L) -> Self {
        Self {
            status: FlushStatus::new(data_sink, log_sink),
            step: 0,
        }
    }
}

impl<W: Write, L: Write> Progression for FlushingProgression<W, L> {
    type Status = FlushStatus<W, L>;

    fn status(&self) -> &Self::Status {
        &self.status
    }

    fn status_mut(&mut self) -> &mut Self::Status {
        &mut self.status
    }

    fn step(&self) -> usize {
        self.step
    }
}
