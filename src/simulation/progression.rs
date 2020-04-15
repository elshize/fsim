use super::{im_status, Status};
use std::cell::RefCell;

/// This is what drives the progression of the simulation.
pub trait Progression: Default {
    /// Type holding simulation status.
    type Status: Status + Clone;

    /// Returns the current simulation status.
    fn status(&self) -> &Self::Status;

    /// Returns the current simulation status as mutable reference.
    fn status_mut(&mut self) -> &mut Self::Status;

    /// Current step.
    fn step(&self) -> usize;

    /// Next step in the simulation.
    ///
    /// # Errors
    ///
    /// It returns an error if the current stauts it the most recently computed one.
    fn next(&mut self) -> Result<(), ()> {
        Err(())
    }

    /// Records a step in the simulation.
    fn record(&mut self, status: Self::Status);

    /// Returns a status object at the beginning of a step, which will be modified,
    /// and passed back at the end of the step by [`record`](#method.record).
    fn init_step(&self) -> RefCell<Self::Status>;
}

/// Simulation progression that can go back and forth in its history.
pub struct ReversibleProgression {
    past: Vec<im_status::Status>,
    future: Vec<im_status::Status>,
}

impl Default for ReversibleProgression {
    fn default() -> Self {
        Self {
            past: vec![im_status::Status::default()],
            future: vec![],
        }
    }
}

impl Progression for ReversibleProgression {
    type Status = im_status::Status;

    fn status(&self) -> &Self::Status {
        self.past.iter().last().unwrap()
    }

    fn status_mut(&mut self) -> &mut Self::Status {
        self.past.iter_mut().last().unwrap()
    }

    fn step(&self) -> usize {
        self.past.len()
    }

    fn next(&mut self) -> Result<(), ()> {
        if let Some(next) = self.future.pop() {
            self.past.push(next);
            Ok(())
        } else {
            Err(())
        }
    }

    fn record(&mut self, status: Self::Status) {
        self.past.push(status);
    }

    fn init_step(&self) -> RefCell<Self::Status> {
        RefCell::new(self.status().clone())
    }
}

impl ReversibleProgression {
    /// Go one step back in history.
    ///
    /// # Errors
    ///
    /// It returns an error if the current stauts it the initial one.
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
