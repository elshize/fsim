use super::{im_status, Status};
use std::cell::RefCell;
use std::rc::Rc;

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
    fn next(&mut self) -> Result<(), ()> {
        Err(())
    }

    /// Records a step in the simulation.
    fn record(&mut self, status: Self::Status);

    fn init_step(&self) -> RefCell<Self::Status>;
}

pub struct ReversibleProgression {
    past: Vec<im_status::Status>,
    future: Vec<im_status::Status>,
    ///// Previous step in the simulation.
    //fn prev(&mut self) -> Result<(), ()>;
    ///// All past statuses.
    //fn past(&self) -> Vec<&Self::Status>;
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

    //fn next(&mut self, simulation: &mut Simulation<Self>) {
    //    if let Some(next) = self.future.pop() {
    //        self.past.push(next);
    //        return;
    //    }
    //    if let Some(Reverse(Event { time, process })) = simulation.next_event() {
    //        assert!(
    //            self.status().time() <= time,
    //            "Current event scheduled at time {:?}, which is earlier than current time {:?}: {:?}",
    //            time,
    //            self.status().time(),
    //            Event { time, process },
    //        );

    //        simulation.log_event(time, &process);
    //        let (effect, status) = simulation.run_process(time, process, self.status().clone());
    //        let status = simulation.handle_effect(time, effect, status);
    //        self.past.push(
    //            status.log_events(
    //                logger::clear()
    //                    .expect("Unable to retrieve logs")
    //                    .into_iter(),
    //            ),
    //        );
    //    }
    //}
}

impl ReversibleProgression {
    pub fn prev(&mut self) -> Result<(), ()> {
        if self.past.len() > 1 {
            self.future.push(self.past.pop().unwrap());
            Ok(())
        } else {
            Err(())
        }
    }
    pub fn past(&self) -> Vec<&<ReversibleProgression as Progression>::Status> {
        self.past.iter().rev().collect()
    }
}
