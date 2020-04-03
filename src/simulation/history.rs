use super::im_status;

/// Simulation history manages states throughout a simulation.
pub trait History: Default {
    /// Type holding simulation status.
    type Status;

    /// Returns the current simulation status.
    fn status(&self) -> &Self::Status;

    /// Current step.
    fn step(&self) -> usize;
}

/// History that retains all past states.
pub struct PersistentHistory(Vec<im_status::Status>, Vec<im_status::Status>);

impl History for PersistentHistory {
    type Status = im_status::Status;

    fn status(&self) -> &Self::Status {
        self.0.iter().last().unwrap()
    }

    fn step(&self) -> usize {
        self.0.len()
    }
}

impl PersistentHistory {
    pub fn next(&mut self) -> Option<im_status::Status> {
        self.1.pop()
    }
    pub fn record(&mut self, status: im_status::Status) {
        self.0.push(status)
    }
    pub fn step_back(&mut self) -> Result<(), ()> {
        if self.0.len() > 1 {
            self.1.push(self.0.pop().unwrap());
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn past(&self) -> Vec<&im_status::Status> {
        self.0.iter().rev().collect()
    }
}

impl Default for PersistentHistory {
    fn default() -> Self {
        Self(vec![im_status::Status::default()], vec![])
    }
}

//pub struct DumpingHistory;
//
//impl History for DumpingHistory {
//    type Status = im_status::Status;
//
//    fn status(&self) -> &Self::Status {
//        self.0.iter().last().unwrap()
//    }
//}
//
//impl Default for DumpingHistory {
//    fn default() -> Self {
//        Self
//    }
//}
