use fsim::{Query, QueryRoutingSimulation, QueryStatus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Component {
    Active {
        queries: Vec<(Query, QueryStatus)>,
        selected: usize,
    },
    ActiveDetails {
        queries: Vec<(Query, QueryStatus)>,
        selected: usize,
    },
    Logs,
}

pub(super) struct App<'a> {
    pub sim: QueryRoutingSimulation<'a>,
    pub focus: Option<Component>,
}

impl<'a> App<'a> {
    pub fn new(sim: QueryRoutingSimulation<'a>) -> Self {
        Self { sim, focus: None }
    }

    pub fn next(&mut self) {
        self.sim.step_forward();
    }

    pub fn prev(&mut self) {
        if let Err(_) = self.sim.step_back() {}
    }
}
