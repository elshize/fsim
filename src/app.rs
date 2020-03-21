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

#[cfg(test)]
mod test {
    use super::*;
    use fsim::Config;
    use fsim::QueryRoutingSimulation;
    use proptest::prelude::*;
    use std::fs::File;

    proptest! {
        #[test]
        fn test_random_selector(sequence in prop::collection::vec(prop::bool::ANY, 100)) {
            let sim = QueryRoutingSimulation::from_config(
                Config::from_yaml(File::open("tests/config.yml").unwrap()).unwrap(),
                serde_json::Deserializer::from_reader(File::open("tests/queries.jl").unwrap())
                    .into_iter()
                    .map(|elem| elem.expect("Failed to parse query"))
                    .collect(),
            );
            let mut app = App::new(sim);
            assert_eq!(app.sim.history().collect::<Vec<_>>().len(), 1);
            let mut history_size = 1;
            for forward in sequence {
                if forward {
                    app.next();
                    history_size += 1;
                } else {
                    app.prev();
                    history_size = std::cmp::max(history_size - 1, 1);
                }
                assert_eq!(app.sim.history().collect::<Vec<_>>().len(), history_size);
            }
        }
    }
}
