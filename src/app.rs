use fsim::{Query, QueryRoutingSimulation, QueryStatus};
use std::cell::RefCell;
use std::rc::Rc;
use tui::layout::Rect;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Navigation,
    ActivePane,
}

/// Current view of the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Window {
    /// Main overall view with optional pane highlighted (navigation mode).
    Main(View, Mode),
    /// One of the views is active and maximized.
    Maximized(View),
}

/// List of queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueriesView {
    List(Option<usize>),
    Details(usize),
}

impl Default for QueriesView {
    fn default() -> Self {
        QueriesView::List(None)
    }
}

/// Type of view in the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum View {
    /// Active queries list.
    ActiveQueries(QueriesView),
    /// Finished queries list.
    FinishedQueries(QueriesView),
    /// Log view.
    Logs,
    /// Current simulation statistics.
    Stats,
}

pub enum VerticalDirection {
    Up,
    Down,
    PageUp,
    PageDown,
}

impl View {
    /// Returns the view to the left (or self if at edge).
    pub fn left(self) -> View {
        use View::*;
        match self {
            ActiveQueries(_) | FinishedQueries(_) => self,
            Logs => FinishedQueries(QueriesView::default()),
            Stats => ActiveQueries(QueriesView::default()),
        }
    }
    /// Returns the view to the right (or self if at edge).
    pub fn right(self) -> View {
        use View::*;
        match self {
            ActiveQueries(_) => Stats,
            FinishedQueries(_) => Logs,
            Logs | Stats => self,
        }
    }
    /// Returns the view down (or self if at edge).
    pub fn down(self) -> View {
        use View::*;
        match self {
            ActiveQueries(_) => FinishedQueries(QueriesView::default()),
            Stats => Logs,
            FinishedQueries(_) | Logs => self,
        }
    }
    /// Returns the view up (or self if at edge).
    pub fn up(self) -> View {
        use View::*;
        match self {
            ActiveQueries(_) | Stats => self,
            Logs => Stats,
            FinishedQueries(_) => ActiveQueries(QueriesView::default()),
        }
    }

    /// Returns the selected item.
    pub fn selected(self) -> Option<usize> {
        use View::*;
        match self {
            ActiveQueries(QueriesView::List(idx)) | FinishedQueries(QueriesView::List(idx)) => idx,
            Logs => {
                None // TODO
            }
            _ => None,
        }
    }

    /// Returns the current list length.
    pub fn list_length(self, snapshot: &Snapshot) -> usize {
        use View::*;
        match self {
            ActiveQueries(QueriesView::List(_)) => snapshot.active_queries.len(),
            FinishedQueries(QueriesView::List(_)) => snapshot.finished_queries.len(),
            Logs => {
                0 // TODO
            }
            _ => unreachable!(),
        }
    }

    /// Selects an item.
    pub fn select(self, idx: usize) -> View {
        use View::*;
        match self {
            ActiveQueries(QueriesView::List(_)) => ActiveQueries(QueriesView::List(Some(idx))),
            Logs => {
                // TODO
                self
            }
            FinishedQueries(QueriesView::List(_)) => FinishedQueries(QueriesView::List(Some(idx))),
            _ => self,
        }
    }

    /// Selects an item.
    pub fn frame_height(self, app: &App) -> u16 {
        app.frame_height(self)
    }

    /// Selects an item.
    pub fn move_selection<'a>(
        self,
        direction: VerticalDirection,
        app: Rc<RefCell<App<'a>>>,
    ) -> View {
        if let Some(selected) = self.selected() {
            if let Some(last) = self.list_length(&app.borrow().snapshot).checked_sub(1) {
                match direction {
                    VerticalDirection::Up => self.select(selected.checked_sub(1).unwrap_or(0)),
                    VerticalDirection::Down => self.select(std::cmp::min(last, selected + 1)),
                    VerticalDirection::PageUp => {
                        let shift = usize::from(self.frame_height(&app.borrow())) / 2;
                        self.select(selected.checked_sub(shift).unwrap_or(0))
                    }
                    VerticalDirection::PageDown => {
                        let shift = usize::from(self.frame_height(&app.borrow())) / 2;
                        self.select(std::cmp::min(last, selected + shift))
                    }
                }
            } else {
                self
            }
        } else {
            self
        }
    }

    /// Is view a list.
    pub fn is_list(self) -> bool {
        use View::*;
        match self {
            ActiveQueries(QueriesView::List(_)) | FinishedQueries(QueriesView::List(_)) => true,
            _ => false,
        }
    }

    /// Return back.
    pub fn back(self) -> View {
        use View::*;
        match self {
            ActiveQueries(QueriesView::List(_)) => ActiveQueries(QueriesView::List(None)),
            FinishedQueries(QueriesView::List(_)) => FinishedQueries(QueriesView::List(None)),
            ActiveQueries(QueriesView::Details(item)) => {
                ActiveQueries(QueriesView::List(Some(item)))
            }
            FinishedQueries(QueriesView::Details(item)) => {
                FinishedQueries(QueriesView::List(Some(item)))
            }
            _ => self,
        }
    }

    /// Enters details.
    pub fn details(self) -> View {
        use View::*;
        match self {
            ActiveQueries(QueriesView::List(Some(item))) => {
                ActiveQueries(QueriesView::Details(item))
            }
            FinishedQueries(QueriesView::List(Some(item))) => {
                FinishedQueries(QueriesView::Details(item))
            }
            _ => self,
        }
    }

    /// Returns `self` if it matches the type of passed view, or that view otherwise.
    pub fn match_or(self, view: View) -> View {
        use View::*;
        match (self, view) {
            (ActiveQueries(_), ActiveQueries(_))
            | (FinishedQueries(_), FinishedQueries(_))
            | (Logs, Logs)
            | (Stats, Stats) => self,
            _ => view,
        }
    }
}

pub struct Snapshot {
    pub active_queries: Vec<(Query, QueryStatus)>,
    pub finished_queries: Vec<(Query, QueryStatus)>,
    pub logs: Vec<String>,
}

impl Default for Snapshot {
    fn default() -> Self {
        Self {
            active_queries: Vec::new(),
            finished_queries: Vec::new(),
            logs: Vec::new(),
        }
    }
}

pub struct Frames {
    pub active: Option<Rect>,
    pub finished: Option<Rect>,
    pub status: Option<Rect>,
    pub logs: Option<Rect>,
}

impl Default for Frames {
    fn default() -> Self {
        Self {
            active: None,
            finished: None,
            status: None,
            logs: None,
        }
    }
}

impl Frames {
    pub fn clear(&mut self) {
        self.active = None;
        self.finished = None;
        self.status = None;
        self.logs = None;
    }

    pub fn set_frame(&mut self, view: View, frame: Rect) {
        use View::*;
        match view {
            ActiveQueries(_) => {
                self.active = Some(frame);
            }
            FinishedQueries(_) => {
                self.finished = Some(frame);
            }
            Stats => {
                self.status = Some(frame);
            }
            Logs => {
                self.logs = Some(frame);
            }
        }
    }
}

pub struct App<'a> {
    pub sim: QueryRoutingSimulation<'a>,
    pub snapshot: Snapshot,
    pub window: Window,
    pub frames: Frames,
}

impl<'a> App<'a> {
    pub fn new(sim: QueryRoutingSimulation<'a>) -> Self {
        Self {
            sim,
            snapshot: Snapshot::default(),
            window: Window::Main(View::Stats, Mode::Navigation),
            frames: Frames::default(),
        }
    }

    pub fn frame_height(&self, view: View) -> u16 {
        use View::*;
        match view {
            ActiveQueries(_) => self.frames.active.as_ref(),
            FinishedQueries(_) => self.frames.finished.as_ref(),
            Logs => self.frames.logs.as_ref(),
            Stats => self.frames.status.as_ref(),
        }
        .map(|f| f.height)
        .unwrap_or(0)
    }

    fn queries_snapshot<'q>(
        queries: impl Iterator<Item = &'q (Query, QueryStatus)>,
    ) -> Vec<(Query, QueryStatus)> {
        let queries: Vec<_> = queries.copied().collect();
        queries
    }

    fn update_snapshot(&mut self) {
        self.snapshot = Snapshot {
            active_queries: Self::queries_snapshot(self.sim.status().active()),
            finished_queries: Self::queries_snapshot(self.sim.status().finished()),
            logs: self.sim.status().logs().cloned().collect(),
        };
    }

    pub fn next(&mut self) {
        self.sim.step_forward();
        self.update_snapshot();
    }

    pub fn prev(&mut self) {
        if let Ok(_) = self.sim.step_back() {
            self.update_snapshot();
        }
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
