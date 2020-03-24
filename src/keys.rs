use super::{app::VerticalDirection, App, Mode, View, Window};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use strum_macros::EnumIter;
use termion::event::Key;

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter)]
pub enum GlobalAction {
    Exit,
    NextStep,
    PrevStep,
    NextSecond,
    PrevSecond,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter)]
pub enum NavigationAction {
    Left,
    Right,
    Up,
    Down,
    Enter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter)]
pub enum ActivePaneAction {
    Back,
    Maximize,
    ItemDown,
    ItemUp,
    PageDown,
    PageUp,
    Home,
    End,
    Details,
}

pub struct KeyBindings {
    global_bindings: HashMap<Key, GlobalAction>,
    navigation_bindings: HashMap<Key, NavigationAction>,
    active_pane_bindings: HashMap<Key, ActivePaneAction>,
}

impl Default for KeyBindings {
    fn default() -> Self {
        use NavigationAction::*;
        let navigation_bindings: HashMap<Key, NavigationAction> = [
            (Key::Left, Left),
            (Key::Right, Right),
            (Key::Down, Down),
            (Key::Up, Up),
            (Key::Char('\n'), Enter),
            (Key::Char('h'), Left),
            (Key::Char('l'), Right),
            (Key::Char('j'), Down),
            (Key::Char('k'), Up),
        ]
        .iter()
        .copied()
        .collect();
        let active_pane_bindings: HashMap<Key, ActivePaneAction> = [
            (Key::Esc, ActivePaneAction::Back),
            (Key::Char('F'), ActivePaneAction::Maximize),
            (Key::Up, ActivePaneAction::ItemUp),
            (Key::Down, ActivePaneAction::ItemDown),
            (Key::PageUp, ActivePaneAction::PageUp),
            (Key::PageDown, ActivePaneAction::PageDown),
            (Key::Home, ActivePaneAction::Home),
            (Key::End, ActivePaneAction::End),
            (Key::Char('k'), ActivePaneAction::ItemUp),
            (Key::Char('j'), ActivePaneAction::ItemDown),
            (Key::Ctrl('u'), ActivePaneAction::PageUp),
            (Key::Ctrl('d'), ActivePaneAction::PageDown),
            (Key::Char('\n'), ActivePaneAction::Details),
            (Key::Char('g'), ActivePaneAction::Home),
            (Key::Char('G'), ActivePaneAction::End),
        ]
        .iter()
        .copied()
        .collect();
        let global_bindings: HashMap<Key, GlobalAction> = [
            (Key::Ctrl('c'), GlobalAction::Exit),
            (Key::Ctrl('q'), GlobalAction::Exit),
            (Key::Char(','), GlobalAction::PrevStep),
            (Key::Char('.'), GlobalAction::NextStep),
            (Key::Char('<'), GlobalAction::PrevSecond),
            (Key::Char('>'), GlobalAction::NextSecond),
        ]
        .iter()
        .copied()
        .collect();
        Self {
            navigation_bindings,
            active_pane_bindings,
            global_bindings,
        }
    }
}

impl KeyBindings {
    /// Iterates over navigation mode bindings for a given action.
    pub fn global_bindings<'a>(&'a self, action: GlobalAction) -> impl Iterator<Item = Key> + 'a {
        self.global_bindings
            .iter()
            .filter_map(move |(k, a)| if *a == action { Some(k) } else { None })
            .copied()
    }

    /// Iterates over navigation mode bindings for a given action.
    pub fn navigation_bindings<'a>(
        &'a self,
        action: NavigationAction,
    ) -> impl Iterator<Item = Key> + 'a {
        self.navigation_bindings
            .iter()
            .filter_map(move |(k, a)| if *a == action { Some(k) } else { None })
            .copied()
    }

    /// Iterates over active pane mode bindings for a given action.
    pub fn active_pane_bindings<'a>(
        &'a self,
        action: ActivePaneAction,
    ) -> impl Iterator<Item = Key> + 'a {
        self.active_pane_bindings
            .iter()
            .filter_map(move |(k, a)| if *a == action { Some(k) } else { None })
            .copied()
    }
}

pub struct KeyHandler<'a> {
    app: Rc<RefCell<App<'a>>>,
    bindings: KeyBindings,
}

impl<'a> KeyHandler<'a> {
    pub fn new(app: Rc<RefCell<App<'a>>>) -> Self {
        Self {
            app,
            bindings: KeyBindings::default(),
        }
    }

    pub fn global_action(&self, key: Key) -> Result<GlobalAction, Key> {
        self.bindings.global_bindings.get(&key).copied().ok_or(key)
    }

    /// Handle keys in navigation mode (selecting pane).
    pub fn handle_navigation(&self, view: View, key: Key) -> Window {
        use NavigationAction::*;
        match self.bindings.navigation_bindings.get(&key) {
            Some(Left) => Window::Main(view.left(), Mode::Navigation),
            Some(Up) => Window::Main(view.up(), Mode::Navigation),
            Some(Right) => Window::Main(view.right(), Mode::Navigation),
            Some(Down) => Window::Main(view.down(), Mode::Navigation),
            Some(Enter) => {
                Window::Main(view.activate(&self.app.borrow().snapshot), Mode::ActivePane)
            }
            None => Window::Main(view, Mode::Navigation),
        }
    }

    /// Handle keys in active pane mode.
    pub fn handle_active_pane(&self, view: View, key: Key) -> Window {
        use ActivePaneAction::*;
        match self.bindings.active_pane_bindings.get(&key) {
            Some(Back) => Window::Main(
                view.back(),
                if view.is_list() {
                    Mode::Navigation
                } else {
                    Mode::ActivePane
                },
            ),
            Some(Maximize) => Window::Maximized(view),
            Some(ItemDown) => Window::Main(
                view.move_selection(VerticalDirection::Down, Rc::clone(&self.app)),
                Mode::ActivePane,
            ),
            Some(ItemUp) => Window::Main(
                view.move_selection(VerticalDirection::Up, Rc::clone(&self.app)),
                Mode::ActivePane,
            ),
            Some(PageDown) => Window::Main(
                view.move_selection(VerticalDirection::PageDown, Rc::clone(&self.app)),
                Mode::ActivePane,
            ),
            Some(PageUp) => Window::Main(
                view.move_selection(VerticalDirection::PageUp, Rc::clone(&self.app)),
                Mode::ActivePane,
            ),
            Some(Home) => {
                let len = view.list_length(&self.app.borrow().snapshot);
                if len == 0 {
                    Window::Main(view, Mode::ActivePane)
                } else {
                    Window::Main(view.select(0), Mode::ActivePane)
                }
            }
            Some(End) => {
                let len = view.list_length(&self.app.borrow().snapshot);
                if len == 0 {
                    Window::Main(view, Mode::ActivePane)
                } else {
                    Window::Main(view.select(len - 1), Mode::ActivePane)
                }
            }
            Some(Details) => Window::Main(view.details(), Mode::ActivePane),
            None => Window::Main(view, Mode::ActivePane),
        }
    }

    /// Handle keys in active pane mode.
    pub fn handle_maximized(&self, view: View, key: Key) -> Window {
        use ActivePaneAction::*;
        match self.bindings.active_pane_bindings.get(&key) {
            Some(Back) => {
                if view.is_list() {
                    Window::Main(view, Mode::Navigation)
                } else {
                    Window::Maximized(view.back())
                }
            }
            Some(Maximize) => Window::Main(view, Mode::ActivePane),
            Some(ItemDown) => Window::Maximized(
                view.move_selection(VerticalDirection::Down, Rc::clone(&self.app)),
            ),
            Some(ItemUp) => {
                Window::Maximized(view.move_selection(VerticalDirection::Up, Rc::clone(&self.app)))
            }
            Some(PageDown) => Window::Maximized(
                view.move_selection(VerticalDirection::PageDown, Rc::clone(&self.app)),
            ),
            Some(PageUp) => Window::Maximized(
                view.move_selection(VerticalDirection::PageUp, Rc::clone(&self.app)),
            ),
            Some(Home) => {
                let len = view.list_length(&self.app.borrow().snapshot);
                if len == 0 {
                    Window::Maximized(view)
                } else {
                    Window::Maximized(view.select(0))
                }
            }
            Some(End) => {
                let len = view.list_length(&self.app.borrow().snapshot);
                if len == 0 {
                    Window::Maximized(view)
                } else {
                    Window::Maximized(view.select(len - 1))
                }
            }
            Some(Details) => Window::Maximized(view.details()),
            None => Window::Maximized(view),
        }
    }
}
