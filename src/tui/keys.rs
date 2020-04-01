use std::collections::HashMap;
use strum_macros::EnumIter;
use termion::event::Key;

/// Actions available on any view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter)]
pub enum GlobalAction {
    /// Exit the application.
    Exit,
    /// Proceed to the next simulation step.
    NextStep,
    /// Go back one simulation step.
    PrevStep,
    /// Run simulation for the next second.
    NextSecond,
    /// Go back one second in the simulation.
    PrevSecond,
}

/// Actions in navigation mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter)]
pub enum NavigationAction {
    /// Navigate to the pane to the left.
    Left,
    /// Navigate to the pane to the right.
    Right,
    /// Navigate to the pane above.
    Up,
    /// Navigate to the pane below.
    Down,
    /// Activate the current pane.
    Enter,
}

/// Actions in active pane mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter)]
pub enum ActivePaneAction {
    /// Go back to the previous view.
    Back,
    /// Maximize or minimize the current pane.
    Maximize,
    /// Select an item below in the list.
    ItemDown,
    /// Select an item above in the list.
    ItemUp,
    /// Move half a page down in the list.
    PageDown,
    /// Move half a page up in the list.
    PageUp,
    /// Move to the top of the list.
    Home,
    /// Move to the bottom of the list.
    End,
    /// Display details of the current item.
    Details,
}

/// Bindings between keys and actions.
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

    /// Resolves a global action or returns back the passed key as an error.
    ///
    /// # Example
    ///
    /// ```
    /// # use fsim::tui::{KeyBindings, GlobalAction};
    /// use termion::event::Key;
    /// # fn main() -> Result<(), Key> {
    /// let keys = KeyBindings::default();
    /// let action = keys.global_action(Key::Ctrl('q'))?;
    /// assert_eq!(action, GlobalAction::Exit);
    /// assert_eq!(
    ///     keys.global_action(Key::Backspace).unwrap_err(),
    ///     Key::Backspace
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn global_action(&self, key: Key) -> Result<GlobalAction, Key> {
        self.global_bindings.get(&key).copied().ok_or(key)
    }

    /// Resolves a navigation action or returns back the passed key as an error.
    ///
    /// # Example
    ///
    /// ```
    /// # use fsim::tui::{KeyBindings, NavigationAction};
    /// use termion::event::Key;
    /// # fn main() -> Result<(), Key> {
    /// let keys = KeyBindings::default();
    /// let action = keys.navigation_action(Key::Up)?;
    /// assert_eq!(action, NavigationAction::Up);
    /// assert_eq!(
    ///     keys.navigation_action(Key::Esc).unwrap_err(),
    ///     Key::Esc
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn navigation_action(&self, key: Key) -> Result<NavigationAction, Key> {
        self.navigation_bindings.get(&key).copied().ok_or(key)
    }

    /// Resolves an active pane action or returns back the passed key as an error.
    ///
    /// # Example
    ///
    /// ```
    /// # use fsim::tui::{KeyBindings, ActivePaneAction};
    /// use termion::event::Key;
    /// # fn main() -> Result<(), Key> {
    /// let keys = KeyBindings::default();
    /// let action = keys.active_pane_action(Key::Esc)?;
    /// assert_eq!(action, ActivePaneAction::Back);
    /// assert_eq!(
    ///     keys.active_pane_action(Key::Backspace).unwrap_err(),
    ///     Key::Backspace
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn active_pane_action(&self, key: Key) -> Result<ActivePaneAction, Key> {
        self.active_pane_bindings.get(&key).copied().ok_or(key)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_default_keys() -> Result<(), Key> {
        let keys = KeyBindings::default();

        assert_eq!(keys.navigation_action(Key::Left)?, NavigationAction::Left);
        assert_eq!(keys.navigation_action(Key::Right)?, NavigationAction::Right);
        assert_eq!(keys.navigation_action(Key::Down)?, NavigationAction::Down);
        assert_eq!(keys.navigation_action(Key::Up)?, NavigationAction::Up);
        assert_eq!(
            keys.navigation_action(Key::Char('h'))?,
            NavigationAction::Left
        );
        assert_eq!(
            keys.navigation_action(Key::Char('l'))?,
            NavigationAction::Right
        );
        assert_eq!(
            keys.navigation_action(Key::Char('j'))?,
            NavigationAction::Down
        );
        assert_eq!(
            keys.navigation_action(Key::Char('k'))?,
            NavigationAction::Up
        );
        assert_eq!(
            keys.navigation_action(Key::Char('\n'))?,
            NavigationAction::Enter
        );

        assert_eq!(keys.active_pane_action(Key::Esc)?, ActivePaneAction::Back);
        assert_eq!(
            keys.active_pane_action(Key::Char('F'))?,
            ActivePaneAction::Maximize
        );
        assert_eq!(keys.active_pane_action(Key::Up)?, ActivePaneAction::ItemUp);
        assert_eq!(
            keys.active_pane_action(Key::Down)?,
            ActivePaneAction::ItemDown
        );
        assert_eq!(
            keys.active_pane_action(Key::Char('k'))?,
            ActivePaneAction::ItemUp
        );
        assert_eq!(
            keys.active_pane_action(Key::Char('j'))?,
            ActivePaneAction::ItemDown
        );
        assert_eq!(
            keys.active_pane_action(Key::PageUp)?,
            ActivePaneAction::PageUp
        );
        assert_eq!(
            keys.active_pane_action(Key::PageDown)?,
            ActivePaneAction::PageDown
        );
        assert_eq!(
            keys.active_pane_action(Key::Ctrl('u'))?,
            ActivePaneAction::PageUp
        );
        assert_eq!(
            keys.active_pane_action(Key::Ctrl('d'))?,
            ActivePaneAction::PageDown
        );
        assert_eq!(keys.active_pane_action(Key::Home)?, ActivePaneAction::Home);
        assert_eq!(keys.active_pane_action(Key::End)?, ActivePaneAction::End);
        assert_eq!(
            keys.active_pane_action(Key::Char('g'))?,
            ActivePaneAction::Home
        );
        assert_eq!(
            keys.active_pane_action(Key::Char('G'))?,
            ActivePaneAction::End
        );
        assert_eq!(
            keys.active_pane_action(Key::Char('\n'))?,
            ActivePaneAction::Details
        );

        assert_eq!(keys.global_action(Key::Ctrl('c'))?, GlobalAction::Exit);
        assert_eq!(keys.global_action(Key::Ctrl('q'))?, GlobalAction::Exit);
        assert_eq!(keys.global_action(Key::Char(','))?, GlobalAction::PrevStep);
        assert_eq!(keys.global_action(Key::Char('.'))?, GlobalAction::NextStep);
        assert_eq!(
            keys.global_action(Key::Char('<'))?,
            GlobalAction::PrevSecond
        );
        assert_eq!(
            keys.global_action(Key::Char('>'))?,
            GlobalAction::NextSecond
        );
        Ok(())
    }
}
