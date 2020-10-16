use std::marker::PhantomData;

use crate::{Scheduler, State};

/// Identifies a simulation component.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ComponentId<E> {
    pub(crate) id: usize,
    pub(crate) state_hash: u64,
    pub(crate) _marker: PhantomData<E>,
}
impl<T> Clone for ComponentId<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            state_hash: self.state_hash,
            _marker: PhantomData,
        }
    }
}
impl<T> Copy for ComponentId<T> {}

/// Interface of a simulation component.
pub trait Component {
    /// Type of event the component reacts to.
    type Event;

    /// Reacts to `event`. A component has access to the following elements of the simulation:
    /// - `self_id`: This is the ID of this component. This is used to schedule events to itself.
    ///              This is passed for convenience, as the ID is only known after the component
    ///              has been already constructed and passed to the simulation.
    /// - `event`: The occurring event.
    /// - `scheduler`: The scheduler used to access time and schedule new events.
    /// - `state`: The state is used to access queues and values in the value store.
    fn process_event(
        &mut self,
        self_id: ComponentId<Self::Event>,
        event: &Self::Event,
        scheduler: &mut Scheduler,
        state: &mut State,
    );
}

/// Container holding type-erased components.
pub struct Components {
    components: Vec<Box<dyn ::std::any::Any>>,
    state_hash: u64,
}

impl Components {
    /// Constructs a new list of components.
    #[must_use]
    pub fn new(state: &State) -> Self {
        Self {
            components: Vec::new(),
            state_hash: state.state_hash,
        }
    }
    /// Returns a mutable reference for the given component.
    pub fn get_mut<E: 'static>(&mut self, id: ComponentId<E>) -> &mut dyn Component<Event = E> {
        assert_eq!(
            self.state_hash, id.state_hash,
            "Component ID hash does not match the simulation state hash."
        );
        self.components[id.id]
            .downcast_mut::<Box<dyn Component<Event = E>>>()
            .unwrap()
            .as_mut()
    }
    /// Registers a new component and returns its ID.
    #[must_use]
    pub fn add_component<E: 'static, C: Component<Event = E> + 'static>(
        &mut self,
        component: C,
    ) -> ComponentId<E> {
        let components = &mut self.components;
        let id = components.len();
        let component: Box<dyn Component<Event = E>> = Box::new(component);
        components.push(Box::new(component));
        ComponentId {
            id,
            state_hash: self.state_hash,
            _marker: ::std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    struct TestComponent(Rc<RefCell<String>>);

    impl Component for TestComponent {
        type Event = String;

        fn process_event(
            &mut self,
            _self_id: ComponentId<Self::Event>,
            event: &Self::Event,
            _scheduler: &mut Scheduler,
            _state: &mut State,
        ) {
            *self.0.borrow_mut() = event.clone();
        }
    }

    struct RcTestComponent(String);

    impl Component for Rc<RefCell<RcTestComponent>> {
        type Event = String;

        fn process_event(
            &mut self,
            _self_id: ComponentId<Self::Event>,
            event: &Self::Event,
            _scheduler: &mut Scheduler,
            _state: &mut State,
        ) {
            self.borrow_mut().0 = event.clone();
        }
    }

    #[test]
    fn test_add_and_get_component() {
        let mut scheduler = Scheduler::default();

        let mut state = State::default();
        let expected_hash = state.state_hash;

        let mut components = Components::new(&state);
        assert_eq!(components.components.len(), 0);
        assert_eq!(components.state_hash, expected_hash);

        let text = Rc::new(RefCell::new(String::from("")));

        let comp: ComponentId<String> = components.add_component(TestComponent(Rc::clone(&text)));
        assert_eq!(components.components.len(), 1);
        assert_eq!(comp.state_hash, expected_hash);

        let comp_mut = components.get_mut(comp);
        comp_mut.process_event(comp, &String::from("Modified"), &mut scheduler, &mut state);

        assert_eq!(*text.borrow(), "Modified");
    }

    #[test]
    fn test_rc_ref_cell() {
        let mut scheduler = Scheduler::default();
        let mut state = State::default();

        let component = Rc::new(RefCell::new(RcTestComponent(String::from(""))));
        let mut components = Components::new(&state);
        let comp: ComponentId<String> = components.add_component(Rc::clone(&component));

        let comp_mut = components.get_mut(comp);
        comp_mut.process_event(comp, &String::from("Modified"), &mut scheduler, &mut state);

        assert_eq!(component.borrow().0, "Modified");
    }

    #[test]
    #[should_panic(expected = "Component ID hash does not match the simulation state hash.")]
    fn test_fail_when_other_state() {
        let state_1 = State::default();
        let mut components_1 = Components::new(&state_1);
        let _ = components_1.add_component(TestComponent(Rc::new(RefCell::new(String::new()))));

        let state_2 = State::default();
        let mut components_2 = Components::new(&state_2);
        let comp_2 =
            components_2.add_component(TestComponent(Rc::new(RefCell::new(String::new()))));

        components_1.get_mut(comp_2);
    }
}
