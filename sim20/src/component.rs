use std::marker::PhantomData;

use crate::{Scheduler, State};

/// Identifies a simulation component.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ComponentId<E> {
    id: usize,
    state_hash: u64,
    _marker: PhantomData<E>,
}
impl<T: Clone> Copy for ComponentId<T> {}

pub trait Component {
    type Event;

    fn process_event(
        &mut self,
        self_id: ComponentId<Self::Event>,
        event: &Self::Event,
        scheduler: &mut Scheduler,
        state: &mut State,
    );
}
