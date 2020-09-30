use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::marker::PhantomData;

use rand::RngCore;

use crate::Queue;

/// A type-safe key used to fetch values from the value store.
///
/// # Construction
///
/// A key can be constructed only by calling [`State::insert`]. The state assigns a new numerical
/// ID to the inserted value.
/// Additionally, the key holds a unique hash for the state object.
/// This prevents from using the key with a different instance of [`State`] object.
/// Such operation will panic:
///
/// ```should_fail
/// # use sim20::{Key, State};
/// let mut state_1 = State::default();
/// let mut state_2 = State::default();
/// let id = state_1.insert(1);
/// let _ = state_2.remove(id);
/// ```
///
/// # Type Safety
///
/// These keys are type-safe in a sense that a key used to insert a value of type `T` cannot be
/// used to access a value of another type `U`. An attempt to do so will result in a compile error.
/// It is achieved by having the key generic over `T`. However, `T` is just a marker, and no
/// values of type `T` are stored internally.
///
/// ```compile_fail
/// # use sim20::{Key, State};
/// let mut state = State::default();
/// let id = state.insert(String::from("1"));
/// let _: Option<i32> = state.remove(id);  // Error!
/// let _ = state.remove::<i32>(id);        // Error!
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key<V> {
    id: usize,
    state_hash: u64,
    _marker: PhantomData<V>,
}

impl<T: Clone> Copy for Key<T> {}

/// A type-safe identifier of a queue.
///
/// This is an analogue of [`Key<T>`](struct.Key.html) used specifically for queues.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueueId<V> {
    id: usize,
    state_hash: u64,
    _marker: PhantomData<V>,
}
impl<T: Clone> Copy for QueueId<T> {}

/// State of a simulation holding all queues and arbitrary values in a store value.
pub struct State {
    store: HashMap<TypeId, HashMap<usize, Box<dyn Any>>>,
    queues: HashMap<TypeId, HashMap<usize, Box<dyn Any>>>,
    next_id: usize,
    state_hash: u64,
}

impl Default for State {
    fn default() -> Self {
        Self {
            store: HashMap::new(),
            queues: HashMap::new(),
            next_id: 0,
            state_hash: rand::thread_rng().next_u64(),
        }
    }
}

impl State {
    /// Inserts an arbitrary value to the value store. Learn more in the documentation for [`Key`].
    #[must_use = "Discarding key results in leaking inserted value"]
    pub fn insert<V: 'static>(&mut self, value: V) -> Key<V> {
        let id = self.next_id;
        self.next_id += 1;
        self.store
            .entry(TypeId::of::<V>())
            .or_default()
            .insert(id, Box::new(value));
        Key {
            id,
            state_hash: self.state_hash,
            _marker: PhantomData,
        }
    }

    /// Removes a value of type `V` from the value store. Learn more in the documentation for [`Key`].
    pub fn remove<V: 'static>(&mut self, key: Key<V>) -> Option<V> {
        assert_eq!(
            key.state_hash, self.state_hash,
            "State hash of the key does not match the hash of the state"
        );
        self.store
            .get_mut(&TypeId::of::<V>())
            .and_then(|m| m.remove(&key.id).map(|v| *v.downcast::<V>().unwrap()))
    }

    /// Creates a new unbounded queue, returning its ID.
    pub fn new_queue<V: 'static>(&mut self) -> QueueId<V> {
        let id = self.next_id;
        self.next_id += 1;
        self.queues
            .entry(TypeId::of::<V>())
            .or_default()
            .insert(id, Box::new(Queue::<V>::default()));
        QueueId {
            id,
            state_hash: self.state_hash,
            _marker: PhantomData,
        }
    }

    /// Creates a new bounded queue, returning its ID.
    pub fn new_bounded_queue<V: 'static>(&mut self, capacity: usize) -> QueueId<V> {
        let id = self.next_id;
        self.next_id += 1;
        self.queues
            .entry(TypeId::of::<V>())
            .or_default()
            .insert(id, Box::new(Queue::<V>::bounded(capacity)));
        QueueId {
            id,
            state_hash: self.state_hash,
            _marker: PhantomData,
        }
    }

    /// Sends `value` to the `queue`. It returns an error if the queue is full.
    pub fn send<V: 'static>(&mut self, queue: QueueId<V>, value: V) -> Result<(), ()> {
        self.queues
            .get_mut(&TypeId::of::<V>())
            .expect("If queue ID was issued for this type, it must exist")
            .get_mut(&queue.id)
            .expect("Invalid queue ID")
            .downcast_mut::<Queue<V>>()
            .unwrap()
            .push_back(value)
    }

    /// Pops the first value from the `queue`. It returns `None` if  the queue is empty.
    pub fn recv<V: 'static>(&mut self, queue: QueueId<V>) -> Option<V> {
        self.queues
            .get_mut(&TypeId::of::<V>())
            .expect("If queue ID was issued for this type, it must exist")
            .get_mut(&queue.id)
            .expect("If this queue ID was issued, a corresponding queue must exist")
            .downcast_mut::<Queue<V>>()
            .unwrap()
            .pop_front()
    }
}
