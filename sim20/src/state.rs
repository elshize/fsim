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
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Key<V> {
    id: usize,
    state_hash: u64,
    _marker: PhantomData<V>,
}

impl<T> Clone for Key<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            state_hash: self.state_hash,
            _marker: PhantomData,
        }
    }
}
impl<T> Copy for Key<T> {}

/// A type-safe identifier of a queue.
///
/// This is an analogue of [`Key<T>`](struct.Key.html) used specifically for queues.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct QueueId<V> {
    id: usize,
    state_hash: u64,
    _marker: PhantomData<V>,
}
impl<T> Clone for QueueId<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            state_hash: self.state_hash,
            _marker: PhantomData,
        }
    }
}
impl<T> Copy for QueueId<T> {}

/// State of a simulation holding all queues and arbitrary values in a store value.
pub struct State {
    store: HashMap<TypeId, HashMap<usize, Box<dyn Any>>>,
    queues: HashMap<TypeId, HashMap<usize, Box<dyn Any>>>,
    next_id: usize,
    pub(crate) state_hash: u64,
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
    fn assert_hash<V: 'static>(&self, key: &Key<V>) {
        assert_eq!(
            key.state_hash, self.state_hash,
            "State hash of the key does not match the hash of the state"
        );
    }
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
        self.assert_hash(&key);
        self.store
            .get_mut(&TypeId::of::<V>())
            .and_then(|m| m.remove(&key.id).map(|v| *v.downcast::<V>().unwrap()))
    }

    /// Gets a immutable reference to a value of a type `V` from the value store.
    /// Learn more in the documentation for [`Key`].
    #[must_use]
    pub fn get<V: 'static>(&mut self, key: Key<V>) -> Option<&V> {
        self.assert_hash(&key);
        self.store
            .get(&TypeId::of::<V>())
            .and_then(|m| m.get(&key.id).map(|v| v.downcast_ref::<V>().unwrap()))
    }

    /// Gets a mutable reference to a value of a type `V` from the value store.
    /// Learn more in the documentation for [`Key`].
    #[must_use]
    pub fn get_mut<V: 'static>(&mut self, key: Key<V>) -> Option<&mut V> {
        self.assert_hash(&key);
        self.store
            .get_mut(&TypeId::of::<V>())
            .and_then(|m| m.get_mut(&key.id).map(|v| v.downcast_mut::<V>().unwrap()))
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

    /// Sends `value` to the `queue`.
    ///
    /// # Errors
    /// It returns an error if the queue is full.
    pub fn send<V: 'static>(&mut self, queue: QueueId<V>, value: V) -> Result<(), ()> {
        assert_eq!(self.state_hash, queue.state_hash, "State hash mismatch.");
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
        assert_eq!(self.state_hash, queue.state_hash, "State hash mismatch.");
        self.queues
            .get_mut(&TypeId::of::<V>())
            .expect("If queue ID was issued for this type, it must exist")
            .get_mut(&queue.id)
            .expect("If this queue ID was issued, a corresponding queue must exist")
            .downcast_mut::<Queue<V>>()
            .unwrap()
            .pop_front()
    }

    /// Checks the number of elements in the queue.
    pub fn len<V: 'static>(&mut self, queue: QueueId<V>) -> usize {
        assert_eq!(self.state_hash, queue.state_hash, "State hash mismatch.");
        self.queues
            .get(&TypeId::of::<V>())
            .expect("If queue ID was issued for this type, it must exist")
            .get(&queue.id)
            .expect("If this queue ID was issued, a corresponding queue must exist")
            .downcast_ref::<Queue<V>>()
            .unwrap()
            .len()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_add_remove_key_values() {
        let mut state = State::default();

        let id = state.insert(1);
        assert_eq!(state.remove(id), Some(1));
        assert_eq!(state.remove(id), None);

        let id = state.insert("string_slice");
        assert_eq!(state.remove(id), Some("string_slice"));
        assert_eq!(state.remove(id), None);

        let id = state.insert(vec![String::from("S")]);
        assert_eq!(state.remove(id), Some(vec![String::from("S")]));
        assert_eq!(state.remove(id), None);
    }

    #[test]
    fn test_bounded_queue() {
        let mut state = State::default();
        let qid = state.new_bounded_queue::<&str>(2);
        assert_eq!(state.len(qid), 0);

        assert!(state.send(qid, "A").is_ok());
        assert!(state.send(qid, "B").is_ok());
        assert!(state.send(qid, "C").is_err());

        assert_eq!(state.recv(qid), Some("A"));
        assert_eq!(state.recv(qid), Some("B"));
        assert_eq!(state.recv(qid), None);
    }

    #[test]
    fn test_unbounded_queue() {
        let mut state = State::default();
        let qid = state.new_queue::<&str>();
        assert_eq!(state.len(qid), 0);

        assert!(state.send(qid, "A").is_ok());
        assert!(state.send(qid, "B").is_ok());
        assert!(state.send(qid, "C").is_ok());

        assert_eq!(state.recv(qid), Some("A"));
        assert_eq!(state.recv(qid), Some("B"));
        assert_eq!(state.recv(qid), Some("C"));
        assert_eq!(state.recv(qid), None);
    }
}
