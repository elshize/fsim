use std::collections::VecDeque;

/// Abstraction over [`VecDeque`] that allows to limit the capacity of the queue.
/// This means that push operations can fail.
/// By default, the capacity is equal to [`usize::MAX`], which makes unlimited in practice.
///
/// [`VecDeque`]: https://doc.rust-lang.org/std/collections/struct.VecDeque.html
/// [`usize::MAX`]: https://doc.rust-lang.org/std/primitive.usize.html#associatedconstant.MAX
///
/// # Examples
///
/// ```
/// # use sim20::Queue;
///
/// let mut queue: Queue<i32> = Queue::default();
/// assert!(queue.push_back(1).is_ok()); // Always succeeds
///
/// let mut queue: Queue<i32> = Queue::bounded(2);
/// assert!(queue.push_back(1).is_ok());
/// assert!(queue.push_back(2).is_ok());
/// assert!(queue.push_back(3).is_err());
/// ```
pub struct Queue<T> {
    inner: VecDeque<T>,
    capacity: usize,
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self {
            inner: VecDeque::default(),
            capacity: usize::MAX,
        }
    }
}

impl<T> Queue<T> {
    /// Creates a queue with the given capacity.
    pub fn bounded(capacity: usize) -> Self {
        Self {
            inner: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// Appends an element to the back of the `Queue`.
    pub fn push_back(&mut self, value: T) -> Result<(), ()> {
        if self.inner.len() < self.capacity {
            self.inner.push_back(value);
            Ok(())
        } else {
            Err(())
        }
    }

    /// Removes the first element and returns it, or `None` if the `Queue` is empty.
    pub fn pop_front(&mut self) -> Option<T> {
        self.inner.pop_front()
    }

    /// Returns the number of elements in the `Queue`.
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}
