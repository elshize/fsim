//! An implementation of a queue that holds values of a certain type,
//! as well as a list of processes (or callbacks to processes) waiting to push to
//! or pop from the queue.

use std::collections::VecDeque;

/// The callback function that produces a process given an element from a queue.
pub struct ProcessCallback<'a, T, P>(Box<dyn Fn(T) -> P + 'a>);

impl<'a, T, P> ProcessCallback<'a, T, P> {
    pub fn new<F>(cb: F) -> Self
    where
        F: Fn(T) -> P + 'a,
    {
        Self(Box::new(cb))
    }

    pub fn process(self, val: T) -> P {
        self.0(val)
    }
}

/// Container of 0, 1, or 2 processes to resume.
#[derive(Debug, PartialEq, Eq)]
pub enum Resumed<P> {
    None,
    One(P),
    Two(P, P),
}

impl<P> Resumed<P> {
    fn new(p: P, q: Option<P>) -> Self {
        if let Some(q) = q {
            Resumed::Two(p, q)
        } else {
            Resumed::One(p)
        }
    }
}

impl<P> From<P> for Resumed<P> {
    fn from(p: P) -> Self {
        Resumed::One(p)
    }
}

impl<P> From<Option<P>> for Resumed<P> {
    fn from(p: Option<P>) -> Self {
        if let Some(p) = p {
            Resumed::One(p)
        } else {
            Resumed::None
        }
    }
}

impl<P> Default for Resumed<P> {
    fn default() -> Self {
        Resumed::None
    }
}

pub struct ResumedIter<P>(Resumed<P>);

impl<P> Iterator for ResumedIter<P> {
    type Item = P;
    fn next(&mut self) -> Option<P> {
        match std::mem::take(&mut self.0) {
            Resumed::None => None,
            Resumed::One(p) => {
                self.0 = Resumed::None;
                Some(p)
            }
            Resumed::Two(p, q) => {
                self.0 = Resumed::One(q);
                Some(p)
            }
        }
    }
}

impl<P> IntoIterator for Resumed<P> {
    type Item = P;
    type IntoIter = ResumedIter<P>;
    fn into_iter(self) -> Self::IntoIter {
        ResumedIter(self)
    }
}

/// Result of `push` on a queue.
#[derive(Debug, PartialEq, Eq)]
pub enum PushResult<P> {
    /// The pushed element is waiting to enter the queue because it is full.
    Full,
    /// The element was pushed.
    Pushed {
        /// Processes to resume.
        resumed: Resumed<P>,
    },
}

/// Result of `pop` on a queue.
#[derive(Debug, PartialEq, Eq)]
pub enum PopResult<P> {
    /// No elements to pop, so the callback needs to wait to pop.
    Empty,
    /// The element was pushed.
    Popped {
        /// Process return by the callback on the popped element.
        process: P,
        /// Optionally resumed process.
        resumed: Option<P>,
    },
}

/// A queue of elements of type `T`.
pub struct Queue<'a, T, P> {
    elements: VecDeque<T>,
    waiting_to_push: VecDeque<(T, P)>,
    waiting_to_pop: VecDeque<ProcessCallback<'a, T, P>>,
    capacity: usize,
}

impl<'a, T: std::fmt::Debug, P> Default for Queue<'a, T, P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, T: std::fmt::Debug, P> Queue<'a, T, P> {
    /// Constructs a new unbounded queue.
    pub fn new() -> Self {
        Self {
            elements: VecDeque::new(),
            waiting_to_push: VecDeque::new(),
            waiting_to_pop: VecDeque::new(),
            capacity: usize::max_value(),
        }
    }

    /// Constructs a new queue of capacity `capacity`.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            elements: VecDeque::new(),
            waiting_to_push: VecDeque::new(),
            waiting_to_pop: VecDeque::new(),
            capacity,
        }
    }

    /// Tries to push `element` to the queue. If succeeds and there is at least one
    /// process waiting to `pop`, it returns one of these processes. If the queue
    /// is currently full, the given process and the element are recorder and put
    /// in line to be awaken up by a future `pop`.
    #[must_use]
    pub fn push(&mut self, element: T, process: P) -> PushResult<P> {
        if self.elements.len() < self.capacity {
            self.elements.push_back(element);
            PushResult::Pushed {
                resumed: Resumed::new(
                    process,
                    self.waiting_to_pop
                        .pop_front()
                        .map(|callback| callback.process(self.elements.pop_front().unwrap())),
                ),
            }
        } else {
            self.waiting_to_push.push_back((element, process));
            PushResult::Full
        }
    }

    /// Tries to push `element` to the queue. If succeeds and there is at least one
    /// process waiting to `pop`, it returns one of these processes. If the queue
    /// is currently full, the given element is dropped.
    #[must_use]
    pub fn push_or_drop(&mut self, element: T) -> PushResult<P> {
        if self.elements.len() < self.capacity {
            self.elements.push_back(element);
            PushResult::Pushed {
                resumed: Resumed::from(
                    self.waiting_to_pop
                        .pop_front()
                        .map(|callback| callback.process(self.elements.pop_front().unwrap())),
                ),
            }
        } else {
            log::warn!("Dropping element: {:?}", element);
            PushResult::Full
        }
    }

    /// Tries to pop an element off the queue. If succeeds, the element is returned,
    /// possibly along with a process that is currently waiting to `put`.
    /// If the queue is currently empty, then the process is put in line to be awaken
    /// by a future `push`.
    #[must_use]
    pub fn pop(&mut self, callback: ProcessCallback<'a, T, P>) -> PopResult<P> {
        if let Some(element) = self.elements.pop_front() {
            PopResult::Popped {
                process: callback.process(element),
                resumed: self.waiting_to_push.pop_front().map(|(e, p)| {
                    self.elements.push_back(e);
                    p
                }),
            }
        } else {
            self.waiting_to_pop.push_back(callback);
            PopResult::Empty
        }
    }

    /// Checks if there are no elements in the queue.
    pub fn is_empty(&self) -> bool {
        self.elements.len() == 0
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use PushResult::*;

    #[derive(Debug, PartialEq, Eq)]
    enum MockProcess {
        Empty(u32),
        WithValue(u32, char),
    }

    type MockQueue<'a> = Queue<'a, char, MockProcess>;

    #[test]
    fn test_capacity() {
        assert_eq!(MockQueue::new().capacity, usize::max_value());
        assert_eq!(MockQueue::with_capacity(43).capacity, 43);
    }

    #[test]
    fn test_push_on_empty() {
        let mut queue = MockQueue::new();
        assert_eq!(
            queue.push('a', MockProcess::Empty(1)),
            Pushed {
                resumed: Resumed::from(MockProcess::Empty(1))
            }
        );
        assert_eq!(queue.elements, VecDeque::from(vec!['a']));
        assert!(queue.waiting_to_pop.is_empty());
        assert!(queue.waiting_to_push.is_empty());
    }

    #[test]
    fn test_push_on_non_empty() {
        let mut queue = MockQueue {
            elements: VecDeque::from(vec!['a', 'b', 'c']),
            ..Default::default()
        };
        assert_eq!(
            queue.push('z', MockProcess::Empty(1)),
            Pushed {
                resumed: Resumed::from(MockProcess::Empty(1))
            }
        );
        assert_eq!(queue.elements, VecDeque::from(vec!['a', 'b', 'c', 'z']));
        assert!(queue.waiting_to_pop.is_empty());
        assert!(queue.waiting_to_push.is_empty());
    }

    #[test]
    fn test_push_on_full() {
        let mut queue = MockQueue {
            elements: VecDeque::from(vec!['a', 'b', 'c']),
            capacity: 3,
            ..Default::default()
        };
        assert_eq!(queue.push('z', MockProcess::Empty(1)), Full);
        assert_eq!(queue.elements, VecDeque::from(vec!['a', 'b', 'c']));
        assert!(queue.waiting_to_pop.is_empty());
        assert_eq!(queue.waiting_to_push, vec![('z', MockProcess::Empty(1))]);
    }

    #[test]
    fn test_push_and_wake() {
        let mut queue = MockQueue {
            elements: VecDeque::from(vec!['a', 'b']),
            waiting_to_pop: VecDeque::from(vec![ProcessCallback::new(|e: char| {
                MockProcess::WithValue(123, e)
            })]),
            capacity: 3,
            ..Default::default()
        };
        assert_eq!(
            queue.push('c', MockProcess::Empty(1)),
            Pushed {
                resumed: Resumed::new(
                    MockProcess::Empty(1),
                    Some(MockProcess::WithValue(123, 'a'))
                )
            }
        );
        assert_eq!(queue.elements, VecDeque::from(vec!['b', 'c']));
        assert!(queue.waiting_to_pop.is_empty());
        assert!(queue.waiting_to_push.is_empty());
    }

    #[test]
    fn test_push_or_drop_on_empty() {
        let mut queue = MockQueue::new();
        assert_eq!(
            queue.push_or_drop('a'),
            Pushed {
                resumed: Resumed::None
            }
        );
        assert_eq!(queue.elements, VecDeque::from(vec!['a']));
        assert!(queue.waiting_to_pop.is_empty());
    }

    #[test]
    fn test_push_or_drop_on_non_empty() {
        let mut queue = MockQueue {
            elements: VecDeque::from(vec!['a', 'b', 'c']),
            ..Default::default()
        };
        assert_eq!(
            queue.push_or_drop('z'),
            Pushed {
                resumed: Resumed::None
            }
        );
        assert_eq!(queue.elements, VecDeque::from(vec!['a', 'b', 'c', 'z']));
        assert!(queue.waiting_to_pop.is_empty());
    }

    #[test]
    fn test_push_or_drop_on_full() {
        let mut queue = MockQueue {
            elements: VecDeque::from(vec!['a', 'b', 'c']),
            capacity: 3,
            ..Default::default()
        };
        assert_eq!(queue.push_or_drop('z'), Full);
        assert_eq!(queue.elements, VecDeque::from(vec!['a', 'b', 'c']));
        assert!(queue.waiting_to_pop.is_empty());
    }

    #[test]
    fn test_push_or_drop_and_wake() {
        let mut queue = MockQueue {
            elements: VecDeque::from(vec!['a', 'b']),
            waiting_to_pop: VecDeque::from(vec![ProcessCallback::new(|e: char| {
                MockProcess::WithValue(123, e)
            })]),
            capacity: 3,
            ..Default::default()
        };
        assert_eq!(
            queue.push_or_drop('c'),
            Pushed {
                resumed: Resumed::from(MockProcess::WithValue(123, 'a'))
            }
        );
        assert_eq!(queue.elements, VecDeque::from(vec!['b', 'c']));
        assert!(queue.waiting_to_pop.is_empty());
    }

    #[test]
    fn test_pop_on_non_empty() {
        let mut queue = MockQueue {
            elements: VecDeque::from(vec!['a', 'b', 'c']),
            ..Default::default()
        };
        let c = ProcessCallback::new(|c| MockProcess::WithValue(321, c));
        assert_eq!(
            queue.pop(c),
            PopResult::Popped {
                process: MockProcess::WithValue(321, 'a'),
                resumed: None
            }
        );
        assert_eq!(queue.elements, VecDeque::from(vec!['b', 'c']));
        assert!(queue.waiting_to_pop.is_empty());
        assert!(queue.waiting_to_push.is_empty());
    }

    #[test]
    fn test_pop_on_empty() {
        let mut queue = MockQueue::default();
        assert_eq!(
            queue.pop(ProcessCallback::new(|c| MockProcess::WithValue(321, c))),
            PopResult::Empty
        );
        assert!(queue.elements.is_empty());
        assert_eq!(
            queue
                .waiting_to_pop
                .into_iter()
                .map(|c| c.process('x'))
                .collect::<Vec<_>>(),
            vec![MockProcess::WithValue(321, 'x')]
        );
        assert!(queue.waiting_to_push.is_empty());
    }

    #[test]
    fn test_pop_and_wake() {
        let mut queue = MockQueue {
            elements: VecDeque::from(vec!['a', 'b', 'c']),
            waiting_to_push: VecDeque::from(vec![
                ('x', MockProcess::Empty(123)),
                ('y', MockProcess::Empty(321)),
            ]),
            capacity: 3,
            ..Default::default()
        };
        assert_eq!(
            queue.pop(ProcessCallback::new(|c| MockProcess::WithValue(321, c))),
            PopResult::Popped {
                process: MockProcess::WithValue(321, 'a'),
                resumed: Some(MockProcess::Empty(123))
            }
        );
        assert_eq!(queue.elements, VecDeque::from(vec!['b', 'c', 'x']));
        assert!(queue.waiting_to_pop.is_empty());
        assert_eq!(queue.waiting_to_push, vec![('y', MockProcess::Empty(321))]);
    }
}
