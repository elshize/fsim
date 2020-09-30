#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::default_trait_access)]

//! This is a general purpose simulation that provides the mechanisms such as: scheduler, state,
//! queues, etc.

pub use sim_derive::define_simulation;

pub use component::{Component, ComponentId};
pub use queue::Queue;
pub use scheduler::{EventEntry, Scheduler};
pub use state::{Key, QueueId, State};

mod component;
mod queue;
mod scheduler;
mod state;

//#[derive(Debug, Copy, Clone, PartialEq, Eq)]
//pub struct QueryRequest {
//    id: usize,
//    query_id: usize,
//}

//#[derive(Debug, Clone, PartialEq, Eq)]
//pub struct NodeRequest;

//#[allow(dead_code)]
//mod query_generator {
//    use super::ComponentId;
//    use crate::{Component, QueryRequest, QueueId, Scheduler, State};
//    use rand::Rng;
//    use rand_distr::Distribution;
//    use std::convert::TryFrom;
//    use std::time::Duration;

//    #[derive(Debug, Clone)]
//    pub enum Event {
//        GenerateQuery,
//    }

//    pub struct QueryGenerator<R, T, Q>
//    where
//        R: Rng,
//        T: Distribution<f32>,
//        Q: Distribution<usize>,
//    {
//        rng: R,
//        time_dist: T,
//        query_dist: Q,
//        counter: usize,
//        queries: Vec<usize>,
//        query_entry_queue: QueueId<QueryRequest>,
//        broker: ComponentId<super::broker::Event>,
//    }

//    impl<R, TD, QD> Component for QueryGenerator<R, TD, QD>
//    where
//        R: Rng,
//        TD: Distribution<f32>,
//        QD: Distribution<usize>,
//    {
//        type Event = Event;

//        // fn register(&mut self, id: ComponentId<Self::Event>) {
//        //     self.id = id;
//        // }

//        fn process_event(
//            &mut self,
//            self_id: ComponentId<Self::Event>,
//            event: &Self::Event,
//            scheduler: &mut Scheduler,
//            state: &mut State,
//        ) {
//            match event {
//                Event::GenerateQuery => {
//                    let request = self.generate_request();
//                    if state.send(self.query_entry_queue, request).is_err() {
//                        todo!("Log dropped request");
//                    } else {
//                        println!("Generated query at {:?}", scheduler.time());
//                    }
//                    scheduler.schedule(
//                        Duration::from_micros(self.next_interval()),
//                        self_id,
//                        Event::GenerateQuery,
//                    );
//                    scheduler.schedule(
//                        Duration::default(),
//                        self.broker,
//                        super::broker::Event::NewRequest,
//                    );
//                }
//            }
//        }
//    }

//    impl<R, T, Q> QueryGenerator<R, T, Q>
//    where
//        R: Rng,
//        T: Distribution<f32>,
//        Q: Distribution<usize>,
//    {
//        pub fn new(
//            rng: R,
//            time_dist: T,
//            query_dist: Q,
//            queries: Vec<usize>,
//            query_entry_queue: QueueId<QueryRequest>,
//            broker: ComponentId<super::broker::Event>,
//        ) -> Self {
//            Self {
//                rng,
//                time_dist,
//                query_dist,
//                counter: 0,
//                queries,
//                query_entry_queue,
//                broker,
//            }
//        }

//        fn generate_request(&mut self) -> QueryRequest {
//            let query_id = self.query_dist.sample(&mut self.rng);
//            let request_id = self.counter;
//            self.counter += 1;
//            QueryRequest {
//                id: request_id,
//                query_id,
//            }
//        }

//        fn next_interval(&mut self) -> u64 {
//            let timeout = self.time_dist.sample(&mut self.rng);
//            let timeout = match timeout.partial_cmp(&0_f32) {
//                None | Some(std::cmp::Ordering::Less) => 0_f32,
//                _ => timeout,
//            };
//            #[allow(clippy::cast_possible_truncation)]
//            u64::try_from(timeout.round() as i64).unwrap()
//        }
//    }
//}

//#[allow(dead_code)]
//mod broker {
//    use crate::{node, Component, ComponentId, Key, QueryRequest, QueueId, Scheduler, State};
//    use std::time::Duration;

//    #[derive(Debug, Clone)]
//    pub enum Event {
//        Idle,
//        NewRequest,
//        Dispatch {
//            request: QueryRequest,
//            shards: Key<Vec<usize>>,
//        },
//    }

//    pub struct Broker {
//        query_entry_queue: QueueId<QueryRequest>,
//        node_queues: Vec<QueueId<QueryRequest>>,
//        node_ids: Vec<ComponentId<node::Event>>,
//    }

//    impl Component for Broker {
//        type Event = Event;

//        // fn register(&mut self, id: ComponentId<Self::Event>) {
//        //     self.id = id;
//        // }

//        fn process_event(
//            &mut self,
//            self_id: ComponentId<Self::Event>,
//            event: &Self::Event,
//            scheduler: &mut Scheduler,
//            state: &mut State,
//        ) {
//            match event {
//                Event::Idle | Event::NewRequest => {
//                    if let Some(request) = state.recv(self.query_entry_queue) {
//                        println!("Broker picked up request: {:?}", request);
//                        // Selecting...
//                        let selection_time = Duration::from_micros(0);
//                        let shards: Vec<usize> = Vec::new();
//                        scheduler.schedule(
//                            selection_time,
//                            self_id,
//                            Event::Dispatch {
//                                request,
//                                shards: state.insert(shards),
//                            },
//                        );
//                    }
//                }
//                Event::Dispatch { request, shards } => {
//                    let shards = state
//                        .remove(*shards)
//                        .expect("Dispatcher cannot find selected shards/replicas");
//                    // Dispatch
//                    for shard in shards {
//                        let dispatch_overhead = Duration::from_micros(0);
//                        // NOTE: This isn't correct, must have both shard and machine
//                        // NOTE: Also, got to implement queue capacity.
//                        let node_queue = self.node_queues[shard];
//                        if state.send(node_queue, *request).is_err() {
//                            todo!("Log dropped shard request");
//                        }
//                        scheduler.schedule(
//                            dispatch_overhead,
//                            self.node_ids[0], // TODO
//                            super::node::Event::NewRequest,
//                        );
//                        scheduler.schedule(dispatch_overhead, self_id, Event::Idle);
//                    }
//                }
//            }
//        }
//    }

//    impl Broker {
//        pub fn new(
//            query_entry_queue: QueueId<QueryRequest>,
//            node_queues: Vec<QueueId<QueryRequest>>,
//            node_ids: Vec<ComponentId<node::Event>>,
//        ) -> Self {
//            Self {
//                query_entry_queue,
//                node_queues,
//                node_ids,
//            }
//        }
//    }
//}

//#[allow(dead_code)]
//mod node {
//    use super::*;

//    #[derive(Debug, Clone)]
//    pub enum Event {
//        NewRequest,
//    }

//    struct Node {}

//    impl Component for Node {
//        type Event = Event;

//        fn process_event(
//            &mut self,
//            _self_id: ComponentId<Self::Event>,
//            _event: &Self::Event,
//            _scheduler: &mut Scheduler,
//            _state: &mut State,
//        ) {
//            //
//        }
//    }
//}

//#[cfg(test)]
//mod test {
//    use super::*;
//    use sim_derive::define_simulation;
//    use std::io::Write;

//    // /// Simulation progression that continuously prints out data to a writer.
//    // pub struct FlushingTimeline<W: Write, L: Write> {
//    //     status: simulation::FlushStatus<W, L>,
//    //     step: usize,
//    // }

//    // impl<W: Write, L: Write> FlushingTimeline<W, L> {
//    //     /// Constructs a new progression with the given sinks.
//    //     pub fn new(data_sink: W, log_sink: L) -> Self {
//    //         Self {
//    //             status: simulation::FlushStatus::new(data_sink, log_sink),
//    //             step: 0,
//    //         }
//    //     }
//    // }

//    // impl<W: Write, L: Write> Timeline for FlushingTimeline<W, L> {
//    //     type Status = simulation::FlushStatus<W, L>;

//    //     fn status(&self) -> &Self::Status {
//    //         &self.status
//    //     }

//    //     fn status_mut(&mut self) -> &mut Self::Status {
//    //         &mut self.status
//    //     }

//    //     fn step(&self) -> usize {
//    //         self.step
//    //     }
//    // }

//    define_simulation! {
//        Simulation {
//            events: (query_generator::Event, broker::Event, node::Event),
//            bounding_trait: ValidEvent,
//        }
//    }

//    struct TestComponent {}
//    impl Component for TestComponent {
//        type Event = node::Event;
//        fn process_event(
//            &mut self,
//            _self_id: ComponentId<Self::Event>,
//            _event: &Self::Event,
//            _scheduler: &mut Scheduler,
//            _state: &mut State,
//        ) {
//            //
//        }
//    }

//    #[test]
//    fn test_xxx() {
//        // let timeline = FlushingTimeline::new(std::io::stdout(), std::io::stderr());
//        let mut sim = Simulation::default();

//        let query_entry_queue = sim.add_bounded_queue::<QueryRequest>(10);

//        let broker = sim.add_component(broker::Broker::new(
//            query_entry_queue,
//            Vec::new(),
//            Vec::new(),
//        ));
//        let query_generator = sim.add_component(query_generator::QueryGenerator::new(
//            rand::thread_rng(),
//            rand_distr::Normal::new(10.0, 1.0).unwrap(),
//            rand_distr::Uniform::new(0, 1),
//            Vec::new(),
//            query_entry_queue,
//            broker,
//        ));
//        sim.schedule(
//            Duration::default(),
//            query_generator,
//            query_generator::Event::GenerateQuery,
//        );

//        //sim.run();
//    }
//}
