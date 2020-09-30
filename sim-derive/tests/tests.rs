// #[test]
// #[ignore]
// fn simulation_derive_compile_fail() {
//     let t = trybuild::TestCases::new();
//     t.compile_fail("tests/errors/*.rs");
// }

// #[test]
// #[ignore]
// pub fn simulation_derive_expand() {
//     macrotest::expand("tests/expand/*.rs");
// }

// mod a {
//     pub enum Event {
//         X,
//     }
// }
// mod b {
//     pub enum Event {}
// }

// sim_derive::simulation! {
//     Simulation,
//     event = enum Event {
//         EventA(a::Event),
//         EventB(b::Event),
//     },
// }

// struct X {}

// impl Component for X {
//     type Event = a::Event;
//     fn process_event(&mut self, event: &Self::Event, scheduler: &mut Scheduler) {
//         scheduler.schedule(std::time::Duration::from_secs(0), a::Event::X);
//     }
// }

// #[test]
// pub fn ttt() {
//     let mut sim = Simulation::new();
//     let event = Event::EventA(a::Event::X);
//     sim.add_component(X {});
//     sim.run();
// }
