struct ComponentId;

enum EventA {}
enum EventB {}

#[sim_derive::simulation(events = [EventA, EventB])]
struct Simulation {}

pub fn main() {
    let sim = Simulation {};
}
