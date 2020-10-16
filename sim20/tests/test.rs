use sim20::*;

#[derive(Debug, Copy, Clone)]
struct EventA;
#[derive(Debug, Copy, Clone)]
struct EventB;

simulation! {
    #[events(EventA, EventB)]
    trait ValidEvent {}

    #[simulation]
    struct Sim {
        state: State,
        scheduler: Scheduler,
        components: Components,
    }
}

fn test() {
    let mut sim = Sim::default();
    sim.run();
}
