/// Interface for runnable objects that take payload and return an effect.
pub trait Runnable {
    /// The argument passed to `run`.
    type Payload;
    /// The type returned from `run`.
    type Effect;
    /// Executes the runnable with a payload, returning an effect.
    fn run(&self, payload: Self::Payload) -> Self::Effect;
}
