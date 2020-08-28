//! Query routing. More docs to come...

#![warn(
    missing_docs,
    rust_2018_idioms,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::module_name_repetitions,
    clippy::default_trait_access,
    clippy::cast_precision_loss
)]

use rand::distributions::Distribution;

/// This distribution produces values between 0 and `N - 1` by requesting the `next_u32` from the
/// random number generator and applying `mod N` operation on it.
///
/// This is meant for testing, e.g., together with `rand::rngs::mock::StepRng` it can produce
/// predictable values that can be used in unit tests.
pub struct WrappingEchoDistribution<T> {
    size: T,
}

impl<T> WrappingEchoDistribution<T> {
    /// Constructs a new distribution generating values between 0 and `size - 1`.
    pub fn new(size: T) -> Self {
        Self { size }
    }
}

impl<T> Distribution<T> for WrappingEchoDistribution<T>
where
    T: std::convert::TryFrom<u32> + std::ops::Rem<T, Output = T> + Copy,
    <T as std::convert::TryFrom<u32>>::Error: std::fmt::Debug,
{
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> T {
        T::try_from(rng.next_u32()).unwrap() % self.size
    }
}

/// A wrapper over a distribution generating integer values that casts them to floats.
pub struct ToFloatDistribution<D>(D);

impl<D> ToFloatDistribution<D> {
    /// Constructs a float distribution from an integer one.
    pub fn new(dist: D) -> Self {
        Self(dist)
    }
}

impl<D> Distribution<f32> for ToFloatDistribution<D>
where
    D: Distribution<u64>,
{
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> f32 {
        self.0.sample(rng) as f32
    }
}
