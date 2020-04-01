//! Query routing. More docs to come...

#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::default_trait_access)]

pub mod optimization;
pub mod simulation;
pub mod tui;

#[cfg(test)]
mod test {
    use rand::distributions::Distribution;

    pub(crate) struct WrappingEchoDistribution<T> {
        size: T,
    }

    impl<T> WrappingEchoDistribution<T> {
        pub(crate) fn new(size: T) -> Self {
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

    pub(crate) struct ToFloatDistribution<D>(pub(crate) D);

    impl<D> Distribution<f32> for ToFloatDistribution<D>
    where
        D: Distribution<u64>,
    {
        fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> f32 {
            self.0.sample(rng) as f32
        }
    }
}
