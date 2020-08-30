/// Defines a strongly typed array wrapper.
#[macro_export]
macro_rules! array_wrapper {
    ($name:ident, $t:ty, $doc:literal) => {
        /// $doc
        #[derive(Debug, Clone)]
        pub struct $name(::ndarray::Array1<$t>);

        impl From<::ndarray::Array1<$t>> for $name {
            fn from(array: ::ndarray::Array1<$t>) -> Self {
                Self(array)
            }
        }

        impl<'a> From<&'a $name> for ::ndarray::ArrayView1<'a, $t> {
            fn from(array: &'a $name) -> Self {
                array.0.view()
            }
        }

        impl ::std::iter::FromIterator<$t> for $name {
            fn from_iter<T>(iter: T) -> Self
            where
                T: IntoIterator<Item = $t>,
            {
                Self(iter.into_iter().collect())
            }
        }

        impl $name {
            /// Vector length.
            #[must_use]
            pub fn len(&self) -> usize {
                self.0.len()
            }

            /// Iterates the vector.
            pub fn iter(&self) -> impl Iterator<Item = &$t> {
                self.0.iter()
            }

            /// Returns the underlying vector.
            #[must_use]
            pub fn vec(&self) -> &::ndarray::Array1<$t> {
                &self.0
            }

            /// Returns the underlying vector.
            pub fn vec_mut(&mut self) -> &mut ::ndarray::Array1<$t> {
                &mut self.0
            }

            /// Iterates over filtered elements of the vector.
            /// Any `i`-th element will be present iff the `i`-th element of `mask` is `true`.
            pub fn filter<'a, I>(&'a self, mask: I) -> impl Iterator<Item = &'a $t>
            where
                I: IntoIterator<Item = &'a bool>,
            {
                self.0
                    .iter()
                    .zip(mask.into_iter())
                    .filter(|(_, m)| **m)
                    .map(|(f, _)| f)
            }
        }
    };
}
