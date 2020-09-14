//! Common tools for setup programs.

#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions, clippy::default_trait_access)]

use std::fmt;
use std::path::Path;
use std::str::FromStr;
use url::Url;

/// Document collection.
#[allow(missing_docs)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Collection {
    Gov2,
    Clueweb09b,
}

impl Collection {
    /// Number of shards for the requested collection.
    #[must_use]
    pub fn num_shards(self) -> usize {
        match self {
            Collection::Gov2 => 199,
            Collection::Clueweb09b => 123,
        }
    }
}

impl fmt::Display for Collection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Gov2 => write!(f, "GOV2"),
            Self::Clueweb09b => write!(f, "CW09B"),
        }
    }
}

impl FromStr for Collection {
    type Err = String;
    fn from_str(collection: &str) -> Result<Self, Self::Err> {
        match collection {
            "gov2" | "Gov2" | "GOV2" => Ok(Self::Gov2),
            "cw09b" | "CW09B" | "CW09b" => Ok(Self::Clueweb09b),
            _ => Err(format!("Unknown collection: {}", collection)),
        }
    }
}

/// Download a file from the `url` and store at `dest`.
///
/// # Errors
///
/// It will return an error if cannot connect, download fails, or an error occurs while writing the
/// downloaded file to the local drive.
pub fn download_file<P: AsRef<Path>>(url: &Url, dest: P) -> anyhow::Result<()> {
    std::fs::write(dest, attohttpc::get(url).send()?.text()?)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_collection_from_str() -> Result<(), String> {
        assert_eq!(Collection::Gov2, "gov2".parse()?);
        assert_eq!(Collection::Gov2, "Gov2".parse()?);
        assert_eq!(Collection::Gov2, "GOV2".parse()?);
        assert_eq!(Collection::Clueweb09b, "cw09b".parse()?);
        assert_eq!(Collection::Clueweb09b, "CW09B".parse()?);
        assert_eq!(Collection::Clueweb09b, "CW09b".parse()?);
        let col: Result<Collection, String> = "CW09".parse();
        assert_eq!(col.err(), Some(String::from("Unknown collection: CW09")));
        Ok(())
    }

    #[test]
    fn test_collection_to_string() {
        assert_eq!(&Collection::Gov2.to_string(), "GOV2");
        assert_eq!(&Collection::Clueweb09b.to_string(), "CW09B");
    }
}
