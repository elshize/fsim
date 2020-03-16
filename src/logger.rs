//! Implementation of a logger that accumulates messages in a vector buffer.
//! Then, all buffered messages can be taken out of at once.
//! This is done to facilitate clustering together log messages and incorporate that
//! in simulation history. See the following section for an example.
//!
//! # Examples
//!
//! In the example below, we first initialize the logger. Note that, in contrast of many logger
//! implementations, calling [`init`](fn.init.html) will not fail—it will simply be a no-op.
//! ```
//! # use fsim::logger;
//! # use log;
//! # fn main() -> anyhow::Result<()> {
//! // Initialize logger.
//! logger::LoggerBuilder::default().level(log::LevelFilter::Trace).init()?;
//! // Log events.
//! log::info!("Info message");
//! log::debug!("Debug message");
//! log::warn!("Warn message");
//! log::error!("Error message");
//! // Clear buffer and take its contents.
//! let logs = logger::clear()?;
//! assert_eq!(
//!     logs,
//!     vec![
//!         String::from("[INFO]  Info message"),
//!         String::from("[DEBUG] Debug message"),
//!         String::from("[WARN]  Warn message"),
//!         String::from("[ERROR] Error message"),
//!     ]
//! );
//! // Log more events.
//! log::info!("Following message");
//! // This time only new events will be returned.
//! assert_eq!(
//!     logger::clear()?,
//!     vec![String::from("[INFO]  Following message")]
//! );
//! # Ok(())
//! # }
//! ```

use anyhow::anyhow;
use log::LevelFilter;
use std::sync::{Arc, RwLock};

lazy_static::lazy_static! {
    static ref LOG_BUFFER: Arc<RwLock<Vec<String>>> = Arc::new(RwLock::new(Vec::new()));
    static ref BUFFER_INITIALIZED: Arc<RwLock<bool>> = Arc::new(RwLock::new(false));
}

/// Builds a vector logger.
pub struct LoggerBuilder {
    level: LevelFilter,
    target: Option<String>,
}

impl Default for LoggerBuilder {
    fn default() -> Self {
        Self {
            level: LevelFilter::Warn,
            target: None,
        }
    }
}

impl LoggerBuilder {
    /// Sets level filter.
    pub fn level(mut self, level: LevelFilter) -> Self {
        self.level = level;
        self
    }
    /// Sets logging target prefix.
    pub fn target<S: Into<String>>(mut self, target: S) -> Self {
        self.target = Some(target.into());
        self
    }
    /// Initializes vector logger.
    pub fn init(self) -> anyhow::Result<()> {
        if !*BUFFER_INITIALIZED
            .read()
            .map_err(|err| anyhow!("{:?}", err))?
        {
            let buffer = Arc::clone(&LOG_BUFFER);
            let mut dispatch = fern::Dispatch::new()
                .level(self.level)
                .chain(fern::Output::call(move |record| {
                    buffer.write().expect("Poisoned lock").push(format!(
                        "{:7} {}",
                        format!("[{}]", record.level()),
                        record.args()
                    ));
                }));
            if let Some(target) = self.target {
                dispatch = dispatch.filter(move |metadata| metadata.target().starts_with(&target))
            }
            dispatch.apply()?;
            *BUFFER_INITIALIZED
                .write()
                .map_err(|err| anyhow!("{:?}", err))? = true;
        }
        Ok(())
    }
}

//pub fn init(level: log::LevelFilter) -> anyhow::Result<()> {
//    if !*BUFFER_INITIALIZED
//        .read()
//        .map_err(|err| anyhow!("{:?}", err))?
//    {
//        let buffer = Arc::clone(&LOG_BUFFER);
//        fern::Dispatch::new()
//            .level(level)
//            .filter(|metadata| metadata.target().starts_with("fsim"))
//            .chain(fern::Output::call(move |record| {
//                buffer.write().expect("Poisoned lock").push(format!(
//                    "{:7} {}",
//                    format!("[{}]", record.level()),
//                    record.args()
//                ));
//            }))
//            .apply()?;
//        *BUFFER_INITIALIZED
//            .write()
//            .map_err(|err| anyhow!("{:?}", err))? = true;
//    }
//    Ok(())
//}

/// Clears the current log buffer and returns its contents.
pub fn clear() -> anyhow::Result<Vec<String>> {
    let mut handle = LOG_BUFFER.write().map_err(|err| anyhow!("{:?}", err))?;
    Ok(handle.drain(..).collect())
}
