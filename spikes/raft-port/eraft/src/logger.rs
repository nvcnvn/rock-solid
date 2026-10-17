// Ported from go.etcd.io/raft v3.7.0 (b867cf13f6bc0dae21204302df97bc2355c3af55),
// logger.go. Licensed under the Apache License 2.0.
//
// Deviation: Go's Logger has a print and a printf method per level plus a
// process-wide default that writes to stderr. Here one `log` method takes
// pre-formatted arguments, `enabled` lets callers skip formatting, and the
// default logger discards everything (a sans-IO crate does no I/O).

//! Logging interface.

use std::fmt;

/// Log level.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    /// Debug.
    Debug = 0,
    /// Info.
    Info = 1,
    /// Warning.
    Warn = 2,
    /// Error.
    Error = 3,
    /// Fatal; logged before a panic.
    Fatal = 4,
}

/// Receives raft's log output.
pub trait Logger {
    /// True if messages at `level` are recorded.
    fn enabled(&self, level: Level) -> bool;
    /// Records a message.
    fn log(&self, level: Level, args: fmt::Arguments<'_>);
}

/// A logger that discards everything.
#[derive(Clone, Copy, Debug, Default)]
pub struct NopLogger;

impl Logger for NopLogger {
    fn enabled(&self, _: Level) -> bool {
        false
    }
    fn log(&self, _: Level, _: fmt::Arguments<'_>) {}
}

macro_rules! log_at {
    ($l:expr, $lvl:expr, $($arg:tt)*) => {{
        let l: &dyn $crate::logger::Logger = &*$l;
        if l.enabled($lvl) {
            l.log($lvl, format_args!($($arg)*));
        }
    }};
}

macro_rules! debugf { ($l:expr, $($arg:tt)*) => { log_at!($l, $crate::logger::Level::Debug, $($arg)*) }; }
macro_rules! infof { ($l:expr, $($arg:tt)*) => { log_at!($l, $crate::logger::Level::Info, $($arg)*) }; }
macro_rules! warningf { ($l:expr, $($arg:tt)*) => { log_at!($l, $crate::logger::Level::Warn, $($arg)*) }; }
macro_rules! errorf { ($l:expr, $($arg:tt)*) => { log_at!($l, $crate::logger::Level::Error, $($arg)*) }; }
macro_rules! panicf {
    ($l:expr, $($arg:tt)*) => {{
        log_at!($l, $crate::logger::Level::Fatal, $($arg)*);
        panic!($($arg)*)
    }};
}
