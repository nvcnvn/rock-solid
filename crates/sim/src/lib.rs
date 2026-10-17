//! Deterministic simulator, fault models, simulated disk and driver-owned heap buffer pool.

pub mod disk;
#[cfg(test)]
mod log_tests;
pub mod pool;
