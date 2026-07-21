//! Run specifications for `PMKit`.
//!
//! A run is one portfolio in one mode with one or more strategy registrations.
//! [`BacktestRun`], [`PaperRun`], and [`LiveRun`] are the user-facing recipes;
//! [`RunSpec`] is the tagged union the runtime consumes.

mod backtest;
mod config;
mod live;
mod paper;
mod replay;
mod run_spec;

#[cfg(test)]
mod test_support;

pub use backtest::BacktestRun;
pub use config::ConservativeV1Config;
pub use live::LiveRun;
pub use paper::PaperRun;
pub use replay::ReplaySpec;
pub use run_spec::RunSpec;
