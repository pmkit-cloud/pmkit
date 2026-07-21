use crate::{BacktestRun, LiveRun, PaperRun};

/// A run specification the runtime can execute.
#[derive(Debug)]
pub enum RunSpec {
    /// A deterministic backtest.
    Backtest(Box<BacktestRun>),
    /// A paper run.
    Paper(Box<PaperRun>),
    /// A live run.
    Live(Box<LiveRun>),
}

impl From<BacktestRun> for RunSpec {
    fn from(run: BacktestRun) -> Self {
        Self::Backtest(Box::new(run))
    }
}

impl From<PaperRun> for RunSpec {
    fn from(run: PaperRun) -> Self {
        Self::Paper(Box::new(run))
    }
}

impl From<LiveRun> for RunSpec {
    fn from(run: LiveRun) -> Self {
        Self::Live(Box::new(run))
    }
}
