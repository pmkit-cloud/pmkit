use std::fmt;
use std::sync::Arc;

use pmkit_core::{PortfolioId, RunId};
use pmkit_data::{LiveAccountDataSource, LiveCexDataSource, LiveDataSource};
use pmkit_exec::Executor;
use pmkit_run::TapePolicy;
use pmkit_runtime::{RiskLimits, StrategyRegistration};

/// A live run: live data, real execution.
pub struct LiveRun {
    id: RunId,
    portfolio: PortfolioId,
    executor: Arc<dyn Executor>,
    market_data: Arc<dyn LiveDataSource>,
    account_data: Option<Arc<dyn LiveAccountDataSource>>,
    reference_data: Option<Arc<dyn LiveCexDataSource>>,
    risk: RiskLimits,
    tape_policy: Option<TapePolicy>,
    strategies: Vec<StrategyRegistration>,
}

impl LiveRun {
    /// Creates a live run with no strategies yet.
    #[must_use]
    pub const fn new(
        id: RunId,
        portfolio: PortfolioId,
        executor: Arc<dyn Executor>,
        market_data: Arc<dyn LiveDataSource>,
        risk: RiskLimits,
    ) -> Self {
        Self {
            id,
            portfolio,
            executor,
            market_data,
            account_data: None,
            reference_data: None,
            risk,
            tape_policy: None,
            strategies: Vec::new(),
        }
    }

    /// Adds an optional live CEX reference source for parity-aware runs.
    #[must_use]
    pub fn reference_data(mut self, source: Arc<dyn LiveCexDataSource>) -> Self {
        self.reference_data = Some(source);
        self
    }

    /// Adds an optional authenticated PM account source.
    #[must_use]
    pub fn account_data(mut self, source: Arc<dyn LiveAccountDataSource>) -> Self {
        self.account_data = Some(source);
        self
    }

    /// Registers a strategy for this run.
    #[must_use]
    pub fn strategy(mut self, registration: StrategyRegistration) -> Self {
        self.strategies.push(registration);
        self
    }

    /// Enables a JSON-lines user tape under the runtime manifest directory.
    #[must_use]
    pub const fn tape(mut self, policy: TapePolicy) -> Self {
        self.tape_policy = Some(policy);
        self
    }

    /// Returns the run id.
    #[must_use]
    pub const fn id(&self) -> &RunId {
        &self.id
    }

    /// Returns the owning portfolio id.
    #[must_use]
    pub const fn portfolio(&self) -> &PortfolioId {
        &self.portfolio
    }

    /// Returns the live executor.
    #[must_use]
    pub const fn executor(&self) -> &Arc<dyn Executor> {
        &self.executor
    }

    /// Returns the live data source.
    #[must_use]
    pub const fn market_data(&self) -> &Arc<dyn LiveDataSource> {
        &self.market_data
    }

    /// Returns the optional live CEX reference source.
    #[must_use]
    pub const fn reference_data_ref(&self) -> Option<&Arc<dyn LiveCexDataSource>> {
        self.reference_data.as_ref()
    }

    /// Returns the optional authenticated PM account source.
    #[must_use]
    pub const fn account_data_ref(&self) -> Option<&Arc<dyn LiveAccountDataSource>> {
        self.account_data.as_ref()
    }

    /// Returns the risk limits.
    #[must_use]
    pub const fn risk(&self) -> &RiskLimits {
        &self.risk
    }

    /// Returns the configured user-tape policy, if tape capture is enabled.
    #[must_use]
    pub const fn tape_policy(&self) -> Option<TapePolicy> {
        self.tape_policy
    }

    /// Returns the registered strategies.
    #[must_use]
    pub fn strategies(&self) -> &[StrategyRegistration] {
        &self.strategies
    }
}

impl fmt::Debug for LiveRun {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveRun")
            .field("id", &self.id)
            .field("portfolio", &self.portfolio)
            .field("risk", &self.risk)
            .field("tape_policy", &self.tape_policy)
            .field(
                "reference_data",
                &self.reference_data.as_ref().map(|_| "configured"),
            )
            .field(
                "account_data",
                &self.account_data.as_ref().map(|_| "configured"),
            )
            .field("strategies", &self.strategies)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::LiveRun;
    use crate::test_support::{NoExec, NoLive, risk};
    use pmkit_core::{PortfolioId, RunId};
    use std::sync::Arc;

    #[test]
    fn live_run_converts_into_run_spec() -> Result<(), Box<dyn std::error::Error>> {
        let live = LiveRun::new(
            RunId::new("alice-live")?,
            PortfolioId::new("alice")?,
            Arc::new(NoExec),
            Arc::new(NoLive),
            risk()?,
        );
        assert!(matches!(
            crate::RunSpec::from(live),
            crate::RunSpec::Live(_)
        ));
        Ok(())
    }
}
