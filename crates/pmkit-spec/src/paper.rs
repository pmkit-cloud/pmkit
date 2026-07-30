use std::fmt;
use std::sync::Arc;

use pmkit_core::{PortfolioId, RunId};
use pmkit_data::{LiveAccountDataSource, LiveCexDataSource, LiveDataSource};
use pmkit_money::Money;
use pmkit_runtime::{RiskLimits, StrategyRegistration};

use crate::ConservativeV1Config;

/// A paper run: live data, simulated execution.
pub struct PaperRun {
    id: RunId,
    portfolio: PortfolioId,
    initial_cash: Money,
    risk: RiskLimits,
    market_data: Arc<dyn LiveDataSource>,
    account_data: Option<Arc<dyn LiveAccountDataSource>>,
    reference_data: Option<Arc<dyn LiveCexDataSource>>,
    simulation: ConservativeV1Config,
    strategies: Vec<StrategyRegistration>,
}

impl PaperRun {
    /// Creates a paper run with no strategies yet.
    #[must_use]
    pub const fn new(
        id: RunId,
        portfolio: PortfolioId,
        initial_cash: Money,
        risk: RiskLimits,
        market_data: Arc<dyn LiveDataSource>,
        simulation: ConservativeV1Config,
    ) -> Self {
        Self {
            id,
            portfolio,
            initial_cash,
            risk,
            market_data,
            account_data: None,
            reference_data: None,
            simulation,
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

    /// Returns the starting cash.
    #[must_use]
    pub const fn initial_cash(&self) -> Money {
        self.initial_cash
    }

    /// Returns the risk limits.
    #[must_use]
    pub const fn risk(&self) -> &RiskLimits {
        &self.risk
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

    /// Returns the fill-model configuration.
    #[must_use]
    pub const fn simulation(&self) -> &ConservativeV1Config {
        &self.simulation
    }

    /// Returns the registered strategies.
    #[must_use]
    pub fn strategies(&self) -> &[StrategyRegistration] {
        &self.strategies
    }
}

impl fmt::Debug for PaperRun {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PaperRun")
            .field("id", &self.id)
            .field("portfolio", &self.portfolio)
            .field("initial_cash", &self.initial_cash)
            .field("risk", &self.risk)
            .field("simulation", &self.simulation)
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
    use super::PaperRun;
    use crate::ConservativeV1Config;
    use crate::test_support::{NoLive, risk};
    use pmkit_core::{PortfolioId, RunId};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn paper_run_converts_into_run_spec() -> Result<(), Box<dyn std::error::Error>> {
        let paper = PaperRun::new(
            RunId::new("alice-paper")?,
            PortfolioId::new("alice")?,
            pmkit_money::Money::usdc(10_000),
            risk()?,
            Arc::new(NoLive),
            ConservativeV1Config {
                activation_latency: Duration::from_millis(100),
                maker_queue_ahead_bps: 0,
                slippage_bps: 0,
                market_impact_bps: 0,
                fee_model: None,
                market_limits: None,
            },
        );
        assert!(matches!(
            crate::RunSpec::from(paper),
            crate::RunSpec::Paper(_)
        ));
        Ok(())
    }
}
