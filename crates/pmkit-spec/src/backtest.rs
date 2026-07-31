use pmkit_core::{PortfolioId, RunId};
use pmkit_money::Money;
use pmkit_runtime::{RiskLimits, StrategyRegistration};

use crate::{ConservativeV1Config, ReplaySpec};

/// A deterministic backtest run.
#[derive(Debug)]
pub struct BacktestRun {
    id: RunId,
    portfolio: PortfolioId,
    replay: ReplaySpec,
    initial_cash: Money,
    risk: RiskLimits,
    simulation: ConservativeV1Config,
    strategies: Vec<StrategyRegistration>,
}

impl BacktestRun {
    /// Creates a backtest run with no strategies yet.
    #[must_use]
    pub const fn new(
        id: RunId,
        portfolio: PortfolioId,
        replay: ReplaySpec,
        initial_cash: Money,
        risk: RiskLimits,
        simulation: ConservativeV1Config,
    ) -> Self {
        Self {
            id,
            portfolio,
            replay,
            initial_cash,
            risk,
            simulation,
            strategies: Vec::new(),
        }
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

    /// Returns the replay specification.
    #[must_use]
    pub const fn replay(&self) -> &ReplaySpec {
        &self.replay
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

#[cfg(test)]
mod tests {
    use super::BacktestRun;
    use crate::test_support::{FlatFactory, NoHistory, risk};
    use crate::{ConservativeV1Config, ReplaySpec};
    use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
    use pmkit_runtime::StrategyRegistration;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn backtest_run_builds_and_converts() -> Result<(), Box<dyn std::error::Error>> {
        let replay = ReplaySpec::new(
            Arc::new(NoHistory),
            "2026-01-01T00:00:00Z".parse()?,
            "2026-02-01T00:00:00Z".parse()?,
            pmkit_run::EvidenceRequirement::CorroboratedOnly,
            pmkit_run::RetrievalWait::ReturnPending,
        );
        let run = BacktestRun::new(
            RunId::new("research")?,
            PortfolioId::new("research")?,
            replay,
            pmkit_money::Money::usdc(100_000),
            risk()?,
            ConservativeV1Config {
                activation_latency: Duration::from_millis(50),
                maker_queue_ahead_bps: 0,
                slippage_bps: 0,
                market_impact_bps: 0,
                fee_model: None,
                market_limits: None,
            },
        )
        .strategy(StrategyRegistration::new(
            StrategyId::new("maker")?,
            MarketId::new("btc-5m")?,
            Arc::new(FlatFactory),
        ));

        assert_eq!(run.strategies().len(), 1);
        assert_eq!(run.initial_cash(), pmkit_money::Money::usdc(100_000));
        assert!(matches!(
            crate::RunSpec::from(run),
            crate::RunSpec::Backtest(_)
        ));
        Ok(())
    }
}
