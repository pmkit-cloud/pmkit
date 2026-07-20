//! Run specifications for `PMKit`.
//!
//! A run is one portfolio in one mode with one or more strategy registrations.
//! [`BacktestRun`], [`PaperRun`], and [`LiveRun`] are the user-facing recipes;
//! [`RunSpec`] is the tagged union the runtime consumes.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use pmkit_core::{PortfolioId, RunId};
use pmkit_data::{HistoricalDataSource, LiveDataSource};
use pmkit_exec::Executor;
use pmkit_money::Money;
use pmkit_run::{EvidenceRequirement, RetrievalWait};
use pmkit_runtime::{RiskLimits, StrategyRegistration};

/// Configuration for the conservative-V1 fill model.
#[derive(Debug, Clone)]
pub struct ConservativeV1Config {
    /// Delay before a newly submitted order can act on fresh data.
    pub activation_latency: Duration,
}

/// A bounded historical replay specification.
#[derive(Clone)]
pub struct ReplaySpec {
    source: Arc<dyn HistoricalDataSource>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    evidence: EvidenceRequirement,
    retrieval_wait: RetrievalWait,
}

impl ReplaySpec {
    /// Creates a replay specification over `source` for `[from, to)`.
    #[must_use]
    pub const fn new(
        source: Arc<dyn HistoricalDataSource>,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        evidence: EvidenceRequirement,
        retrieval_wait: RetrievalWait,
    ) -> Self {
        Self {
            source,
            from,
            to,
            evidence,
            retrieval_wait,
        }
    }

    /// Returns the historical data source.
    #[must_use]
    pub const fn source(&self) -> &Arc<dyn HistoricalDataSource> {
        &self.source
    }

    /// Returns the inclusive window start.
    #[must_use]
    pub const fn from(&self) -> DateTime<Utc> {
        self.from
    }

    /// Returns the exclusive window end.
    #[must_use]
    pub const fn to(&self) -> DateTime<Utc> {
        self.to
    }

    /// Returns the required corroboration.
    #[must_use]
    pub const fn evidence(&self) -> EvidenceRequirement {
        self.evidence
    }

    /// Returns the retrieval-wait policy.
    #[must_use]
    pub const fn retrieval_wait(&self) -> RetrievalWait {
        self.retrieval_wait
    }
}

impl fmt::Debug for ReplaySpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplaySpec")
            .field("from", &self.from)
            .field("to", &self.to)
            .field("evidence", &self.evidence)
            .field("retrieval_wait", &self.retrieval_wait)
            .finish_non_exhaustive()
    }
}

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

/// A paper run: live data, simulated execution.
pub struct PaperRun {
    id: RunId,
    portfolio: PortfolioId,
    initial_cash: Money,
    risk: RiskLimits,
    market_data: Arc<dyn LiveDataSource>,
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
            .field("strategies", &self.strategies)
            .finish_non_exhaustive()
    }
}

/// A live run: live data, real execution.
pub struct LiveRun {
    id: RunId,
    portfolio: PortfolioId,
    executor: Arc<dyn Executor>,
    market_data: Arc<dyn LiveDataSource>,
    risk: RiskLimits,
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
            risk,
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

    /// Returns the risk limits.
    #[must_use]
    pub const fn risk(&self) -> &RiskLimits {
        &self.risk
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
            .field("strategies", &self.strategies)
            .finish_non_exhaustive()
    }
}

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

#[cfg(test)]
mod tests {
    use super::{BacktestRun, ConservativeV1Config, LiveRun, PaperRun, ReplaySpec, RunSpec};
    use async_trait::async_trait;
    use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
    use pmkit_data::{DataSourceError, HistoricalDataSource, LiveDataSource, ReplayQuery};
    use pmkit_event::MarketEvent;
    use pmkit_exec::{ExecError, Executor, OrderId, PlaceOrder};
    use pmkit_market::Outcome;
    use pmkit_money::Money;
    use pmkit_run::{EvidenceRequirement, RetrievalWait};
    use pmkit_runtime::{RiskLimits, StrategyRegistration};
    use pmkit_strategy::{
        Actions, Strategy, StrategyContext, StrategyError, StrategyFactory, StrategyInitError,
    };
    use std::num::NonZeroU32;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc::Sender;

    struct NoHistory;

    #[async_trait]
    impl HistoricalDataSource for NoHistory {
        async fn replay(
            &self,
            _query: ReplayQuery,
            _sink: Sender<MarketEvent>,
        ) -> Result<(), DataSourceError> {
            Ok(())
        }
    }

    struct NoLive;

    #[async_trait]
    impl LiveDataSource for NoLive {
        async fn subscribe(
            &self,
            _market: MarketId,
            _outcome: Outcome,
            _sink: Sender<MarketEvent>,
        ) -> Result<(), DataSourceError> {
            Ok(())
        }
    }

    struct NoExec;

    #[async_trait]
    impl Executor for NoExec {
        async fn submit(&self, _order: &PlaceOrder, _now_ms: i64) -> Result<OrderId, ExecError> {
            Ok(OrderId("x".to_owned()))
        }

        async fn cancel(&self, _order_id: &OrderId) -> Result<(), ExecError> {
            Ok(())
        }

        async fn cancel_all(&self) -> Result<(), ExecError> {
            Ok(())
        }
    }

    struct FlatStrategy;

    impl Strategy for FlatStrategy {
        fn on_event(&mut self, _ctx: StrategyContext<'_>) -> Result<Actions, StrategyError> {
            Ok(Actions::none())
        }
    }

    struct FlatFactory;

    impl StrategyFactory for FlatFactory {
        fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
            Ok(Box::new(FlatStrategy))
        }
    }

    fn risk() -> Result<RiskLimits, Box<dyn std::error::Error>> {
        Ok(RiskLimits {
            max_order_notional: Money::usdc(100),
            max_position_notional: Money::usdc(1_000),
            max_open_orders: NonZeroU32::new(10).ok_or("nonzero")?,
            max_loss: Money::usdc(500),
        })
    }

    #[test]
    fn backtest_run_builds_and_converts() -> Result<(), Box<dyn std::error::Error>> {
        let replay = ReplaySpec::new(
            Arc::new(NoHistory),
            "2026-01-01T00:00:00Z".parse()?,
            "2026-02-01T00:00:00Z".parse()?,
            EvidenceRequirement::CorroboratedOnly,
            RetrievalWait::ReturnPending,
        );
        let run = BacktestRun::new(
            RunId::new("research")?,
            PortfolioId::new("research")?,
            replay,
            Money::usdc(100_000),
            risk()?,
            ConservativeV1Config {
                activation_latency: Duration::from_millis(50),
            },
        )
        .strategy(StrategyRegistration::new(
            StrategyId::new("maker")?,
            MarketId::new("btc-5m")?,
            Arc::new(FlatFactory),
        ));

        assert_eq!(run.strategies().len(), 1);
        assert_eq!(run.initial_cash(), Money::usdc(100_000));
        assert!(matches!(RunSpec::from(run), RunSpec::Backtest(_)));
        Ok(())
    }

    #[test]
    fn paper_and_live_convert_into_run_spec() -> Result<(), Box<dyn std::error::Error>> {
        let paper = PaperRun::new(
            RunId::new("alice-paper")?,
            PortfolioId::new("alice")?,
            Money::usdc(10_000),
            risk()?,
            Arc::new(NoLive),
            ConservativeV1Config {
                activation_latency: Duration::from_millis(100),
            },
        );
        assert!(matches!(RunSpec::from(paper), RunSpec::Paper(_)));

        let live = LiveRun::new(
            RunId::new("alice-live")?,
            PortfolioId::new("alice")?,
            Arc::new(NoExec),
            Arc::new(NoLive),
            risk()?,
        );
        assert!(matches!(RunSpec::from(live), RunSpec::Live(_)));
        Ok(())
    }
}
