//! `PMKit` orchestration engine.
//!
//! [`Pmkit::builder`] collects [`RunSpec`]s; [`PmkitBuilder::start`] validates
//! them and drives each backtest end to end (replay -> simulation -> strategy
//! -> fills), returning an [`AppHandle`] that exposes each run's
//! [`RunReport`]. Paper runs are driven similarly against a live data source;
//! live runs route strategy orders through a consented executor behind a
//! minimal risk gate (a v0 that omits reconciliation, tape, and full limits).

use std::collections::{HashMap, HashSet};

use pmkit_book::OrderBookL2;
use pmkit_core::{MarketId, RunId};
use pmkit_event::MarketEvent;
use pmkit_exec::{Executor, PlaceOrder};
use pmkit_market::Outcome;
use pmkit_paper::PaperExecutor;
use pmkit_run::LiveConsent;
use pmkit_runtime::{RiskLimits, RuntimeConfig, StrategyRegistration};
use pmkit_sim::{MarketCategory, SimEngine};
use pmkit_spec::{BacktestRun, LiveRun, PaperRun, RunSpec};
use pmkit_strategy::{Action, LogicalTimestamp, Strategy, StrategyContext};
use thiserror::Error;

/// A registered strategy instance keyed by its exact market.
type StrategyInstance = (MarketId, Box<dyn Strategy>);

pub use pmkit_data::ReplayQuery;

/// The terminal report of a backtest run.
#[derive(Debug, Clone)]
pub struct BacktestReport {
    /// The run this report belongs to.
    pub run: RunId,
    /// Total events consumed from the replay.
    pub events_processed: usize,
    /// Total simulated fills produced.
    pub fills: usize,
}

/// The terminal report of a paper run.
#[derive(Debug, Clone)]
pub struct PaperReport {
    /// The run this report belongs to.
    pub run: RunId,
    /// Total events consumed from the live feed.
    pub events_processed: usize,
    /// Total simulated fills produced.
    pub fills: usize,
}

/// The terminal report of a live run.
#[derive(Debug, Clone)]
pub struct LiveReport {
    /// The run this report belongs to.
    pub run: RunId,
    /// Total events consumed from the live feed.
    pub events_processed: usize,
    /// Total venue fills observed.
    pub fills: usize,
    /// Orders rejected by the risk gate before reaching the venue.
    pub rejected: usize,
}

/// The terminal report of a run.
#[derive(Debug, Clone)]
pub enum RunReport {
    /// A completed backtest.
    Backtest(BacktestReport),
    /// A completed paper run.
    Paper(PaperReport),
    /// A completed live run.
    Live(LiveReport),
}

/// A failure raised while starting runs.
#[derive(Debug, Error)]
pub enum StartError {
    /// Two runs shared the same [`RunId`].
    #[error("duplicate run id: {0}")]
    DuplicateRunId(RunId),
    /// A strategy factory failed during instantiation.
    #[error("strategy init failed for run {run}: {source}")]
    StrategyInit {
        /// The run whose strategy failed.
        run: RunId,
        /// The underlying factory error.
        source: pmkit_strategy::StrategyInitError,
    },
    /// A live run was configured without calling `enable_live`.
    #[error("live run {0} requires enable_live consent")]
    LiveConsentMissing(RunId),
}

/// Entry point to the engine.
#[derive(Debug)]
pub struct Pmkit;

impl Pmkit {
    /// Starts a builder with the given runtime configuration.
    #[must_use]
    pub const fn builder(config: RuntimeConfig) -> PmkitBuilder {
        PmkitBuilder {
            config,
            runs: Vec::new(),
            consent: None,
        }
    }
}

/// Collects runs before starting the engine.
#[derive(Debug)]
pub struct PmkitBuilder {
    config: RuntimeConfig,
    runs: Vec<RunSpec>,
    consent: Option<LiveConsent>,
}

impl PmkitBuilder {
    /// Adds a run to the topology.
    #[must_use]
    pub fn run(mut self, spec: impl Into<RunSpec>) -> Self {
        self.runs.push(spec.into());
        self
    }

    /// Records explicit consent to place real live orders.
    #[must_use]
    pub const fn enable_live(mut self, consent: LiveConsent) -> Self {
        self.consent = Some(consent);
        self
    }

    /// Validates the topology and drives every run to completion.
    ///
    /// # Errors
    ///
    /// Returns [`StartError`] on a duplicate run id or a strategy that fails to
    /// initialise.
    pub async fn start(self) -> Result<AppHandle, StartError> {
        // ponytail: backtest_concurrency is accepted but v0 runs sequentially.
        let Self {
            config,
            runs,
            consent,
        } = self;

        let mut seen = HashSet::new();
        for spec in &runs {
            let id = run_id_of(spec);
            if !seen.insert(id.clone()) {
                return Err(StartError::DuplicateRunId(id.clone()));
            }
        }

        let mut reports = HashMap::new();
        for spec in runs {
            match spec {
                RunSpec::Backtest(run) => {
                    let report = drive_backtest(&run).await?;
                    reports.insert(run.id().clone(), RunReport::Backtest(report));
                }
                RunSpec::Paper(run) => {
                    let report = drive_paper(&run).await?;
                    reports.insert(run.id().clone(), RunReport::Paper(report));
                }
                RunSpec::Live(run) => {
                    if consent.is_none() {
                        return Err(StartError::LiveConsentMissing(run.id().clone()));
                    }
                    let report = drive_live(&run).await?;
                    reports.insert(run.id().clone(), RunReport::Live(report));
                }
            }
        }
        Ok(AppHandle { reports, config })
    }
}

/// Handle to a started engine, holding each run's terminal report.
#[derive(Debug)]
pub struct AppHandle {
    reports: HashMap<RunId, RunReport>,
    config: RuntimeConfig,
}

impl AppHandle {
    /// Returns the report for `run`, if it exists.
    #[must_use]
    pub fn report(&self, run: &RunId) -> Option<&RunReport> {
        self.reports.get(run)
    }

    /// Returns the runtime configuration the engine started with.
    #[must_use]
    pub const fn config(&self) -> &RuntimeConfig {
        &self.config
    }
}

fn run_id_of(spec: &RunSpec) -> &RunId {
    match spec {
        RunSpec::Backtest(run) => run.id(),
        RunSpec::Paper(run) => run.id(),
        RunSpec::Live(run) => run.id(),
    }
}

async fn drive_backtest(run: &BacktestRun) -> Result<BacktestReport, StartError> {
    let mut strategies = instantiate_strategies(run.strategies(), run.id())?;

    let markets = strategies
        .iter()
        .map(|(market, _)| market.clone())
        .collect();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    let source = run.replay().source().clone();
    let query = ReplayQuery {
        markets,
        from: run.replay().from(),
        to: run.replay().to(),
        evidence: run.replay().evidence(),
        retrieval_wait: run.replay().retrieval_wait(),
    };
    let replay = tokio::spawn(async move { source.replay(query, tx).await });

    // ponytail: fee category fixed to Crypto and positions untracked in v0.
    let mut sim = SimEngine::new("bt", 0, MarketCategory::Crypto);
    let positions = Vec::new();
    let mut events_processed = 0_usize;
    let mut fills = 0_usize;

    while let Some(event) = rx.recv().await {
        events_processed += 1;
        if let MarketEvent::BookUpdate {
            market,
            outcome,
            bids,
            asks,
            timestamp_ms,
        } = &event
        {
            let book = OrderBookL2 {
                bids: bids.clone(),
                asks: asks.clone(),
                timestamp_ms: *timestamp_ms,
                last_trade_price: None,
            };
            sim.update_book(market, *outcome, book.clone());
            fills += sim.drain_fills().len();
            fills += run_strategies(
                &mut strategies,
                market,
                &book,
                &positions,
                *timestamp_ms,
                &mut sim,
            );
        }
    }

    let _ = replay.await;
    Ok(BacktestReport {
        run: run.id().clone(),
        events_processed,
        fills,
    })
}

fn run_strategies(
    strategies: &mut [StrategyInstance],
    market: &pmkit_core::MarketId,
    book: &OrderBookL2,
    positions: &[pmkit_book::Position],
    timestamp_ms: i64,
    sim: &mut SimEngine,
) -> usize {
    let mut fills = 0;
    for (registered_market, strategy) in &mut *strategies {
        if *registered_market != *market {
            continue;
        }
        let context = StrategyContext {
            market,
            book,
            positions,
            now: LogicalTimestamp::from_millis(timestamp_ms),
        };
        if let Ok(actions) = strategy.on_event(context) {
            for action in actions.as_slice() {
                if let Action::Place(order) = action {
                    sim.submit(order, timestamp_ms);
                }
            }
        }
        fills += sim.drain_fills().len();
    }
    fills
}

fn instantiate_strategies(
    registrations: &[StrategyRegistration],
    run: &RunId,
) -> Result<Vec<StrategyInstance>, StartError> {
    let mut strategies = Vec::new();
    for registration in registrations {
        let strategy =
            registration
                .factory()
                .create()
                .map_err(|source| StartError::StrategyInit {
                    run: run.clone(),
                    source,
                })?;
        strategies.push((registration.market().clone(), strategy));
    }
    Ok(strategies)
}

fn drain_paper_fills(rx: &mut tokio::sync::mpsc::Receiver<MarketEvent>) -> usize {
    let mut fills = 0;
    while rx.try_recv().is_ok() {
        fills += 1;
    }
    fills
}

async fn drive_paper(run: &PaperRun) -> Result<PaperReport, StartError> {
    let mut strategies = instantiate_strategies(run.strategies(), run.id())?;

    let (fill_tx, mut fill_rx) = tokio::sync::mpsc::channel(1024);
    let paper = PaperExecutor::new(fill_tx, "paper", MarketCategory::Crypto);

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1024);
    let mut subscribed = HashSet::new();
    for (market, _) in &strategies {
        if !subscribed.insert(market.clone()) {
            continue;
        }
        for outcome in [Outcome::Up, Outcome::Down] {
            let source = run.market_data().clone();
            let sink = event_tx.clone();
            let market = market.clone();
            tokio::spawn(async move { source.subscribe(market, outcome, sink).await });
        }
    }
    drop(event_tx);

    // ponytail: fee category fixed to Crypto, positions untracked, fill buffer bounded at 1024.
    let positions = Vec::new();
    let mut events_processed = 0_usize;
    let mut fills = 0_usize;

    while let Some(event) = event_rx.recv().await {
        events_processed += 1;
        if let MarketEvent::BookUpdate {
            market,
            outcome,
            bids,
            asks,
            timestamp_ms,
        } = &event
        {
            let book = OrderBookL2 {
                bids: bids.clone(),
                asks: asks.clone(),
                timestamp_ms: *timestamp_ms,
                last_trade_price: None,
            };
            let _ = paper.update_book(market, *outcome, book.clone()).await;
            fills += drain_paper_fills(&mut fill_rx);
            for (registered_market, strategy) in &mut *strategies {
                if *registered_market != *market {
                    continue;
                }
                let context = StrategyContext {
                    market,
                    book: &book,
                    positions: &positions,
                    now: LogicalTimestamp::from_millis(*timestamp_ms),
                };
                if let Ok(actions) = strategy.on_event(context) {
                    for action in actions.as_slice() {
                        if let Action::Place(order) = action {
                            let _ = paper.submit(order, *timestamp_ms).await;
                        }
                    }
                }
                fills += drain_paper_fills(&mut fill_rx);
            }
        }
    }
    fills += drain_paper_fills(&mut fill_rx);

    Ok(PaperReport {
        run: run.id().clone(),
        events_processed,
        fills,
    })
}

#[must_use]
fn passes_risk(order: &PlaceOrder, limits: &RiskLimits) -> bool {
    order.qty * order.price <= limits.max_order_notional.as_decimal()
}

async fn drive_live(run: &LiveRun) -> Result<LiveReport, StartError> {
    let mut strategies = instantiate_strategies(run.strategies(), run.id())?;
    let executor = run.executor().clone();
    let limits = run.risk().clone();

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1024);
    let mut subscribed = HashSet::new();
    for (market, _) in &strategies {
        if !subscribed.insert(market.clone()) {
            continue;
        }
        for outcome in [Outcome::Up, Outcome::Down] {
            let source = run.market_data().clone();
            let sink = event_tx.clone();
            let market = market.clone();
            tokio::spawn(async move { source.subscribe(market, outcome, sink).await });
        }
    }
    drop(event_tx);

    // ponytail: v0 risk gate checks only per-order notional; positions,
    // loss/open-order limits, reconciliation, and tape are not yet enforced.
    let positions = Vec::new();
    let mut events_processed = 0_usize;
    let mut fills = 0_usize;
    let mut rejected = 0_usize;

    while let Some(event) = event_rx.recv().await {
        events_processed += 1;
        match &event {
            MarketEvent::BookUpdate {
                market,
                bids,
                asks,
                timestamp_ms,
                ..
            } => {
                let book = OrderBookL2 {
                    bids: bids.clone(),
                    asks: asks.clone(),
                    timestamp_ms: *timestamp_ms,
                    last_trade_price: None,
                };
                for (registered_market, strategy) in &mut *strategies {
                    if *registered_market != *market {
                        continue;
                    }
                    let context = StrategyContext {
                        market,
                        book: &book,
                        positions: &positions,
                        now: LogicalTimestamp::from_millis(*timestamp_ms),
                    };
                    if let Ok(actions) = strategy.on_event(context) {
                        for action in actions.as_slice() {
                            if let Action::Place(order) = action {
                                if passes_risk(order, &limits) {
                                    let _ = executor.submit(order, *timestamp_ms).await;
                                } else {
                                    rejected += 1;
                                }
                            }
                        }
                    }
                }
            }
            MarketEvent::Fill { .. } => {
                fills += 1;
            }
            _ => {}
        }
    }

    Ok(LiveReport {
        run: run.id().clone(),
        events_processed,
        fills,
        rejected,
    })
}

#[cfg(test)]
mod tests {
    use super::{Pmkit, RunReport, StartError};
    use async_trait::async_trait;
    use pmkit_book::Side;
    use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
    use pmkit_data::{DataSourceError, HistoricalDataSource, LiveDataSource, ReplayQuery};
    use pmkit_event::{Liquidity, MarketEvent};
    use pmkit_exec::{ExecError, Executor, OrderId, PlaceOrder};
    use pmkit_market::Outcome;
    use pmkit_money::Money;
    use pmkit_run::{EvidenceRequirement, RetrievalWait};
    use pmkit_runtime::{RiskLimits, RuntimeConfig, ShutdownConfig, StrategyRegistration};
    use pmkit_spec::{BacktestRun, ConservativeV1Config, LiveRun, PaperRun, ReplaySpec};
    use pmkit_strategy::{
        Actions, Strategy, StrategyContext, StrategyError, StrategyFactory, StrategyInitError,
    };
    use rust_decimal::Decimal;
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc::Sender;

    struct ScriptedHistory {
        ticks: Vec<i64>,
    }

    #[async_trait]
    impl HistoricalDataSource for ScriptedHistory {
        async fn replay(
            &self,
            _query: ReplayQuery,
            sink: Sender<MarketEvent>,
        ) -> Result<(), DataSourceError> {
            let market = MarketId::new("btc-5m").map_err(|_| DataSourceError::NotAvailable)?;
            for &timestamp_ms in &self.ticks {
                let event = MarketEvent::BookUpdate {
                    market: market.clone(),
                    outcome: Outcome::Up,
                    bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
                    asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
                    timestamp_ms,
                };
                sink.send(event)
                    .await
                    .map_err(|_| DataSourceError::SinkClosed)?;
            }
            Ok(())
        }
    }

    struct BuyOnce {
        placed: bool,
    }

    impl Strategy for BuyOnce {
        fn on_event(&mut self, ctx: StrategyContext<'_>) -> Result<Actions, StrategyError> {
            if self.placed {
                return Ok(Actions::none());
            }
            self.placed = true;
            Ok(Actions::place(PlaceOrder {
                market: ctx.market.clone(),
                outcome: Outcome::Up,
                side: Side::Buy,
                price: Decimal::new(50, 2),
                qty: Decimal::from(10),
                post_only: false,
            }))
        }
    }

    struct BuyFactory;

    impl StrategyFactory for BuyFactory {
        fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
            Ok(Box::new(BuyOnce { placed: false }))
        }
    }

    fn config() -> Result<RuntimeConfig, Box<dyn std::error::Error>> {
        Ok(RuntimeConfig {
            backtest_concurrency: NonZeroUsize::new(1).ok_or("nonzero")?,
            startup_timeout: Duration::from_secs(30),
            shutdown: ShutdownConfig {
                live_orders: pmkit_runtime::LiveOrderPolicy::CancelOwned,
                reconciliation_timeout: Duration::from_secs(30),
                tape_flush_timeout: Duration::from_secs(10),
            },
            manifest_dir: "./runs".into(),
        })
    }

    fn risk() -> Result<RiskLimits, Box<dyn std::error::Error>> {
        Ok(RiskLimits {
            max_order_notional: Money::usdc(100),
            max_position_notional: Money::usdc(1_000),
            max_open_orders: NonZeroU32::new(10).ok_or("nonzero")?,
            max_loss: Money::usdc(500),
        })
    }

    #[tokio::test]
    async fn backtest_drives_replay_through_strategy_to_fill()
    -> Result<(), Box<dyn std::error::Error>> {
        let replay = ReplaySpec::new(
            Arc::new(ScriptedHistory { ticks: vec![1, 2] }),
            "2026-01-01T00:00:00Z".parse()?,
            "2026-02-01T00:00:00Z".parse()?,
            EvidenceRequirement::CorroboratedOnly,
            RetrievalWait::ReturnPending,
        );
        let run = BacktestRun::new(
            RunId::new("bt")?,
            PortfolioId::new("research")?,
            replay,
            Money::usdc(1_000),
            risk()?,
            ConservativeV1Config {
                activation_latency: Duration::ZERO,
            },
        )
        .strategy(StrategyRegistration::new(
            StrategyId::new("buyer")?,
            MarketId::new("btc-5m")?,
            Arc::new(BuyFactory),
        ));

        let app = Pmkit::builder(config()?).run(run).start().await?;

        let report = app.report(&RunId::new("bt")?).ok_or("missing report")?;
        let RunReport::Backtest(backtest) = report else {
            return Err("expected a backtest report".into());
        };
        assert_eq!(backtest.events_processed, 2);
        assert!(
            backtest.fills >= 1,
            "the taker buy should fill against the ask"
        );
        Ok(())
    }

    struct ScriptedLive;

    #[async_trait]
    impl LiveDataSource for ScriptedLive {
        async fn subscribe(
            &self,
            market: MarketId,
            outcome: Outcome,
            sink: Sender<MarketEvent>,
        ) -> Result<(), DataSourceError> {
            if outcome == Outcome::Up {
                sink.send(MarketEvent::BookUpdate {
                    market,
                    outcome,
                    bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
                    asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
                    timestamp_ms: 1,
                })
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn paper_run_drives_live_feed_to_fill() -> Result<(), Box<dyn std::error::Error>> {
        let run = PaperRun::new(
            RunId::new("paper")?,
            PortfolioId::new("alice")?,
            Money::usdc(10_000),
            risk()?,
            Arc::new(ScriptedLive),
            ConservativeV1Config {
                activation_latency: Duration::ZERO,
            },
        )
        .strategy(StrategyRegistration::new(
            StrategyId::new("buyer")?,
            MarketId::new("btc-5m")?,
            Arc::new(BuyFactory),
        ));

        let app = Pmkit::builder(config()?).run(run).start().await?;
        let report = app.report(&RunId::new("paper")?).ok_or("missing report")?;
        let RunReport::Paper(paper) = report else {
            return Err("expected a paper report".into());
        };
        assert_eq!(paper.events_processed, 1);
        assert!(
            paper.fills >= 1,
            "the taker buy should fill against the ask"
        );
        Ok(())
    }

    struct RecordingExec;

    #[async_trait]
    impl Executor for RecordingExec {
        async fn submit(&self, _order: &PlaceOrder, _now_ms: i64) -> Result<OrderId, ExecError> {
            Ok(OrderId("live-1".to_owned()))
        }

        async fn cancel(&self, _order_id: &OrderId) -> Result<(), ExecError> {
            Ok(())
        }

        async fn cancel_all(&self) -> Result<(), ExecError> {
            Ok(())
        }
    }

    struct LiveWithFill;

    #[async_trait]
    impl LiveDataSource for LiveWithFill {
        async fn subscribe(
            &self,
            market: MarketId,
            outcome: Outcome,
            sink: Sender<MarketEvent>,
        ) -> Result<(), DataSourceError> {
            if outcome == Outcome::Up {
                sink.send(MarketEvent::BookUpdate {
                    market: market.clone(),
                    outcome,
                    bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
                    asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
                    timestamp_ms: 1,
                })
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
                sink.send(MarketEvent::Fill {
                    strategy: None,
                    order_id: "venue-1".to_owned(),
                    market,
                    outcome,
                    price: Decimal::new(46, 2),
                    size: Decimal::from(10),
                    side: Side::Buy,
                    fee: Decimal::ZERO,
                    liquidity: Liquidity::Taker,
                    timestamp_ms: 2,
                })
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            }
            Ok(())
        }
    }

    fn live_run() -> Result<LiveRun, Box<dyn std::error::Error>> {
        Ok(LiveRun::new(
            RunId::new("live")?,
            PortfolioId::new("alice")?,
            Arc::new(RecordingExec),
            Arc::new(LiveWithFill),
            risk()?,
        )
        .strategy(StrategyRegistration::new(
            StrategyId::new("buyer")?,
            MarketId::new("btc-5m")?,
            Arc::new(BuyFactory),
        )))
    }

    #[tokio::test]
    async fn live_run_routes_orders_and_counts_fills() -> Result<(), Box<dyn std::error::Error>> {
        let report = super::drive_live(&live_run()?).await?;
        assert_eq!(report.events_processed, 2);
        assert_eq!(report.fills, 1);
        assert_eq!(report.rejected, 0);
        Ok(())
    }

    #[tokio::test]
    async fn live_run_without_consent_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let result = Pmkit::builder(config()?).run(live_run()?).start().await;
        assert!(matches!(result, Err(StartError::LiveConsentMissing(_))));
        Ok(())
    }
}
