use crate::{
    AppHandle, Cancellation, FeedHealthSnapshot, Pmkit, RunLifecycleEvent, RunMetricsSnapshot,
    RunReport, RuntimeError, StartError,
    test_support::{BuyFactory, config, risk},
};
use async_trait::async_trait;
use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
use pmkit_data::{DataSourceError, HistoricalDataSource, ReplayQuery, SourceSignal};
use pmkit_event::{
    CexReferenceEnvelope, CexReferenceEvent, MarketEvent, PmMarketEnvelope, SourceEnvelope,
    StreamMetadata,
};
use pmkit_market::{Asset, Exchange, Outcome};
use pmkit_money::Money;
use pmkit_run::{EvidenceRequirement, RetrievalWait};
use pmkit_runtime::StrategyRegistration;
use pmkit_spec::{BacktestRun, ConservativeV1Config, ReplaySpec};
use pmkit_store::{OwnerScope, TapeStore, TursoTapeStore};
use pmkit_strategy::{
    Actions, Strategy, StrategyContext, StrategyError, StrategyFactory, StrategyInitError,
};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::Sender;

struct ScriptedHistory {
    ticks: Vec<i64>,
}

struct ScriptedReference;

struct TwoMarketHistory;

struct StaleMarkHistory;

struct FailingHistory;

struct Taker;

struct TakerFactory;

struct PositionProbe(Arc<Mutex<Vec<usize>>>);

struct PositionProbeFactory(Arc<Mutex<Vec<usize>>>);

impl Strategy for Taker {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        let Some((price, _)) = context.book.best_ask() else {
            return Ok(Actions::none());
        };
        Ok(Actions::place(pmkit_exec::PlaceOrder {
            market: context.market.clone(),
            outcome: Outcome::Up,
            side: pmkit_book::Side::Buy,
            price,
            qty: Decimal::ONE,
            post_only: false,
            tif: pmkit_exec::TimeInForce::Gtc,
        }))
    }
}

impl StrategyFactory for TakerFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(Taker))
    }
}

impl Strategy for PositionProbe {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        self.0
            .lock()
            .map_err(|_| StrategyError {
                message: "position probe lock poisoned".into(),
            })?
            .push(context.positions.len());
        Ok(Actions::none())
    }
}

impl StrategyFactory for PositionProbeFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(PositionProbe(Arc::clone(&self.0))))
    }
}

#[async_trait]
impl HistoricalDataSource for ScriptedReference {
    async fn replay(
        &self,
        _query: ReplayQuery,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        sink.send(SourceSignal::Data(Box::new(SourceEnvelope::CexReference(
            CexReferenceEnvelope {
                metadata: StreamMetadata {
                    schema_version: 1,
                    source_id: "binance-history".into(),
                    source_time_ms: 1,
                    canonical_source_rank: 1,
                    receipt_time_ms: 1,
                    connection_id: "archive".into(),
                    connection_epoch: 0,
                    frame_sequence: 7,
                    ingest_sequence: 7,
                },
                fact: CexReferenceEvent::Trade {
                    asset: Asset::Btc,
                    exchange: Exchange::Binance,
                    aggregate_trade_id: 7,
                    price: Decimal::new(42, 2),
                    qty: Decimal::from(2),
                    is_buyer_maker: false,
                    timestamp_ms: 1,
                },
            },
        ))))
        .await
        .map_err(|_| DataSourceError::SinkClosed)?;
        sink.send(SourceSignal::Watermark(i64::MAX))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        sink.send(SourceSignal::Eof)
            .await
            .map_err(|_| DataSourceError::SinkClosed)
    }
}

#[async_trait]
impl HistoricalDataSource for TwoMarketHistory {
    async fn replay(
        &self,
        _query: ReplayQuery,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        let first = MarketId::new("first-5m").map_err(|_| DataSourceError::NotAvailable)?;
        let second = MarketId::new("second-5m").map_err(|_| DataSourceError::NotAvailable)?;
        for (market, timestamp_ms) in [(first.clone(), 1), (first, 2), (second, 3)] {
            sink.send(SourceSignal::market_event(MarketEvent::BookUpdate {
                market,
                outcome: Outcome::Up,
                bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
                asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
                timestamp_ms,
            }))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        }
        sink.send(SourceSignal::Watermark(i64::MAX))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        sink.send(SourceSignal::Eof)
            .await
            .map_err(|_| DataSourceError::SinkClosed)
    }
}

#[async_trait]
impl HistoricalDataSource for StaleMarkHistory {
    async fn replay(
        &self,
        _query: ReplayQuery,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        let market = MarketId::new("btc-5m").map_err(|_| DataSourceError::NotAvailable)?;
        for (bids, asks, timestamp_ms) in [
            (
                vec![(Decimal::new(44, 2), Decimal::from(50))],
                vec![(Decimal::new(46, 2), Decimal::from(50))],
                1,
            ),
            (Vec::new(), Vec::new(), 2),
        ] {
            sink.send(SourceSignal::market_event(MarketEvent::BookUpdate {
                market: market.clone(),
                outcome: Outcome::Up,
                bids,
                asks,
                timestamp_ms,
            }))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        }
        sink.send(SourceSignal::Watermark(i64::MAX))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        sink.send(SourceSignal::Eof)
            .await
            .map_err(|_| DataSourceError::SinkClosed)
    }
}

#[async_trait]
impl HistoricalDataSource for FailingHistory {
    async fn replay(
        &self,
        _query: ReplayQuery,
        _sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        Err(DataSourceError::ReplayGap {
            message: "missing test replay segment".to_owned(),
        })
    }
}

#[async_trait]
impl HistoricalDataSource for ScriptedHistory {
    async fn replay(
        &self,
        _query: ReplayQuery,
        sink: Sender<SourceSignal>,
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
            sink.send(SourceSignal::market_event(event))
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
        }
        sink.send(SourceSignal::Watermark(i64::MAX))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        sink.send(SourceSignal::Eof)
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        Ok(())
    }
}

fn backtest_run() -> Result<BacktestRun, Box<dyn std::error::Error>> {
    let replay = ReplaySpec::new(
        Arc::new(ScriptedHistory { ticks: vec![1, 2] }),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    )
    .reference_source(Arc::new(ScriptedReference));
    let run = BacktestRun::new(
        RunId::new("bt")?,
        PortfolioId::new("research")?,
        replay,
        Money::usdc(1_000),
        risk()?,
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));
    Ok(run)
}

async fn backtest_app() -> Result<AppHandle, Box<dyn std::error::Error>> {
    let run = backtest_run()?;
    let runtime = config()?;
    let app = Pmkit::builder(runtime).run(run).start().await?;
    Ok(app)
}

#[tokio::test]
async fn backtest_drives_replay_through_strategy_to_fill() -> Result<(), Box<dyn std::error::Error>>
{
    let app = backtest_app().await?;

    let report = app.wait_for(RunId::new("bt")?).await?;
    let RunReport::Backtest(backtest) = report else {
        return Err("expected a backtest report".into());
    };
    assert_eq!(backtest.events_processed, 2);
    assert!(
        backtest.fills >= 1,
        "the taker buy should fill against the ask"
    );
    assert!(backtest.exposure.portfolio_notional > Decimal::ZERO);
    let manifest = app.manifest(&RunId::new("bt")?).ok_or("missing manifest")?;
    assert_eq!(manifest["mode"], "backtest");
    assert_eq!(manifest["run"], "bt");
    Ok(())
}

#[tokio::test]
async fn metrics_match_report() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a completed backtest with two PM book events.
    let app = backtest_app().await?;
    let run = RunId::new("bt")?;
    let RunReport::Backtest(report) = app.wait_for(run.clone()).await? else {
        return Err("expected a backtest report".into());
    };

    // When: metrics are read from the public application handle.
    let metrics = app.metrics(&run).ok_or("missing run metrics")?;

    // Then: the snapshot agrees with the terminal report and owns no unrelated data.
    assert_eq!(metrics.run, run);
    assert_eq!(metrics.events_processed, report.events_processed);
    assert_eq!(metrics.fills, report.fills);
    assert_eq!(metrics.rejected, 0);
    assert_eq!(metrics.reconnects, 0);
    assert_eq!(metrics.decisions, 2);
    assert_eq!(report.metrics, *metrics);
    Ok(())
}

#[tokio::test]
async fn feed_gap_counted_and_still_aborts() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a replay source that fails before producing an event.
    let replay = ReplaySpec::new(
        Arc::new(FailingHistory),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    );
    let run = BacktestRun::new(
        RunId::new("metrics-failure")?,
        PortfolioId::new("research")?,
        replay,
        Money::usdc(1_000),
        risk()?,
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
        },
    );

    // When: the application start fails through its public boundary.
    let error = Pmkit::builder(config()?)
        .run(run)
        .start()
        .await
        .err()
        .ok_or("failing replay unexpectedly completed")?;

    // Then: the typed, run-scoped diagnostics remain available without storage internals.
    let metrics: RunMetricsSnapshot = error.diagnostics().ok_or("missing diagnostics")?.clone();
    println!("feed gap diagnostics: {metrics:?}");
    assert_eq!(metrics.run, RunId::new("metrics-failure")?);
    assert_eq!(metrics.events_processed, 0);
    assert_eq!(metrics.fills, 0);
    assert_eq!(metrics.rejected, 0);
    assert_eq!(metrics.reconnects, 0);
    assert_eq!(metrics.decisions, 0);
    assert_eq!(
        metrics.feed_health,
        vec![FeedHealthSnapshot {
            source: "pm".to_owned(),
            last_event_timestamp_ms: None,
            watermark_ms: None,
            logical_lag_ms: None,
            gap_count: 1,
        }]
    );
    assert!(matches!(
        error,
        StartError::RunFailed { source, .. }
            if matches!(
                source.as_ref(),
                StartError::Source {
                    source: DataSourceError::ReplayGap { .. },
                    ..
                }
            )
    ));
    Ok(())
}

#[test]
fn reconnects_follow_connection_epoch() {
    let source = |outcome, connection_epoch| {
        SourceEnvelope::PmMarket(PmMarketEnvelope {
            metadata: StreamMetadata {
                schema_version: 1,
                source_id: "polymarket-live".into(),
                source_time_ms: 1,
                canonical_source_rank: 0,
                receipt_time_ms: 1,
                connection_id: format!("connection-{connection_epoch}"),
                connection_epoch,
                frame_sequence: 1,
                ingest_sequence: 1,
            },
            raw_frame: Vec::new(),
            fact: MarketEvent::BookUpdate {
                market: MarketId::new("btc-5m").unwrap_or_else(|_| unreachable!()),
                outcome,
                bids: Vec::new(),
                asks: Vec::new(),
                timestamp_ms: 1,
            },
        })
    };
    let mut epochs = HashMap::new();

    assert!(!crate::observe_reconnect(
        &source(Outcome::Up, 0),
        &mut epochs
    ));
    assert!(!crate::observe_reconnect(
        &source(Outcome::Down, 1),
        &mut epochs
    ));
    assert!(crate::observe_reconnect(
        &source(Outcome::Up, 1),
        &mut epochs
    ));
    assert!(!crate::observe_reconnect(
        &source(Outcome::Up, 0),
        &mut epochs
    ));
    assert!(!crate::observe_reconnect(
        &source(Outcome::Up, 1),
        &mut epochs
    ));
}

#[tokio::test]
async fn metrics_count_each_strategy_evaluation() -> Result<(), Box<dyn std::error::Error>> {
    let mut run = backtest_run()?;
    run = run.strategy(StrategyRegistration::new(
        StrategyId::new("second-buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));
    let app = Pmkit::builder(config()?).run(run).start().await?;
    let metrics = app.metrics(&RunId::new("bt")?).ok_or("missing metrics")?;

    assert_eq!(metrics.decisions, 4);
    Ok(())
}

#[tokio::test]
async fn backtest_clears_exposure_when_book_loses_its_mark()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a filled position followed by an unmarkable book for the same outcome.
    let replay = ReplaySpec::new(
        Arc::new(StaleMarkHistory),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    );
    let run = BacktestRun::new(
        RunId::new("bt-stale-mark")?,
        PortfolioId::new("research")?,
        replay,
        Money::usdc(1_000),
        risk()?,
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));

    // When: the replay completes.
    let app = Pmkit::builder(config()?).run(run).start().await?;
    let RunReport::Backtest(report) = app.wait_for(RunId::new("bt-stale-mark")?).await? else {
        return Err("expected a backtest report".into());
    };

    // Then: the obsolete mid-price cannot survive in reported exposure.
    assert_eq!(report.exposure.portfolio_notional, Decimal::ZERO);
    Ok(())
}

#[tokio::test]
async fn two_market_positions_isolated() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a first market that fills before a second market's strategy runs.
    let observed_positions = Arc::new(Mutex::new(Vec::new()));
    let replay = ReplaySpec::new(
        Arc::new(TwoMarketHistory),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    );
    let run = BacktestRun::new(
        RunId::new("two-market")?,
        PortfolioId::new("research")?,
        replay,
        Money::usdc(1_000),
        risk()?,
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("first-taker")?,
        MarketId::new("first-5m")?,
        Arc::new(TakerFactory),
    ))
    .strategy(StrategyRegistration::new(
        StrategyId::new("second-probe")?,
        MarketId::new("second-5m")?,
        Arc::new(PositionProbeFactory(Arc::clone(&observed_positions))),
    ));

    // When: the backtest drives both markets through their registered strategies.
    Pmkit::builder(config()?).run(run).start().await?;

    // Then: the second market's strategy cannot observe the first market's fill.
    let observed_positions = observed_positions
        .lock()
        .map_err(|_| "position probe lock poisoned")?;
    assert_eq!(*observed_positions, [0]);
    drop(observed_positions);
    Ok(())
}

#[tokio::test]
async fn wait_for_is_repeatable() -> Result<(), Box<dyn std::error::Error>> {
    let app = backtest_app().await?;

    let first = app.wait_for(RunId::new("bt")?).await?;
    let second = app.wait_for(RunId::new("bt")?).await?;

    let (RunReport::Backtest(first), RunReport::Backtest(second)) = (first, second) else {
        return Err("expected backtest reports".into());
    };
    assert_eq!(first.events_processed, second.events_processed);
    assert_eq!(first.fills, second.fills);
    Ok(())
}

#[tokio::test]
async fn wait_for_rejects_unknown_run() -> Result<(), Box<dyn std::error::Error>> {
    let app = backtest_app().await?;
    let unknown = RunId::new("missing")?;

    let result = app.wait_for(unknown.clone()).await;

    assert!(matches!(result, Err(RuntimeError::UnknownRun(run)) if run == unknown));
    Ok(())
}

#[tokio::test]
async fn backtest_records_one_decision_per_book_event() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a store-backed backtest over two scripted book events.
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("pmkit-bt-decisions.db");
    let store = Arc::new(TursoTapeStore::open_local(&path).await?);
    let replay = ReplaySpec::new(
        Arc::new(ScriptedHistory { ticks: vec![1, 2] }),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    )
    .reference_source(Arc::new(ScriptedReference));
    let run = BacktestRun::new(
        RunId::new("bt-rec")?,
        PortfolioId::new("research")?,
        replay,
        Money::usdc(1_000),
        risk()?,
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));

    // When: the engine drives the backtest with durable storage configured.
    Pmkit::builder(config()?)
        .storage(store.clone())
        .run(run)
        .start()
        .await?;

    // Then: exactly one causal decision is recorded per book event, owner-scoped.
    let scope = OwnerScope::new(PortfolioId::new("research")?, RunId::new("bt-rec")?);
    let decisions = store.read_decisions(&scope).await?;
    assert_eq!(decisions.len(), 2);
    assert!(
        decisions.iter().any(|decision| {
            decision.payload["snapshot"]["cex_trade"]["volume"] == "2"
                && decision.payload["snapshot"]["cex_trade"]["cvd"] == "2"
        }),
        "decisions: {decisions:?}"
    );
    drop(store);
    let _ = std::fs::remove_file(&path);
    Ok(())
}

#[tokio::test]
async fn cancellation_before_start_stops_the_run() -> Result<(), Box<dyn std::error::Error>> {
    let cancel = Cancellation::new();
    cancel.cancel();
    let app = Pmkit::builder(config()?)
        .cancellation(cancel)
        .run(backtest_run()?)
        .start()
        .await?;

    let RunReport::Backtest(report) = app.wait_for(RunId::new("bt")?).await? else {
        return Err("expected a backtest report".into());
    };
    // The run was cancelled before consuming any event.
    assert_eq!(report.events_processed, 0);
    Ok(())
}

#[tokio::test]
async fn lifecycle_events_are_published() -> Result<(), Box<dyn std::error::Error>> {
    let (subscriber, mut events) = tokio::sync::mpsc::unbounded_channel();
    Pmkit::builder(config()?)
        .subscribe(subscriber)
        .run(backtest_run()?)
        .start()
        .await?;

    let mut observed = Vec::new();
    while let Ok(event) = events.try_recv() {
        observed.push(event);
    }
    let run = RunId::new("bt")?;
    assert_eq!(
        observed,
        vec![
            RunLifecycleEvent::Started { run: run.clone() },
            RunLifecycleEvent::Completed { run },
        ]
    );
    Ok(())
}
