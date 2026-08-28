use crate::{
    Cancellation, Pmkit, RunControl, RunLifecycleEvent, RunReport, StartError,
    test_support::{BuyFactory, config, risk},
};
use async_trait::async_trait;
use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
use pmkit_data::{
    DataSourceError, LiveAccountDataSource, LiveCexDataSource, LiveDataSource, SourceSignal,
};
use pmkit_event::{
    CexReferenceEnvelope, CexReferenceEvent, MarketEvent, PmAccountEnvelope, PmAccountEvent,
    SourceEnvelope, StrategyFact, StreamMetadata,
};
use pmkit_market::{Asset, Exchange, Outcome};
use pmkit_money::Money;
use pmkit_runtime::StrategyRegistration;
use pmkit_spec::{ConservativeV1Config, PaperRun};
use pmkit_store::{OwnerScope, TapeStore, TursoTapeStore};
use pmkit_strategy::{
    Actions, Strategy, StrategyContext, StrategyError, StrategyFactory, StrategyInitError,
};
use rust_decimal::Decimal;
use std::num::NonZeroUsize;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::mpsc::Sender;

struct ScriptedLive;

struct IdleLive;

struct StaleMarkLive;

struct FailingLive;

struct MismatchedAccountSource;

struct FiniteReferenceSource {
    events: Vec<CexReferenceEvent>,
}

struct RecordingStrategy {
    facts: Arc<Mutex<Vec<StrategyFact>>>,
    nonempty_reference_books: Arc<AtomicUsize>,
}

struct RecordingFactory {
    facts: Arc<Mutex<Vec<StrategyFact>>>,
    nonempty_reference_books: Arc<AtomicUsize>,
}

struct FailingStrategy(Arc<AtomicUsize>);

struct FailingFactory(Arc<AtomicUsize>);

struct RepeatBuy;

struct RepeatBuyFactory;

struct MismatchedMarket;
struct MismatchedMarketFactory;

fn record_fact(facts: &Mutex<Vec<StrategyFact>>, fact: &StrategyFact) {
    match facts.lock() {
        Ok(mut facts) => facts.push(fact.clone()),
        Err(poisoned) => poisoned.into_inner().push(fact.clone()),
    }
}

fn reference_trade(aggregate_trade_id: u64, timestamp_ms: i64, price: i64) -> CexReferenceEvent {
    CexReferenceEvent::Trade {
        asset: Asset::Btc,
        exchange: Exchange::Binance,
        aggregate_trade_id,
        price: Decimal::from(price),
        qty: Decimal::ONE,
        is_buyer_maker: false,
        timestamp_ms,
    }
}

#[async_trait]
impl LiveCexDataSource for FiniteReferenceSource {
    async fn subscribe_reference(&self, sink: Sender<SourceSignal>) -> Result<(), DataSourceError> {
        for (index, fact) in self.events.iter().cloned().enumerate() {
            let timestamp_ms = match &fact {
                CexReferenceEvent::Trade { timestamp_ms, .. } => *timestamp_ms,
            };
            let sequence = i64::try_from(index).unwrap_or(i64::MAX);
            sink.send(SourceSignal::Data(Box::new(SourceEnvelope::CexReference(
                CexReferenceEnvelope {
                    metadata: StreamMetadata {
                        schema_version: 1,
                        source_id: "test-cex".into(),
                        source_time_ms: timestamp_ms,
                        canonical_source_rank: 1,
                        receipt_time_ms: timestamp_ms,
                        connection_id: "test-cex".into(),
                        connection_epoch: 0,
                        frame_sequence: sequence,
                        ingest_sequence: u64::try_from(index).unwrap_or(u64::MAX),
                    },
                    fact,
                },
            ))))
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

impl Strategy for RecordingStrategy {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        if matches!(context.fact, StrategyFact::Reference(_))
            && (!context.book.bids.is_empty()
                || !context.book.asks.is_empty()
                || context.book.last_trade_price.is_some()
                || context.book.timestamp_ms != 0)
        {
            self.nonempty_reference_books
                .fetch_add(1, Ordering::Relaxed);
        }
        record_fact(&self.facts, context.fact);
        Ok(Actions::none())
    }
}

impl StrategyFactory for RecordingFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(RecordingStrategy {
            facts: Arc::clone(&self.facts),
            nonempty_reference_books: Arc::clone(&self.nonempty_reference_books),
        }))
    }
}

impl Strategy for FailingStrategy {
    fn on_event(&mut self, _context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(StrategyError {
            message: "injected paper strategy failure".into(),
        })
    }
}

impl StrategyFactory for FailingFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(FailingStrategy(Arc::clone(&self.0))))
    }
}

impl Strategy for RepeatBuy {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        Ok(Actions::place(pmkit_exec::PlaceOrder {
            market: context.market.clone(),
            outcome: Outcome::Up,
            side: pmkit_book::Side::Buy,
            price: Decimal::new(50, 2),
            qty: Decimal::from(10),
            post_only: false,
            tif: pmkit_exec::TimeInForce::Gtc,
        }))
    }
}

impl StrategyFactory for RepeatBuyFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(RepeatBuy))
    }
}

impl Strategy for MismatchedMarket {
    fn on_event(&mut self, _context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        Ok(Actions::place(pmkit_exec::PlaceOrder {
            market: MarketId::new("other-market").map_err(|error| StrategyError {
                message: error.to_string(),
            })?,
            outcome: Outcome::Up,
            side: pmkit_book::Side::Buy,
            price: Decimal::new(50, 2),
            qty: Decimal::ONE,
            post_only: false,
            tif: pmkit_exec::TimeInForce::Gtc,
        }))
    }
}

impl StrategyFactory for MismatchedMarketFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(MismatchedMarket))
    }
}

#[async_trait]
impl LiveAccountDataSource for MismatchedAccountSource {
    async fn subscribe_account(
        &self,
        _portfolio: PortfolioId,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        sink.send(SourceSignal::Data(Box::new(SourceEnvelope::PmAccount(
            PmAccountEnvelope {
                portfolio: PortfolioId::new("mallory").map_err(|error| {
                    DataSourceError::ReplayGap {
                        message: error.to_string(),
                    }
                })?,
                metadata: StreamMetadata {
                    schema_version: 4,
                    source_id: "mismatched-account".into(),
                    source_time_ms: 1,
                    canonical_source_rank: 0,
                    receipt_time_ms: 1,
                    connection_id: "mismatched-account".into(),
                    connection_epoch: 0,
                    frame_sequence: 1,
                    ingest_sequence: 1,
                },
                raw_frame: Vec::new(),
                fact: PmAccountEvent::OrderAck {
                    strategy: None,
                    order_id: "foreign-order".into(),
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
impl LiveDataSource for IdleLive {
    async fn subscribe(
        &self,
        _market: MarketId,
        _outcome: Outcome,
        _sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        std::future::pending::<Result<(), DataSourceError>>().await
    }
}

#[async_trait]
impl LiveDataSource for ScriptedLive {
    async fn subscribe(
        &self,
        market: MarketId,
        outcome: Outcome,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        if outcome == Outcome::Up {
            sink.send(SourceSignal::market_event(MarketEvent::BookUpdate {
                market,
                outcome,
                bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
                asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
                timestamp_ms: 1,
            }))
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

#[async_trait]
impl LiveDataSource for StaleMarkLive {
    async fn subscribe(
        &self,
        market: MarketId,
        outcome: Outcome,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        if outcome == Outcome::Up {
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
                    outcome,
                    bids,
                    asks,
                    timestamp_ms,
                }))
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
            }
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

#[async_trait]
impl LiveDataSource for FailingLive {
    async fn subscribe(
        &self,
        _market: MarketId,
        _outcome: Outcome,
        _sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        Err(DataSourceError::NotAvailable)
    }
}

#[tokio::test]
async fn duplicate_strategy_ids_fail_before_factory_or_feed_start()
-> Result<(), Box<dyn std::error::Error>> {
    let market = MarketId::new("btc-5m")?;
    let run = PaperRun::new(
        RunId::new("paper-duplicate-strategy")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        risk()?,
        Arc::new(ScriptedLive),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("duplicate")?,
        market.clone(),
        Arc::new(BuyFactory),
    ))
    .strategy(StrategyRegistration::new(
        StrategyId::new("duplicate")?,
        market,
        Arc::new(BuyFactory),
    ));

    let Err(error) = Pmkit::builder(config()?).run(run).start().await else {
        return Err("duplicate id should fail".into());
    };
    assert!(matches!(
        error,
        StartError::RunFailed { source, .. }
            if matches!(source.as_ref(), StartError::StrategyInit { source, .. } if source.message.contains("duplicate strategy id"))
    ));
    Ok(())
}

#[tokio::test]
async fn paper_rejects_market_mismatch_without_fill() -> Result<(), Box<dyn std::error::Error>> {
    let run = PaperRun::new(
        RunId::new("paper-market-mismatch")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        risk()?,
        Arc::new(ScriptedLive),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("mismatch")?,
        MarketId::new("btc-5m")?,
        Arc::new(MismatchedMarketFactory),
    ));
    let app = Pmkit::builder(config()?).run(run).start().await?;
    let RunReport::Paper(report) = app
        .report(&RunId::new("paper-market-mismatch")?)
        .ok_or("missing report")?
    else {
        return Err("expected paper report".into());
    };
    assert_eq!(report.metrics.rejected, 1);
    assert_eq!(report.fills, 0);
    Ok(())
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
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
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
    assert!(paper.exposure.portfolio_notional > Decimal::ZERO);
    Ok(())
}

#[tokio::test]
async fn paper_run_delivers_reference_facts_once_in_causal_order()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a finite reference stream, one failing strategy, and one recorder.
    let market = MarketId::new("btc-5m")?;
    let failing_calls = Arc::new(AtomicUsize::new(0));
    let recording_facts = Arc::new(Mutex::new(Vec::new()));
    let nonempty_reference_books = Arc::new(AtomicUsize::new(0));
    let reference = FiniteReferenceSource {
        events: vec![
            reference_trade(3, 4, 103),
            reference_trade(1, 2, 101),
            reference_trade(2, 3, 102),
        ],
    };
    let run = PaperRun::new(
        RunId::new("paper-reference")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        risk()?,
        Arc::new(ScriptedLive),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .reference_data(Arc::new(reference))
    .strategy(StrategyRegistration::new(
        StrategyId::new("failing")?,
        market.clone(),
        Arc::new(FailingFactory(Arc::clone(&failing_calls))),
    ))
    .strategy(StrategyRegistration::new(
        StrategyId::new("recording")?,
        market.clone(),
        Arc::new(RecordingFactory {
            facts: Arc::clone(&recording_facts),
            nonempty_reference_books: Arc::clone(&nonempty_reference_books),
        }),
    ))
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        market,
        Arc::new(BuyFactory),
    ));

    // When: the public paper run consumes both PM and reference sources.
    let app = Pmkit::builder(config()?).run(run).start().await?;
    let RunReport::Paper(report) = app
        .report(&RunId::new("paper-reference")?)
        .ok_or("missing report")?
    else {
        return Err("expected a paper report".into());
    };
    let recording_facts = match recording_facts.lock() {
        Ok(facts) => facts.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };

    // Then: reference facts are ordered once, and one failing key does not stop the recorder.
    assert_eq!(failing_calls.load(Ordering::Relaxed), 4);
    assert_eq!(nonempty_reference_books.load(Ordering::Relaxed), 0);
    assert_eq!(recording_facts.len(), 4);
    assert!(matches!(
        recording_facts.first(),
        Some(StrategyFact::Market(_))
    ));
    assert_eq!(
        recording_facts[1..].to_vec(),
        vec![
            StrategyFact::Reference(reference_trade(1, 2, 101)),
            StrategyFact::Reference(reference_trade(2, 3, 102)),
            StrategyFact::Reference(reference_trade(3, 4, 103)),
        ]
    );
    assert_eq!(report.events_processed, 1);
    assert_eq!(report.fills, 1);
    assert_eq!(report.exposure.portfolio_notional, Decimal::new(45, 1));
    Ok(())
}

#[tokio::test]
#[allow(clippy::significant_drop_tightening)]
async fn paper_reference_rejection_is_durable_and_risk_only()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(TursoTapeStore::open_local(directory.path().join("reference.db")).await?);
    let mut limits = risk()?;
    limits.max_order_notional = Money::usdc(0);
    let run = PaperRun::new(
        RunId::new("paper-reference-rejection")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        limits,
        Arc::new(ScriptedLive),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .reference_data(Arc::new(FiniteReferenceSource {
        events: vec![reference_trade(1, 2, 101)],
    }))
    .strategy(StrategyRegistration::new(
        StrategyId::new("reference")?,
        MarketId::new("btc-5m")?,
        Arc::new(RepeatBuyFactory),
    ));
    let app = Pmkit::builder(config()?)
        .storage(store.clone())
        .run(run)
        .start()
        .await?;
    let report = app
        .report(&RunId::new("paper-reference-rejection")?)
        .ok_or("missing report")?;
    let RunReport::Paper(report) = report else {
        return Err("expected paper report".into());
    };
    assert_eq!(report.fills, 0);
    assert_eq!(report.metrics.rejected, 2);
    let scope = OwnerScope::new(
        PortfolioId::new("alice")?,
        RunId::new("paper-reference-rejection")?,
    );
    let decisions = store.read_decisions(&scope).await?;
    let reference = decisions
        .iter()
        .find(|decision| decision.identity.correlation_id.contains(":cex-trade:"))
        .ok_or("missing durable CEX decision")?;
    assert_eq!(
        reference.payload["decision"]["risk"][0]["verdict"]["kind"],
        "rejected"
    );
    assert_eq!(
        reference.payload["decision"]["risk"][0]["verdict"]["reason"],
        "risk gate"
    );
    Ok(())
}

#[tokio::test]
async fn paper_run_rejects_mismatched_account_owner() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a store-backed paper run receives another portfolio's account envelope.
    let directory = tempfile::tempdir()?;
    let store =
        Arc::new(TursoTapeStore::open_local(directory.path().join("owner-check.db")).await?);
    let run = PaperRun::new(
        RunId::new("paper-owner-check")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        risk()?,
        Arc::new(ScriptedLive),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .account_data(Arc::new(MismatchedAccountSource));
    let scope = OwnerScope::new(run.portfolio().clone(), run.id().clone());

    // When: the public paper boundary starts the run.
    let result = Pmkit::builder(config()?)
        .storage(store.clone())
        .run(run)
        .start()
        .await;
    let page = store
        .read_envelopes(&scope, None, NonZeroUsize::MIN)
        .await?;

    // Then: owner mismatch aborts before durable or ledger mutation.
    assert!(result.is_err());
    assert!(page.items.is_empty());
    drop(store);
    Ok(())
}

#[tokio::test]
async fn paper_failure_retains_restored_fill_diagnostics() -> Result<(), Box<dyn std::error::Error>>
{
    // Given: a durable paper run with an authoritative fill.
    let directory = tempfile::tempdir()?;
    let store: Arc<dyn TapeStore> =
        Arc::new(TursoTapeStore::open_local(directory.path().join("paper.db")).await?);
    let run_id = RunId::new("paper-failure-diagnostics")?;
    let initial_run = PaperRun::new(
        run_id.clone(),
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        risk()?,
        Arc::new(ScriptedLive),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));
    Pmkit::builder(config()?)
        .storage(Arc::clone(&store))
        .run(initial_run)
        .start()
        .await?;

    let failing_run = PaperRun::new(
        run_id.clone(),
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        risk()?,
        Arc::new(FailingLive),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));

    // When: the restored paper run fails through the public start boundary.
    let error = Pmkit::builder(config()?)
        .storage(store)
        .run(failing_run)
        .start()
        .await
        .err()
        .ok_or("failing paper feed unexpectedly completed")?;

    // Then: its typed diagnostics retain the restored authoritative fill count.
    let diagnostics = error.diagnostics().ok_or("missing diagnostics")?;
    println!("paper failure diagnostics: {diagnostics:?}");
    assert_eq!(diagnostics.run, run_id);
    assert!(diagnostics.fills > 0, "diagnostics: {diagnostics:?}");
    Ok(())
}

#[tokio::test]
#[allow(clippy::significant_drop_tightening)]
async fn paper_run_clears_exposure_when_book_loses_its_mark()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a filled paper position followed by an unmarkable book for the same outcome.
    let run = PaperRun::new(
        RunId::new("paper-stale-mark")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        risk()?,
        Arc::new(StaleMarkLive),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(RepeatBuyFactory),
    ));

    // When: the live feed completes.
    let directory = tempfile::tempdir()?;
    let store =
        Arc::new(TursoTapeStore::open_local(directory.path().join("missing-mark.db")).await?);
    let app = Pmkit::builder(config()?)
        .storage(store.clone())
        .run(run)
        .start()
        .await?;
    let RunReport::Paper(report) = app
        .report(&RunId::new("paper-stale-mark")?)
        .ok_or("missing report")?
    else {
        return Err("expected a paper report".into());
    };

    // Then: the obsolete mid-price cannot survive in reported exposure, and the second action is rejected once.
    assert_eq!(report.exposure.portfolio_notional, Decimal::ZERO);
    assert_eq!(report.metrics.rejected, 1);
    let scope = OwnerScope::new(PortfolioId::new("alice")?, RunId::new("paper-stale-mark")?);
    let decisions = store.read_decisions(&scope).await?;
    let missing_mark = decisions
        .iter()
        .find(|decision| decision.payload["snapshot"]["timing"]["observation_ms"] == 2)
        .ok_or("missing-mark decision was not recorded")?;
    assert_eq!(
        missing_mark.payload["decision"]["risk"][0]["verdict"]["reason"],
        "risk data unavailable"
    );
    Ok(())
}

#[tokio::test]
async fn default_fee_unchanged() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a durable paper run with no explicit fee-model override.
    let directory = tempfile::tempdir()?;
    let store =
        Arc::new(TursoTapeStore::open_local(directory.path().join("default-fee.db")).await?);
    let run = PaperRun::new(
        RunId::new("paper-default-fee")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        risk()?,
        Arc::new(ScriptedLive),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));

    // When: the default paper path fills ten shares at the 46-cent ask.
    Pmkit::builder(config()?)
        .storage(store.clone())
        .run(run)
        .start()
        .await?;
    let scope = OwnerScope::new(PortfolioId::new("alice")?, RunId::new("paper-default-fee")?);
    let decisions = store.read_decisions(&scope).await?;
    let fill = decisions
        .iter()
        .find(|decision| decision.payload["event"]["kind"] == "fill")
        .ok_or("paper fill was not recorded")?;

    // Then: the durable fill fee exactly matches the legacy Crypto calculation.
    assert_eq!(fill.payload["event"]["fee"], "0.17388");
    drop(store);
    Ok(())
}

#[tokio::test]
async fn paper_driver_cancellation_wakes_without_an_event() -> Result<(), Box<dyn std::error::Error>>
{
    let run = PaperRun::new(
        RunId::new("paper-idle-cancel")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        risk()?,
        Arc::new(IdleLive),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));
    let cancellation = Cancellation::new();
    let (subscriber, mut events) = tokio::sync::mpsc::unbounded_channel();
    let control = RunControl {
        cancel: Some(cancellation.clone()),
        subscriber: Some(subscriber),
        metrics: None,
    };
    let task =
        tokio::spawn(async move { crate::paper::drive_with_control(&run, None, &control).await });
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await?
            .ok_or("missing started event")?,
        RunLifecycleEvent::Started { .. }
    ));
    cancellation.cancel();
    let report = tokio::time::timeout(Duration::from_secs(1), task).await???;
    assert_eq!(report.events_processed, 0);
    assert!(matches!(
        events.recv().await,
        Some(RunLifecycleEvent::Cancelled { .. })
    ));
    Ok(())
}
