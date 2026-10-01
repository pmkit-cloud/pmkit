use crate::{
    Pmkit, RunReport, StartError,
    test_support::{BuyFactory, config, risk},
};
use async_trait::async_trait;
use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
use pmkit_data::{
    DataSourceError, LiveAccountDataSource, LiveCexDataSource, LiveDataSource,
    LiveOracleDataSource, SourceSignal,
};
use pmkit_event::{
    CexReferenceEnvelope, CexReferenceEvent, MarketEvent, PmAccountEnvelope, PmAccountEvent,
    PmMarketEnvelope, SourceEnvelope, StreamMetadata,
};
use pmkit_market::{Asset, Outcome};
use pmkit_money::Money;
use pmkit_runtime::{PartialRiskLimits, RiskLimitOverrides, StrategyRegistration};
use pmkit_spec::{ConservativeV1Config, POLYBOLT_ORACLE_SOURCE_ID, PaperRun};
use pmkit_store::{OwnerScope, TapeStore, TursoTapeStore};
use pmkit_strategy::{
    Action, Actions, Strategy, StrategyContext, StrategyError, StrategyFactory, StrategyInitError,
};
use pmkit_tape::{RawCapturePayload, RawCaptureReader};
use rust_decimal::Decimal;
use std::io::BufReader;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::mpsc::Sender;

const ORACLE_BINDING: &str = "polybolt:oracle";
const ORACLE_FRAME: &[u8] = br#"{"oracle":"twap60s","price":"0.42"}"#;
const CEX_LIQUIDITY_BOOK_FRAME: &[u8] = br#"{"event_type":"book","market":"0x0000000000000000000000000000000000000000000000000000000000000001","asset_id":"1","timestamp":"0","bids":[{"price":"0.38","size":"2"}],"asks":[{"price":"0.39","size":"2"}]}"#;

struct ScriptedLive;

struct BothOutcomesLive;

struct NoEventLive;

struct CexLiquidityLive;

struct ScriptedReferenceLive;

struct ReferenceBuyer {
    calls: Arc<AtomicUsize>,
    nonempty_books: Arc<AtomicUsize>,
}

struct ReferenceBuyerFactory {
    calls: Arc<AtomicUsize>,
    nonempty_books: Arc<AtomicUsize>,
}

struct CexOrderStrategy(Arc<AtomicUsize>);

struct CexOrderFactory(Arc<AtomicUsize>);

struct StaleMarkLive;

struct RiskSequenceLive {
    books: Vec<(Decimal, Decimal, i64)>,
}

struct FailingLive;

struct MismatchedAccountSource;

struct ScriptedOracle {
    frames: Vec<OracleFrame>,
    gap_before_frames: bool,
}

struct OracleFrame {
    frame_sequence: i64,
    ingest_sequence: u64,
    timestamp_ms: i64,
    raw_frame: Vec<u8>,
}

struct OracleProbe(Arc<AtomicUsize>);

struct OracleProbeFactory(Arc<AtomicUsize>);

struct CancelLive;

struct CancelAfterPlace {
    seen: usize,
}

struct CancelAfterPlaceFactory;

struct RepeatPlace {
    seen: usize,
}

struct RepeatPlaceFactory;

struct MultiQuote;

struct MultiQuoteFactory;

struct RepeatTaker;

struct RepeatTakerFactory;

impl Strategy for RepeatTaker {
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

impl StrategyFactory for RepeatTakerFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(RepeatTaker))
    }
}

impl Strategy for RepeatPlace {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        self.seen += 1;
        Ok(Actions::place(pmkit_exec::PlaceOrder {
            market: context.market.clone(),
            outcome: Outcome::Up,
            side: pmkit_book::Side::Buy,
            price: Decimal::new(45, 2),
            qty: Decimal::ONE,
            post_only: true,
            tif: pmkit_exec::TimeInForce::Gtc,
        }))
    }
}

impl StrategyFactory for RepeatPlaceFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(RepeatPlace { seen: 0 }))
    }
}

impl Strategy for MultiQuote {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        let mut actions = Actions::none();
        actions.push(Action::ReplaceQuotes {
            cancel: Vec::new(),
            place: vec![
                pmkit_exec::PlaceOrder {
                    market: context.market.clone(),
                    outcome: Outcome::Up,
                    side: pmkit_book::Side::Buy,
                    price: Decimal::new(40, 2),
                    qty: Decimal::ONE,
                    post_only: true,
                    tif: pmkit_exec::TimeInForce::Gtc,
                },
                pmkit_exec::PlaceOrder {
                    market: context.market.clone(),
                    outcome: Outcome::Up,
                    side: pmkit_book::Side::Buy,
                    price: Decimal::new(41, 2),
                    qty: Decimal::ONE,
                    post_only: true,
                    tif: pmkit_exec::TimeInForce::Gtc,
                },
            ],
        });
        Ok(actions)
    }
}

impl StrategyFactory for MultiQuoteFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(MultiQuote))
    }
}

impl Strategy for OracleProbe {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        if matches!(
            context.fact,
            pmkit_event::StrategyFact::Reference(CexReferenceEvent::Twap60sUpdate {
                asset: Asset::Btc,
                ..
            })
        ) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        Ok(Actions::none())
    }
}

impl StrategyFactory for OracleProbeFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(OracleProbe(Arc::clone(&self.0))))
    }
}

impl Strategy for CancelAfterPlace {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        self.seen += 1;
        if self.seen == 1 {
            Ok(Actions::place(pmkit_exec::PlaceOrder {
                market: context.market.clone(),
                outcome: Outcome::Up,
                side: pmkit_book::Side::Buy,
                price: Decimal::new(45, 2),
                qty: Decimal::ONE,
                post_only: true,
                tif: pmkit_exec::TimeInForce::Gtc,
            }))
        } else {
            Ok(Actions::cancel_all())
        }
    }
}

impl StrategyFactory for CancelAfterPlaceFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(CancelAfterPlace { seen: 0 }))
    }
}

impl Strategy for ReferenceBuyer {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        if !matches!(context.fact, pmkit_event::StrategyFact::Reference(_)) {
            return Ok(Actions::none());
        }
        self.calls.fetch_add(1, Ordering::Relaxed);
        if !context.book.bids.is_empty()
            || !context.book.asks.is_empty()
            || context.book.last_trade_price.is_some()
            || context.book.timestamp_ms != 0
        {
            self.nonempty_books.fetch_add(1, Ordering::Relaxed);
        }
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

impl StrategyFactory for ReferenceBuyerFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(ReferenceBuyer {
            calls: Arc::clone(&self.calls),
            nonempty_books: Arc::clone(&self.nonempty_books),
        }))
    }
}

impl Strategy for CexOrderStrategy {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        if !matches!(context.fact, pmkit_event::StrategyFact::Reference(_)) {
            return Ok(Actions::none());
        }
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(Actions::place(pmkit_exec::PlaceOrder {
            market: context.market.clone(),
            outcome: Outcome::Up,
            side: pmkit_book::Side::Buy,
            price: Decimal::new(40, 2),
            qty: Decimal::ONE,
            post_only: false,
            tif: pmkit_exec::TimeInForce::Gtc,
        }))
    }
}

impl StrategyFactory for CexOrderFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(CexOrderStrategy(Arc::clone(&self.0))))
    }
}

#[async_trait]
impl LiveDataSource for CancelLive {
    async fn subscribe(
        &self,
        market: MarketId,
        outcome: Outcome,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        if outcome == Outcome::Up {
            for timestamp_ms in [1, 2] {
                sink.send(SourceSignal::market_event(MarketEvent::BookUpdate {
                    market: market.clone(),
                    outcome,
                    bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
                    asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
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
            .map_err(|_| DataSourceError::SinkClosed)
    }
}

#[async_trait]
impl LiveCexDataSource for ScriptedReferenceLive {
    async fn subscribe_reference(&self, sink: Sender<SourceSignal>) -> Result<(), DataSourceError> {
        sink.send(SourceSignal::Data(Box::new(SourceEnvelope::CexReference(
            CexReferenceEnvelope {
                metadata: StreamMetadata {
                    schema_version: 1,
                    source_id: "binance-live".into(),
                    source_time_ms: 1,
                    canonical_source_rank: 1,
                    receipt_time_ms: 1,
                    connection_id: "reference".into(),
                    connection_epoch: 0,
                    frame_sequence: 1,
                    ingest_sequence: 1,
                },
                raw_frame: br#"{"oracle":"twap60s","price":"0.42"}"#.to_vec(),
                fact: CexReferenceEvent::Twap60sUpdate {
                    asset: Asset::Btc,
                    price: Decimal::new(42, 2),
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
impl LiveOracleDataSource for ScriptedOracle {
    async fn subscribe_oracle(
        &self,
        asset: Asset,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        if self.gap_before_frames {
            sink.send(SourceSignal::Gap(pmkit_data::SourceGap {
                source_id: POLYBOLT_ORACLE_SOURCE_ID.to_owned(),
                detected_at_ms: 1, /*explicit syntheticclock fixture*/
                metadata: None,
                raw_frame: Vec::new(),
                range: None,
                unresolved_count: None,
                reason: "scripted oracle gap".into(),
            }))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        }
        for frame in &self.frames {
            sink.send(SourceSignal::Data(Box::new(SourceEnvelope::CexReference(
                CexReferenceEnvelope {
                    metadata: StreamMetadata {
                        schema_version: 1,
                        source_id: POLYBOLT_ORACLE_SOURCE_ID.to_owned(),
                        source_time_ms: frame.timestamp_ms,
                        canonical_source_rank: 1,
                        receipt_time_ms: frame.timestamp_ms,
                        connection_id: "paper-oracle".into(),
                        connection_epoch: 0,
                        frame_sequence: frame.frame_sequence,
                        ingest_sequence: frame.ingest_sequence,
                    },
                    raw_frame: frame.raw_frame.clone(),
                    fact: CexReferenceEvent::Twap60sUpdate {
                        asset,
                        price: Decimal::new(42, 2),
                        timestamp_ms: frame.timestamp_ms,
                    },
                },
            ))))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
        }
        // Paper receipt-order feeds must accept finite oracle sources without watermarks.
        sink.send(SourceSignal::Eof)
            .await
            .map_err(|_| DataSourceError::SinkClosed)
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
impl LiveDataSource for NoEventLive {
    async fn subscribe(
        &self,
        _market: MarketId,
        _outcome: Outcome,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        sink.send(SourceSignal::Eof)
            .await
            .map_err(|_| DataSourceError::SinkClosed)
    }
}

#[async_trait]
impl LiveDataSource for CexLiquidityLive {
    async fn subscribe(
        &self,
        market: MarketId,
        outcome: Outcome,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        if outcome == Outcome::Up {
            sink.send(SourceSignal::Data(Box::new(SourceEnvelope::PmMarket(
                PmMarketEnvelope {
                    metadata: StreamMetadata {
                        schema_version: 1,
                        source_id: "polymarket:market-ws".into(),
                        source_time_ms: 0,
                        canonical_source_rank: 0,
                        receipt_time_ms: 0,
                        connection_id: "paper-cex-market".into(),
                        connection_epoch: 0,
                        frame_sequence: 1,
                        ingest_sequence: 1,
                    },
                    raw_frame: CEX_LIQUIDITY_BOOK_FRAME.to_vec(),
                    fact: MarketEvent::BookUpdate {
                        market,
                        outcome,
                        bids: vec![(Decimal::new(38, 2), Decimal::from(2))],
                        asks: vec![(Decimal::new(39, 2), Decimal::from(2))],
                        timestamp_ms: 0,
                    },
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
impl LiveDataSource for BothOutcomesLive {
    async fn subscribe(
        &self,
        market: MarketId,
        outcome: Outcome,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        sink.send(SourceSignal::market_event(MarketEvent::BookUpdate {
            market,
            outcome,
            bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
            asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
            timestamp_ms: 1,
        }))
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
impl LiveDataSource for RiskSequenceLive {
    async fn subscribe(
        &self,
        market: MarketId,
        outcome: Outcome,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        if outcome == Outcome::Up {
            for (bid, ask, timestamp_ms) in &self.books {
                sink.send(SourceSignal::market_event(MarketEvent::BookUpdate {
                    market: market.clone(),
                    outcome,
                    bids: vec![(*bid, Decimal::from(50))],
                    asks: vec![(*ask, Decimal::from(50))],
                    timestamp_ms: *timestamp_ms,
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
            .map_err(|_| DataSourceError::SinkClosed)
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
async fn paper_cancel_all_releases_strategy_orders() -> Result<(), Box<dyn std::error::Error>> {
    let run = PaperRun::new(
        RunId::new("paper-cancel-action")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        risk()?,
        Arc::new(CancelLive),
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
        StrategyId::new("cancel-maker")?,
        MarketId::new("btc-5m")?,
        Arc::new(CancelAfterPlaceFactory),
    ));

    let app = Pmkit::builder(config()?).run(run).start().await?;
    let RunReport::Paper(report) = app.wait_for(RunId::new("paper-cancel-action")?).await? else {
        return Err("expected a paper report".into());
    };
    assert_eq!(report.fills, 0);
    assert_eq!(report.exposure.portfolio_notional, Decimal::ZERO);
    Ok(())
}

#[tokio::test]
async fn paper_risk_gate_counts_resting_order_reservation() -> Result<(), Box<dyn std::error::Error>>
{
    let mut limits = risk()?;
    limits.max_open_orders = NonZeroU32::new(1).ok_or("nonzero")?;
    let run = PaperRun::new(
        RunId::new("paper-open-order-limit")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        limits,
        Arc::new(CancelLive),
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
        StrategyId::new("maker")?,
        MarketId::new("btc-5m")?,
        Arc::new(RepeatPlaceFactory),
    ));

    let app = Pmkit::builder(config()?).run(run).start().await?;
    let RunReport::Paper(report) = app
        .report(&RunId::new("paper-open-order-limit")?)
        .ok_or("missing report")?
    else {
        return Err("expected a paper report".into());
    };
    assert_eq!(report.fills, 0);
    assert_eq!(report.metrics.rejected, 1);
    assert_eq!(report.exposure.portfolio_notional, Decimal::new(45, 2));
    Ok(())
}

#[tokio::test]
async fn paper_delivers_reference_facts_with_latest_market_context()
-> Result<(), Box<dyn std::error::Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let nonempty_books = Arc::new(AtomicUsize::new(0));
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
    .reference_data(Arc::new(ScriptedReferenceLive))
    .strategy(StrategyRegistration::new(
        StrategyId::new("reference-buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(ReferenceBuyerFactory {
            calls: Arc::clone(&calls),
            nonempty_books: Arc::clone(&nonempty_books),
        }),
    ));

    let app = Pmkit::builder(config()?).run(run).start().await?;
    let RunReport::Paper(report) = app.wait_for(RunId::new("paper-reference")?).await? else {
        return Err("expected a paper report".into());
    };

    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(nonempty_books.load(Ordering::Relaxed), 1);
    assert_eq!(
        report.fills, 1,
        "reference actions should reach paper execution"
    );
    Ok(())
}

#[tokio::test]
#[allow(clippy::significant_drop_tightening)]
async fn paper_cex_verdicts_are_scoped_to_each_strategy_decision()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let run_id = RunId::new("paper-cex-strategies")?;
    let portfolio = PortfolioId::new("alice")?;
    let market = MarketId::new("btc-5m")?;
    let rejected_id = StrategyId::new("cex-rejected")?;
    let accepted_id = StrategyId::new("cex-accepted")?;
    let rejected_calls = Arc::new(AtomicUsize::new(0));
    let accepted_calls = Arc::new(AtomicUsize::new(0));
    let store =
        Arc::new(TursoTapeStore::open_local(directory.path().join("cex-decisions.db")).await?);
    let mut rejected_overrides = RiskLimitOverrides::default();
    rejected_overrides.per_strategy.insert(
        rejected_id.clone(),
        PartialRiskLimits {
            max_order_notional: Some(Money::ZERO),
            ..PartialRiskLimits::default()
        },
    );
    let run = PaperRun::new(
        run_id.clone(),
        portfolio.clone(),
        Money::usdc(10_000),
        risk()?,
        Arc::new(CexLiquidityLive),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .reference_data(Arc::new(ScriptedReferenceLive))
    .strategy(
        StrategyRegistration::new(
            rejected_id.clone(),
            market.clone(),
            Arc::new(CexOrderFactory(Arc::clone(&rejected_calls))),
        )
        .risk_overrides(rejected_overrides),
    )
    .strategy(StrategyRegistration::new(
        accepted_id.clone(),
        market,
        Arc::new(CexOrderFactory(Arc::clone(&accepted_calls))),
    ));

    let app = Pmkit::builder(config()?)
        .storage(store.clone())
        .capture(directory.path())
        .run(run)
        .start()
        .await?;
    let RunReport::Paper(report) = app.wait_for(run_id.clone()).await? else {
        return Err("expected a paper report".into());
    };
    assert_eq!(report.metrics.rejected, 1);
    assert_eq!(report.fills, 1);
    assert_eq!(rejected_calls.load(Ordering::Relaxed), 1);
    assert_eq!(accepted_calls.load(Ordering::Relaxed), 1);

    let file = std::fs::File::open(directory.path().join(format!("run-{run_id}.jsonl")))?;
    let mut capture = RawCaptureReader::new(BufReader::new(file));
    let mut records = Vec::new();
    while let Some(record) = capture.next_record()? {
        records.push(record);
    }
    let cex_records = records
        .iter()
        .filter(|record| record.capture.source_id == "binance-live")
        .collect::<Vec<_>>();
    assert_eq!(
        cex_records.len(),
        1,
        "CEX event was not captured exactly once"
    );
    assert_eq!(
        cex_records[0].capture.payload,
        RawCapturePayload::Frame {
            raw_bytes: br#"{"oracle":"twap60s","price":"0.42"}"#.to_vec(),
        }
    );
    let book_record = records
        .iter()
        .find(|record| record.capture.source_id == "polymarket:market-ws")
        .ok_or("native PM book frame was not captured")?;
    assert_eq!(
        book_record.capture.payload,
        RawCapturePayload::Frame {
            raw_bytes: CEX_LIQUIDITY_BOOK_FRAME.to_vec(),
        }
    );

    assert_cex_decisions_are_strategy_scoped(&store, portfolio, run_id, &rejected_id, &accepted_id)
        .await?;
    drop(store);
    Ok(())
}

async fn assert_cex_decisions_are_strategy_scoped(
    store: &TursoTapeStore,
    portfolio: PortfolioId,
    run_id: RunId,
    rejected_id: &StrategyId,
    accepted_id: &StrategyId,
) -> Result<(), Box<dyn std::error::Error>> {
    let scope = OwnerScope::new(portfolio, run_id);
    let decisions = store.read_decisions(&scope).await?;
    let warmup_decisions = decisions
        .iter()
        .filter(|decision| {
            decision.payload["snapshot"].is_object()
                && decision
                    .identity
                    .correlation_id
                    .starts_with("paper-market:")
        })
        .collect::<Vec<_>>();
    assert_eq!(warmup_decisions.len(), 2);
    for decision in &warmup_decisions {
        assert_eq!(decision.payload["snapshot"]["pm_book"]["best_bid"], "0.38");
        assert_eq!(decision.payload["snapshot"]["pm_book"]["best_ask"], "0.39");
        assert_eq!(decision.payload["decision"]["kind"], "no_action");
    }
    let cex_decisions = decisions
        .iter()
        .filter(|decision| {
            decision.payload["snapshot"].is_object()
                && decision
                    .identity
                    .correlation_id
                    .contains(":source:12:binance-live:")
        })
        .collect::<Vec<_>>();
    assert_eq!(cex_decisions.len(), 2);
    assert_ne!(
        cex_decisions[0].identity.correlation_id,
        cex_decisions[1].identity.correlation_id
    );
    for (strategy, expected_kind, expected_reason) in [
        (rejected_id, "rejected", Some("risk gate")),
        (accepted_id, "accepted", None),
    ] {
        let id = strategy.to_string();
        let decision = cex_decisions
            .iter()
            .find(|decision| {
                decision
                    .identity
                    .correlation_id
                    .contains(&format!("strategy:{}:{id}:market:", id.len()))
            })
            .ok_or("missing strategy-scoped CEX decision")?;
        assert_eq!(
            decision.identity.correlation_id,
            format!(
                "paper-reference:strategy:{}:{id}:market:6:btc-5m:source:12:binance-live:connection:9:reference:epoch:0:frame:1:ingest:1:stream:9:reference:event:1",
                id.len()
            )
        );
        let verdicts = decision.payload["decision"]["risk"]
            .as_array()
            .ok_or("missing stored risk verdict")?;
        assert_eq!(verdicts.len(), 1);
        assert_eq!(verdicts[0]["action_index"], 0);
        assert_eq!(verdicts[0]["verdict"]["kind"], expected_kind);
        if let Some(reason) = expected_reason {
            assert_eq!(verdicts[0]["verdict"]["reason"], reason);
        }
    }
    let placements = decisions
        .iter()
        .filter(|decision| {
            decision.payload["record_type"] == "paper_ledger"
                && decision.payload["event"]["kind"] == "order_placed"
        })
        .collect::<Vec<_>>();
    assert_eq!(placements.len(), 1);
    assert_eq!(
        placements[0].payload["event"]["order"]["strategy"],
        "cex-accepted"
    );
    Ok(())
}

fn oracle_frame(frame_sequence: i64, ingest_sequence: u64, timestamp_ms: i64) -> OracleFrame {
    OracleFrame {
        frame_sequence,
        ingest_sequence,
        timestamp_ms,
        raw_frame: ORACLE_FRAME.to_vec(),
    }
}

#[tokio::test]
async fn paper_registered_oracle_is_delivered_and_captured_once()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let run_id = RunId::new("paper-oracle")?;
    let market = MarketId::new("btc-5m")?;
    let calls = Arc::new(AtomicUsize::new(0));
    let run = PaperRun::new(
        run_id.clone(),
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        risk()?,
        Arc::new(NoEventLive),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .settlement_oracle_named(ORACLE_BINDING, market.clone(), "BTC")
    .require_settlement_oracle(ORACLE_BINDING, market.clone())
    .strategy(StrategyRegistration::new(
        StrategyId::new("oracle-probe")?,
        market,
        Arc::new(OracleProbeFactory(Arc::clone(&calls))),
    ));

    let app = Pmkit::builder(config()?)
        .capture(directory.path())
        .register_oracle_source(
            ORACLE_BINDING,
            Arc::new(ScriptedOracle {
                frames: vec![oracle_frame(1, 1, 1)],
                gap_before_frames: false,
            }),
        )
        .run(run)
        .start()
        .await?;
    let RunReport::Paper(report) = app.wait_for(run_id.clone()).await? else {
        return Err("expected a paper report".into());
    };

    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(report.events_processed, 0);
    let file = std::fs::File::open(directory.path().join(format!("run-{run_id}.jsonl")))?;
    let mut capture = RawCaptureReader::new(BufReader::new(file));
    let record = capture
        .next_record()?
        .ok_or("oracle frame was not captured")?;
    assert_eq!(record.capture.source_id, POLYBOLT_ORACLE_SOURCE_ID);
    assert_eq!(
        record.capture.payload,
        RawCapturePayload::Frame {
            raw_bytes: ORACLE_FRAME.to_vec(),
        }
    );
    assert!(
        capture.next_record()?.is_none(),
        "oracle frame was captured twice"
    );
    Ok(())
}

#[tokio::test]
async fn paper_oracle_gap_is_captured_and_fails_before_strategy()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let run_id = RunId::new("paper-oracle-gap")?;
    let market = MarketId::new("btc-5m")?;
    let calls = Arc::new(AtomicUsize::new(0));
    let run = PaperRun::new(
        run_id.clone(),
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        risk()?,
        Arc::new(NoEventLive),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .settlement_oracle_named(ORACLE_BINDING, market.clone(), "BTC")
    .require_settlement_oracle(ORACLE_BINDING, market.clone())
    .strategy(StrategyRegistration::new(
        StrategyId::new("oracle-probe")?,
        market,
        Arc::new(OracleProbeFactory(Arc::clone(&calls))),
    ));

    let error = Pmkit::builder(config()?)
        .capture(directory.path())
        .register_oracle_source(
            ORACLE_BINDING,
            Arc::new(ScriptedOracle {
                frames: vec![oracle_frame(2, 2, 2)],
                gap_before_frames: true,
            }),
        )
        .run(run)
        .start()
        .await
        .err()
        .ok_or("oracle gap unexpectedly completed")?;
    assert!(matches!(
        error,
        StartError::RunFailed { diagnostics, source }
            if diagnostics.decisions == 0
                && matches!(
                    source.as_ref(),
                    StartError::Source {
                        source: DataSourceError::ReplayGap { .. },
                        ..
                    }
                )
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 0);

    let file = std::fs::File::open(directory.path().join(format!("run-{run_id}.jsonl")))?;
    let mut capture = RawCaptureReader::new_audit(BufReader::new(file));
    let gap = capture
        .next_record()?
        .ok_or("oracle gap was not captured")?;
    assert_eq!(gap.ingest_sequence, 0);
    assert_eq!(gap.capture.source_id, POLYBOLT_ORACLE_SOURCE_ID);
    assert_eq!(
        gap.capture.payload,
        RawCapturePayload::Gap {
            range: None,
            unresolved_count: None,
            reason: "scripted oracle gap".into(),
            raw_frame: Vec::new(),
        }
    );
    assert!(capture.next_record()?.is_none());
    Ok(())
}

#[tokio::test]
async fn paper_does_not_substitute_a_named_cex_source_for_a_missing_oracle()
-> Result<(), Box<dyn std::error::Error>> {
    let market = MarketId::new("btc-5m")?;
    let calls = Arc::new(AtomicUsize::new(0));
    let run = PaperRun::new(
        RunId::new("paper-missing-oracle")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        risk()?,
        Arc::new(NoEventLive),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .reference_data_named(ORACLE_BINDING, Arc::new(ScriptedReferenceLive))
    .settlement_oracle_named(ORACLE_BINDING, market.clone(), "BTC")
    .require_settlement_oracle(ORACLE_BINDING, market.clone())
    .strategy(StrategyRegistration::new(
        StrategyId::new("oracle-probe")?,
        market,
        Arc::new(OracleProbeFactory(Arc::clone(&calls))),
    ));

    let error = Pmkit::builder(config()?)
        .run(run)
        .start()
        .await
        .err()
        .ok_or("run without a registered live oracle unexpectedly completed")?;

    assert!(matches!(
        error,
        StartError::OracleSourceMissing { binding, .. } if binding == ORACLE_BINDING
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
#[allow(clippy::significant_drop_tightening)]
async fn paper_same_timestamp_outcomes_have_distinct_decision_identities()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(TursoTapeStore::open_local(directory.path().join("outcomes.db")).await?);
    let run = PaperRun::new(
        RunId::new("paper-outcomes")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        risk()?,
        Arc::new(BothOutcomesLive),
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
        StrategyId::new("observer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));

    let app = Pmkit::builder(config()?)
        .storage(store.clone())
        .run(run)
        .start()
        .await?;
    let RunReport::Paper(report) = app.wait_for(RunId::new("paper-outcomes")?).await? else {
        return Err("expected a paper report".into());
    };
    assert_eq!(report.events_processed, 2);

    let scope = OwnerScope::new(PortfolioId::new("alice")?, RunId::new("paper-outcomes")?);
    let decisions = store.read_decisions(&scope).await?;
    let mut ids = decisions
        .iter()
        .filter(|decision| decision.payload["snapshot"].is_object())
        .map(|decision| decision.identity.correlation_id.clone())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(ids.len(), 2);
    assert!(ids.windows(2).all(|pair| pair[0] != pair[1]), "{ids:?}");
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
async fn paper_risk_gate_rejects_before_ledger_submission() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let store = Arc::new(TursoTapeStore::open_local(directory.path().join("risk-gate.db")).await?);
    let mut limits = risk()?;
    limits.max_order_notional = Money::ZERO;
    let run = PaperRun::new(
        RunId::new("paper-risk-gate")?,
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
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));

    let app = Pmkit::builder(config()?)
        .storage(store.clone())
        .run(run)
        .start()
        .await?;
    let RunReport::Paper(report) = app
        .report(&RunId::new("paper-risk-gate")?)
        .ok_or("missing report")?
    else {
        return Err("expected a paper report".into());
    };
    assert_eq!(report.fills, 0);
    assert_eq!(report.metrics.rejected, 1);

    let scope = OwnerScope::new(PortfolioId::new("alice")?, RunId::new("paper-risk-gate")?);
    let decisions = store.read_decisions(&scope).await?;
    let book_decision = decisions
        .iter()
        .find(|decision| decision.payload["snapshot"].is_object())
        .ok_or("missing durable book decision")?;
    assert_eq!(
        book_decision.payload["decision"]["risk"][0]["verdict"]["kind"],
        "rejected"
    );
    assert_eq!(
        book_decision.payload["decision"]["risk"][0]["verdict"]["reason"],
        "risk gate"
    );
    assert!(
        decisions
            .iter()
            .all(|decision| decision.payload["event"]["kind"] != "placement")
    );
    drop(store);
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "durable strategy and sub-order identity assertions stay together"
)]
#[allow(clippy::significant_drop_tightening)]
async fn paper_multi_strategy_replace_quotes_persist_strategy_and_suborder_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(
        TursoTapeStore::open_local(directory.path().join("paper-multi-identities.db")).await?,
    );
    let run = PaperRun::new(
        RunId::new("paper-multi-identities")?,
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
        StrategyId::new("alpha")?,
        MarketId::new("btc-5m")?,
        Arc::new(MultiQuoteFactory),
    ))
    .strategy(StrategyRegistration::new(
        StrategyId::new("beta")?,
        MarketId::new("btc-5m")?,
        Arc::new(MultiQuoteFactory),
    ));

    let app = Pmkit::builder(config()?)
        .storage(store.clone())
        .run(run)
        .start()
        .await?;
    let RunReport::Paper(report) = app
        .report(&RunId::new("paper-multi-identities")?)
        .ok_or("missing report")?
    else {
        return Err("expected a paper report".into());
    };
    assert_eq!(report.metrics.rejected, 0);

    let scope = OwnerScope::new(
        PortfolioId::new("alice")?,
        RunId::new("paper-multi-identities")?,
    );
    let decisions = store.read_decisions(&scope).await?;
    let snapshots = decisions
        .iter()
        .filter(|decision| decision.payload["snapshot"].is_object())
        .collect::<Vec<_>>();
    assert_eq!(snapshots.len(), 2);
    let mut decision_ids = snapshots
        .iter()
        .map(|decision| decision.identity.correlation_id.clone())
        .collect::<Vec<_>>();
    decision_ids.sort_unstable();
    assert!(decision_ids.windows(2).all(|pair| pair[0] != pair[1]));
    for strategy in ["alpha", "beta"] {
        let decision = snapshots
            .iter()
            .find(|decision| decision.identity.correlation_id.contains(strategy))
            .ok_or("missing strategy-scoped decision")?;
        let verdicts = decision.payload["decision"]["risk"]
            .as_array()
            .ok_or("missing paper risk verdicts")?;
        assert_eq!(verdicts.len(), 2);
        assert_eq!(verdicts[0]["action_index"], 0);
        assert_eq!(verdicts[1]["action_index"], 1);
        assert!(
            verdicts
                .iter()
                .all(|verdict| verdict["verdict"]["kind"] == "accepted")
        );
    }

    let placements = decisions
        .iter()
        .filter(|decision| {
            decision.payload["record_type"] == "paper_ledger"
                && decision.payload["event"]["kind"] == "order_placed"
        })
        .collect::<Vec<_>>();
    assert_eq!(placements.len(), 4);
    let mut placement_ids = placements
        .iter()
        .map(|decision| decision.identity.correlation_id.clone())
        .collect::<Vec<_>>();
    placement_ids.sort_unstable();
    assert!(placement_ids.windows(2).all(|pair| pair[0] != pair[1]));
    for strategy in ["alpha", "beta"] {
        let strategy_placements = placements
            .iter()
            .filter(|decision| decision.payload["event"]["order"]["strategy"] == strategy)
            .collect::<Vec<_>>();
        assert_eq!(strategy_placements.len(), 2);
        let prices = strategy_placements
            .iter()
            .map(|decision| decision.payload["event"]["order"]["price"].clone())
            .collect::<Vec<_>>();
        assert!(prices.contains(&serde_json::json!("0.40")));
        assert!(prices.contains(&serde_json::json!("0.41")));
    }
    drop(store);
    Ok(())
}

#[tokio::test]
#[allow(clippy::significant_drop_tightening)]
async fn paper_max_loss_breach_survives_restart() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(TursoTapeStore::open_local(directory.path().join("loss-latch.db")).await?);
    let mut limits = risk()?;
    limits.max_loss = Money::usdc(1);
    let first_run = PaperRun::new(
        RunId::new("paper-loss-latch")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        limits.clone(),
        Arc::new(RiskSequenceLive {
            books: vec![
                (Decimal::new(44, 2), Decimal::new(46, 2), 1),
                (Decimal::new(4, 2), Decimal::new(6, 2), 2),
            ],
        }),
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
        Arc::new(RepeatTakerFactory),
    ));
    let first_app = Pmkit::builder(config()?)
        .storage(store.clone())
        .run(first_run)
        .start()
        .await?;
    let RunReport::Paper(first_report) = first_app
        .report(&RunId::new("paper-loss-latch")?)
        .ok_or("missing first report")?
    else {
        return Err("expected a paper report".into());
    };
    assert_eq!(first_report.fills, 1);
    assert_eq!(first_report.metrics.rejected, 1);
    drop(first_app);

    let second_run = PaperRun::new(
        RunId::new("paper-loss-latch")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        limits,
        Arc::new(RiskSequenceLive {
            books: vec![(Decimal::new(48, 2), Decimal::new(49, 2), 3)],
        }),
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
        Arc::new(RepeatTakerFactory),
    ));
    let second_app = Pmkit::builder(config()?)
        .storage(store.clone())
        .run(second_run)
        .start()
        .await?;
    let RunReport::Paper(second_report) = second_app
        .report(&RunId::new("paper-loss-latch")?)
        .ok_or("missing second report")?
    else {
        return Err("expected a paper report".into());
    };
    assert_eq!(second_report.fills, 1);
    assert_eq!(second_report.metrics.rejected, 1);
    drop(second_app);
    drop(store);
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "restart regression keeps both runs and durable assertions together"
)]
async fn paper_strategy_max_loss_latch_survives_recovery_and_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let store =
        Arc::new(TursoTapeStore::open_local(directory.path().join("override-loss.db")).await?);
    let strategy = StrategyId::new("override-buyer")?;
    let market = MarketId::new("btc-5m")?;
    let mut overrides = RiskLimitOverrides::default();
    overrides.per_strategy.insert(
        strategy.clone(),
        PartialRiskLimits {
            max_loss: Some(Money::usdc(1)),
            ..PartialRiskLimits::default()
        },
    );
    let limits = risk()?;
    let first_run = PaperRun::new(
        RunId::new("paper-override-loss-latch")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        limits.clone(),
        Arc::new(RiskSequenceLive {
            books: vec![
                (Decimal::new(44, 2), Decimal::new(46, 2), 1),
                (Decimal::new(4, 2), Decimal::new(6, 2), 2),
                (Decimal::new(49, 2), Decimal::new(50, 2), 3),
            ],
        }),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .strategy(
        StrategyRegistration::new(
            strategy.clone(),
            market.clone(),
            Arc::new(RepeatTakerFactory),
        )
        .risk_overrides(overrides.clone()),
    );
    let first_app = Pmkit::builder(config()?)
        .storage(store.clone())
        .run(first_run)
        .start()
        .await?;
    let RunReport::Paper(first_report) = first_app
        .report(&RunId::new("paper-override-loss-latch")?)
        .ok_or("missing first report")?
    else {
        return Err("expected a paper report".into());
    };
    assert_eq!(first_report.fills, 1);
    assert_eq!(first_report.metrics.rejected, 2);
    let scope = OwnerScope::new(
        PortfolioId::new("alice")?,
        RunId::new("paper-override-loss-latch")?,
    );
    let decisions = store.read_decisions(&scope).await?;
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| decision.payload["kind"] == "paper-risk-breach")
            .count(),
        1
    );
    drop(first_app);

    let second_run = PaperRun::new(
        RunId::new("paper-override-loss-latch")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        limits,
        Arc::new(RiskSequenceLive {
            books: vec![(Decimal::new(49, 2), Decimal::new(50, 2), 4)],
        }),
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .strategy(
        StrategyRegistration::new(strategy, market, Arc::new(RepeatTakerFactory))
            .risk_overrides(overrides),
    );
    let second_app = Pmkit::builder(config()?)
        .storage(store.clone())
        .run(second_run)
        .start()
        .await?;
    let RunReport::Paper(second_report) = second_app
        .report(&RunId::new("paper-override-loss-latch")?)
        .ok_or("missing second report")?
    else {
        return Err("expected a paper report".into());
    };
    assert_eq!(second_report.fills, 1);
    assert_eq!(second_report.metrics.rejected, 1);
    drop(second_app);
    drop(store);
    Ok(())
}

#[tokio::test]
async fn paper_risk_gate_counts_resting_position_reservation()
-> Result<(), Box<dyn std::error::Error>> {
    let mut limits = risk()?;
    limits.max_position_notional = Money::from_decimal(Decimal::new(75, 2));
    let run = PaperRun::new(
        RunId::new("paper-resting-position-limit")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        limits,
        Arc::new(CancelLive),
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
        StrategyId::new("maker")?,
        MarketId::new("btc-5m")?,
        Arc::new(RepeatPlaceFactory),
    ));

    let app = Pmkit::builder(config()?).run(run).start().await?;
    let RunReport::Paper(report) = app
        .report(&RunId::new("paper-resting-position-limit")?)
        .ok_or("missing report")?
    else {
        return Err("expected a paper report".into());
    };
    assert_eq!(report.fills, 0);
    assert_eq!(report.metrics.rejected, 1);
    assert_eq!(report.exposure.portfolio_notional, Decimal::new(45, 2));
    Ok(())
}

#[tokio::test]
async fn paper_risk_gate_counts_delayed_position_reservation()
-> Result<(), Box<dyn std::error::Error>> {
    let mut limits = risk()?;
    limits.max_position_notional = Money::usdc(8);
    let run = PaperRun::new(
        RunId::new("paper-delayed-position-limit")?,
        PortfolioId::new("alice")?,
        Money::usdc(10_000),
        limits,
        Arc::new(RiskSequenceLive {
            books: vec![
                (Decimal::new(44, 2), Decimal::new(46, 2), 1),
                (Decimal::new(4, 2), Decimal::new(6, 2), 2),
            ],
        }),
        ConservativeV1Config {
            activation_latency: Duration::from_millis(100),
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("taker")?,
        MarketId::new("btc-5m")?,
        Arc::new(RepeatTakerFactory),
    ));

    let app = Pmkit::builder(config()?).run(run).start().await?;
    let RunReport::Paper(report) = app
        .report(&RunId::new("paper-delayed-position-limit")?)
        .ok_or("missing report")?
    else {
        return Err("expected a paper report".into());
    };
    assert_eq!(report.fills, 0);
    assert_eq!(report.metrics.rejected, 1);
    assert_eq!(report.exposure.portfolio_notional, Decimal::from(5));
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
        Arc::new(BuyFactory),
    ));

    // When: the live feed completes.
    let app = Pmkit::builder(config()?).run(run).start().await?;
    let RunReport::Paper(report) = app
        .report(&RunId::new("paper-stale-mark")?)
        .ok_or("missing report")?
    else {
        return Err("expected a paper report".into());
    };

    // Then: the obsolete mid-price cannot survive in reported exposure.
    assert_eq!(report.exposure.portfolio_notional, Decimal::ZERO);
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
