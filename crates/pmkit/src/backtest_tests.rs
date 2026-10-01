use crate::{
    AppHandle, Cancellation, FeedHealthSnapshot, Pmkit, RunLifecycleEvent, RunMetricsSnapshot,
    RunReport, RuntimeError, StartError,
    test_support::{BuyFactory, config, risk},
};
use async_trait::async_trait;
use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
use pmkit_data::{
    DataSourceError, HistoricalDataSource, ReplayQuery, SourceSignal, parse_binance_agg_trade_live,
};
use pmkit_event::{
    CexReferenceEnvelope, CexReferenceEvent, MarketEvent, PmMarketEnvelope, SourceEnvelope,
    StrategyFact, StreamMetadata,
};
use pmkit_exec::MarketLimits;
use pmkit_market::{Asset, Exchange, Outcome};
use pmkit_money::Money;
use pmkit_polymarket::MarketTokens;
use pmkit_run::{EvidenceRequirement, RetrievalWait};
use pmkit_runtime::StrategyRegistration;
use pmkit_spec::{BacktestRun, ConservativeV1Config, ReplaySpec};
use pmkit_store::{OwnerScope, TapeStore, TursoTapeStore};
use pmkit_strategy::{
    Action, Actions, Strategy, StrategyContext, StrategyError, StrategyFactory, StrategyInitError,
};
use pmkit_tape::{CaptureTransport, RawCaptureDraft, RawCapturePayload, RawCaptureWriter};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::mpsc::Sender;

struct ScriptedHistory {
    ticks: Vec<i64>,
}

struct MarketFactsHistory {
    market: MarketId,
}

struct BothOutcomesHistory;

struct ScriptedReference;

struct TwoMarketHistory;

struct StaleMarkHistory;

struct FailingHistory;

struct Taker;

struct TakerFactory;

struct MultiQuote;

struct MultiQuoteFactory;

struct ReferenceBuyer {
    calls: Arc<AtomicUsize>,
    nonempty_books: Arc<AtomicUsize>,
}

struct ReferenceBuyerFactory {
    calls: Arc<AtomicUsize>,
    nonempty_books: Arc<AtomicUsize>,
}

struct RestThenCancel {
    seen: usize,
}

struct RestThenCancelFactory;

struct PositionProbe(Arc<Mutex<Vec<usize>>>);

struct PositionProbeFactory(Arc<Mutex<Vec<usize>>>);

struct RecordedFactProbe(Arc<Mutex<Vec<(StrategyFact, i64)>>>);

struct RecordedFactProbeFactory(Arc<Mutex<Vec<(StrategyFact, i64)>>>);

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

impl Strategy for RestThenCancel {
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

impl StrategyFactory for RestThenCancelFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(RestThenCancel { seen: 0 }))
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

impl Strategy for RecordedFactProbe {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        self.0
            .lock()
            .map_err(|_| StrategyError {
                message: "recorded fact probe lock poisoned".into(),
            })?
            .push((context.fact.clone(), context.now.as_millis()));
        Ok(Actions::none())
    }
}

impl StrategyFactory for RecordedFactProbeFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(RecordedFactProbe(Arc::clone(&self.0))))
    }
}

#[async_trait]
impl HistoricalDataSource for MarketFactsHistory {
    async fn replay(
        &self,
        _query: ReplayQuery,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        for fact in [
            MarketEvent::BookUpdate {
                market: self.market.clone(),
                outcome: Outcome::Up,
                bids: vec![(Decimal::new(49, 2), Decimal::from(2))],
                asks: vec![(Decimal::new(51, 2), Decimal::from(3))],
                timestamp_ms: 100,
            },
            MarketEvent::BestBidAsk {
                market: self.market.clone(),
                outcome: Outcome::Up,
                bid: Decimal::new(50, 2),
                ask: Decimal::new(51, 2),
                timestamp_ms: 101,
            },
            MarketEvent::LastTrade {
                market: self.market.clone(),
                outcome: Outcome::Up,
                price: Decimal::new(505, 3),
                side: pmkit_book::Side::Sell,
                size: Decimal::new(125, 2),
                timestamp_ms: 102,
            },
        ] {
            sink.send(SourceSignal::market_event(fact))
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
                raw_frame: br#"{"e":"aggTrade","a":7}"#.to_vec(),
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
impl HistoricalDataSource for BothOutcomesHistory {
    async fn replay(
        &self,
        _query: ReplayQuery,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        let market = MarketId::new("btc-5m").map_err(|_| DataSourceError::NotAvailable)?;
        for outcome in [Outcome::Up, Outcome::Down] {
            sink.send(SourceSignal::market_event(MarketEvent::BookUpdate {
                market: market.clone(),
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

fn native_book_capture(
    market: &MarketId,
    source_time_ms: i64,
    receipt_time_ms: i64,
    ask_price: Decimal,
    ordinal: u64,
) -> Result<RawCaptureDraft, std::num::TryFromIntError> {
    let fact = MarketEvent::BookUpdate {
        market: market.clone(),
        outcome: Outcome::Up,
        bids: vec![(Decimal::new(3, 1), Decimal::ONE)],
        asks: vec![(ask_price, Decimal::ONE)],
        timestamp_ms: source_time_ms,
    };
    let raw_bytes = format!(
        r#"{{"event_type":"book","market":"0x0000000000000000000000000000000000000000000000000000000000000001","asset_id":"1","timestamp":"{source_time_ms}","bids":[{{"price":"0.3","size":"1"}}],"asks":[{{"price":"{ask_price}","size":"1"}}]}}"#
    )
    .into_bytes();
    native_market_capture(fact, receipt_time_ms, ordinal, 1, raw_bytes)
}

fn native_market_capture(
    fact: MarketEvent,
    receipt_time_ms: i64,
    ordinal: u64,
    source_ingest_sequence: u64,
    raw_bytes: Vec<u8>,
) -> Result<RawCaptureDraft, std::num::TryFromIntError> {
    const SOURCE_ID: &str = "polymarket:market-ws";
    let metadata = StreamMetadata {
        schema_version: 3,
        source_id: SOURCE_ID.to_owned(),
        source_time_ms: fact.timestamp_ms(),
        canonical_source_rank: 1,
        receipt_time_ms,
        connection_id: "recorded-pm".to_owned(),
        connection_epoch: 0,
        frame_sequence: i64::try_from(ordinal)?,
        ingest_sequence: source_ingest_sequence,
    };
    let envelope = PmMarketEnvelope {
        metadata: metadata.clone(),
        raw_frame: Vec::new(),
        fact,
    };

    Ok(RawCaptureDraft {
        schema_version: metadata.schema_version,
        source_id: metadata.source_id,
        canonical_source_rank: metadata.canonical_source_rank,
        receipt_time_ms,
        transport: Some(CaptureTransport {
            source_time_ms: Some(metadata.source_time_ms),
            connection_id: metadata.connection_id,
            connection_epoch: 0,
            frame_sequence: ordinal,
            ingest_sequence: source_ingest_sequence,
        }),
        normalized_projection: Some(pmkit_tape::market_envelope_json(&envelope)),
        payload: RawCapturePayload::Frame { raw_bytes },
    })
}

fn native_binance_trade_capture(
    source_time_ms: i64,
    receipt_time_ms: i64,
) -> Result<RawCaptureDraft, Box<dyn std::error::Error>> {
    const SOURCE_ID: &str = "binance:aggTrade";
    const RAW_FRAME: &str =
        r#"{"e":"aggTrade","a":7,"p":"0.6","q":"1","f":7,"l":7,"T":50,"m":false}"#;
    let source_ingest_sequence = 1;
    let fact = parse_binance_agg_trade_live(RAW_FRAME, Asset::Btc)?;
    let envelope = CexReferenceEnvelope {
        metadata: StreamMetadata {
            schema_version: 3,
            source_id: SOURCE_ID.to_owned(),
            source_time_ms,
            canonical_source_rank: 1,
            receipt_time_ms,
            connection_id: "recorded-binance-btcusdt".to_owned(),
            connection_epoch: 0,
            frame_sequence: 0,
            ingest_sequence: source_ingest_sequence,
        },
        raw_frame: RAW_FRAME.as_bytes().to_vec(),
        fact,
    };

    Ok(RawCaptureDraft {
        schema_version: envelope.metadata.schema_version,
        source_id: SOURCE_ID.to_owned(),
        canonical_source_rank: envelope.metadata.canonical_source_rank,
        receipt_time_ms,
        transport: Some(CaptureTransport {
            source_time_ms: Some(source_time_ms),
            connection_id: envelope.metadata.connection_id.clone(),
            connection_epoch: 0,
            frame_sequence: 0,
            ingest_sequence: source_ingest_sequence,
        }),
        normalized_projection: Some(pmkit_tape::reference_envelope_json(&envelope)),
        payload: RawCapturePayload::Frame {
            raw_bytes: envelope.raw_frame,
        },
    })
}

fn market_fact_probe_run(
    id: &str,
    replay: ReplaySpec,
    market: MarketId,
    observed: Arc<Mutex<Vec<(StrategyFact, i64)>>>,
) -> Result<BacktestRun, Box<dyn std::error::Error>> {
    Ok(BacktestRun::new(
        RunId::new(id)?,
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
            market_limits: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("market-fact-probe")?,
        market,
        Arc::new(RecordedFactProbeFactory(observed)),
    )))
}

fn backtest_run() -> Result<BacktestRun, Box<dyn std::error::Error>> {
    backtest_run_with_limits(None)
}

fn backtest_run_with_limits(
    market_limits: Option<MarketLimits>,
) -> Result<BacktestRun, Box<dyn std::error::Error>> {
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
            market_limits,
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
#[allow(clippy::significant_drop_tightening)]
async fn backtest_same_timestamp_outcomes_have_distinct_decision_identities()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(TursoTapeStore::open_local(directory.path().join("outcomes.db")).await?);
    let replay = ReplaySpec::new(
        Arc::new(BothOutcomesHistory),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    );
    let run = BacktestRun::new(
        RunId::new("backtest-outcomes")?,
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
    let RunReport::Backtest(report) = app.wait_for(RunId::new("backtest-outcomes")?).await? else {
        return Err("expected a backtest report".into());
    };
    assert_eq!(report.events_processed, 2);

    let scope = OwnerScope::new(
        PortfolioId::new("research")?,
        RunId::new("backtest-outcomes")?,
    );
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
async fn backtest_delivers_reference_facts_with_latest_market_context()
-> Result<(), Box<dyn std::error::Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let nonempty_books = Arc::new(AtomicUsize::new(0));
    let replay = ReplaySpec::new(
        Arc::new(ScriptedHistory { ticks: vec![1, 2] }),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    )
    .reference_source(Arc::new(ScriptedReference));
    let run = BacktestRun::new(
        RunId::new("bt-reference")?,
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
            market_limits: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("reference-buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(ReferenceBuyerFactory {
            calls: Arc::clone(&calls),
            nonempty_books: Arc::clone(&nonempty_books),
        }),
    ));

    let app = Pmkit::builder(config()?).run(run).start().await?;
    let RunReport::Backtest(report) = app.wait_for(RunId::new("bt-reference")?).await? else {
        return Err("expected a backtest report".into());
    };

    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(nonempty_books.load(Ordering::Relaxed), 1);
    assert_eq!(
        report.fills, 1,
        "reference actions should reach the simulator"
    );
    Ok(())
}

#[tokio::test]
async fn historical_backtest_counts_all_market_facts_once_without_non_book_callbacks()
-> Result<(), Box<dyn std::error::Error>> {
    let market = MarketId::new("btc-5m")?;
    let observed = Arc::new(Mutex::new(Vec::new()));
    let replay = ReplaySpec::new(
        Arc::new(MarketFactsHistory {
            market: market.clone(),
        }),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    );
    let run = market_fact_probe_run("bt-market-facts", replay, market, Arc::clone(&observed))?;
    let app = Pmkit::builder(config()?).run(run).start().await?;
    let RunReport::Backtest(report) = app.wait_for(RunId::new("bt-market-facts")?).await? else {
        return Err("expected a backtest report".into());
    };

    assert_eq!(report.events_processed, 3);
    assert_eq!(report.metrics.events_processed, 3);
    assert_eq!(report.metrics.decisions, 1);
    let observed = observed
        .lock()
        .map_err(|_| "recorded fact probe lock poisoned")?
        .clone();
    assert_eq!(observed.len(), 1);
    assert!(matches!(
        &observed[0].0,
        StrategyFact::Market(MarketEvent::BookUpdate { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn recorded_backtest_counts_all_native_market_facts_once_without_non_book_callbacks()
-> Result<(), Box<dyn std::error::Error>> {
    let market = MarketId::new("btc-5m")?;
    let mut writer = RawCaptureWriter::new(Vec::new());
    writer.append(native_book_capture(
        &market,
        100,
        1_000,
        Decimal::new(4, 1),
        0,
    )?)?;
    writer.append(native_market_capture(
        MarketEvent::BestBidAsk {
            market: market.clone(),
            outcome: Outcome::Up,
            bid: Decimal::new(50, 2),
            ask: Decimal::new(51, 2),
            timestamp_ms: 101,
        },
        1_100,
        1,
        2,
        br#"{"event_type":"best_bid_ask","market":"0x0000000000000000000000000000000000000000000000000000000000000001","asset_id":"1","best_bid":"0.50","best_ask":"0.51","spread":"0.01","timestamp":"101"}"#.to_vec(),
    )?)?;
    writer.append(native_market_capture(
        MarketEvent::LastTrade {
            market: market.clone(),
            outcome: Outcome::Up,
            price: Decimal::new(505, 3),
            side: pmkit_book::Side::Sell,
            size: Decimal::new(125, 2),
            timestamp_ms: 102,
        },
        1_200,
        2,
        3,
        br#"{"event_type":"last_trade_price","market":"0x0000000000000000000000000000000000000000000000000000000000000001","asset_id":"1","price":"0.505","side":"SELL","size":"1.25","timestamp":"102"}"#.to_vec(),
    )?)?;
    let capture = writer.into_inner();

    let observed = Arc::new(Mutex::new(Vec::new()));
    let replay = ReplaySpec::new(
        Arc::new(ScriptedHistory { ticks: Vec::new() }),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    );
    let run = market_fact_probe_run(
        "bt-recorded-market-facts",
        replay,
        market.clone(),
        Arc::clone(&observed),
    )?;
    let tokens = MarketTokens::new(market.clone(), "1".parse()?, "2".parse()?);
    let report = crate::RecordedBacktest::replay_capture(
        &run,
        crate::capture_replay::CaptureReplaySource::new_with_market_tokens(
            Cursor::new(capture),
            HashMap::from([(market, tokens)]),
        ),
    )
    .drive(None)
    .await?;

    assert_eq!(report.events_processed, 3);
    assert_eq!(report.metrics.events_processed, 3);
    assert_eq!(report.metrics.decisions, 1);
    let observed = observed
        .lock()
        .map_err(|_| "recorded fact probe lock poisoned")?
        .clone();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].1, 100);
    assert!(matches!(
        &observed[0].0,
        StrategyFact::Market(MarketEvent::BookUpdate {
            timestamp_ms: 100,
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn recorded_backtest_preserves_native_market_order_and_strategy_clock()
-> Result<(), Box<dyn std::error::Error>> {
    let market = MarketId::new("btc-5m")?;
    let mut writer = RawCaptureWriter::new(Vec::new());
    writer.append(native_book_capture(
        &market,
        100,
        1_000,
        Decimal::new(4, 1),
        0,
    )?)?;
    writer.append(native_binance_trade_capture(50, 1_100)?)?;
    let capture = writer.into_inner();

    let observed = Arc::new(Mutex::new(Vec::new()));
    let replay = ReplaySpec::new(
        Arc::new(ScriptedHistory { ticks: Vec::new() }),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    );
    let run = BacktestRun::new(
        RunId::new("bt-recorded-order")?,
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
            market_limits: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("recorded-fact-probe")?,
        market.clone(),
        Arc::new(RecordedFactProbeFactory(Arc::clone(&observed))),
    ));
    let tokens = MarketTokens::new(market.clone(), "1".parse()?, "2".parse()?);
    let report = crate::RecordedBacktest::replay_capture(
        &run,
        crate::capture_replay::CaptureReplaySource::new_with_market_tokens(
            Cursor::new(capture),
            HashMap::from([(market.clone(), tokens)]),
        ),
    )
    .drive(None)
    .await?;

    assert_eq!(report.events_processed, 1);
    let observed = observed
        .lock()
        .map_err(|_| "recorded fact probe lock poisoned")?;
    assert_eq!(observed.len(), 2);
    assert_eq!(observed[0].1, 100);
    let StrategyFact::Market(MarketEvent::BookUpdate {
        market: fact_market,
        outcome: Outcome::Up,
        asks,
        timestamp_ms,
        ..
    }) = &observed[0].0
    else {
        return Err(format!(
            "expected a native PM book fact first, got {:?}",
            observed[0].0
        )
        .into());
    };
    assert_eq!(fact_market, &market);
    assert_eq!(*timestamp_ms, 100);
    assert_eq!(asks[0].0, Decimal::new(4, 1));

    assert_eq!(observed[1].1, 50);
    let StrategyFact::Reference(CexReferenceEvent::Trade {
        asset: Asset::Btc,
        exchange: Exchange::Binance,
        price,
        timestamp_ms,
        ..
    }) = &observed[1].0
    else {
        return Err(format!(
            "expected a Binance reference trade second, got {:?}",
            observed[1].0
        )
        .into());
    };
    assert_eq!(*timestamp_ms, 50);
    assert_eq!(*price, Decimal::new(6, 1));
    drop(observed);
    Ok(())
}

#[tokio::test]
async fn metrics_match_report() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a completed backtest with two PM book events and one CEX reference fact.
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
    assert_eq!(metrics.decisions, 3);
    assert_eq!(report.metrics, *metrics);
    Ok(())
}

#[tokio::test]
async fn venue_limit_refusal_is_counted_not_reported_as_placed()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a backtest whose venue minimum sits above the only order the
    // strategy places (ten shares at 0.50, itself on the tick grid).
    let run = backtest_run_with_limits(Some(MarketLimits {
        min_order_size: Decimal::from(50),
        tick_size: Decimal::new(1, 2),
    }))?;
    let dir = tempfile::tempdir()?;
    let store = Arc::new(TursoTapeStore::open_local(dir.path().join("causal-rejection.db")).await?);
    let app = Pmkit::builder(config()?)
        .storage(store.clone())
        .run(run)
        .start()
        .await?;
    let run_id = RunId::new("bt")?;

    // When: the run completes.
    let RunReport::Backtest(report) = app.wait_for(run_id.clone()).await? else {
        return Err("expected a backtest report".into());
    };

    // Then: the engine's refusal is counted rather than silently dropped.
    // Counting it as placed would move the optimistic bias off the fill and
    // onto the report: one placement, zero fills, and no reason why.
    assert_eq!(report.metrics.rejected, 1);
    assert_eq!(report.fills, 0);
    let metrics = app.metrics(&run_id).ok_or("missing run metrics")?;
    assert_eq!(metrics.rejected, 1);
    let decisions = store
        .read_decisions(&OwnerScope::new(PortfolioId::new("research")?, run_id))
        .await?;
    assert!(decisions.iter().any(|decision| {
        decision.payload["decision"]["kind"] == "actions"
            && decision.payload["decision"]["risk"][0]["verdict"]["kind"] == "rejected"
            && decision.payload["decision"]["risk"][0]["verdict"]["reason"]
                == "simulation/execution rejection"
    }));
    drop(store);
    Ok(())
}

#[tokio::test]
async fn backtest_cancel_all_releases_strategy_orders() -> Result<(), Box<dyn std::error::Error>> {
    let replay = ReplaySpec::new(
        Arc::new(ScriptedHistory { ticks: vec![1, 2] }),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    );
    let run = BacktestRun::new(
        RunId::new("cancel-all")?,
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
            market_limits: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("cancel-maker")?,
        MarketId::new("btc-5m")?,
        Arc::new(RestThenCancelFactory),
    ));

    let app = Pmkit::builder(config()?).run(run).start().await?;
    let RunReport::Backtest(report) = app.wait_for(RunId::new("cancel-all")?).await? else {
        return Err("expected a backtest report".into());
    };
    assert_eq!(report.fills, 0);
    assert_eq!(report.exposure.portfolio_notional, Decimal::ZERO);
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
            market_limits: None,
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

    // Two strategies evaluate two PM books and one reference fact each.
    assert_eq!(metrics.decisions, 6);
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
            market_limits: None,
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
            market_limits: None,
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
    // Given: a store-backed backtest over two scripted book events and one reference fact.
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
            market_limits: None,
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

    // Then: each book and reference evaluation is recorded once, owner-scoped.
    let scope = OwnerScope::new(PortfolioId::new("research")?, RunId::new("bt-rec")?);
    let decisions = store.read_decisions(&scope).await?;
    assert_eq!(decisions.len(), 3);
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
#[allow(clippy::significant_drop_tightening)]
async fn backtest_multi_strategy_replace_quotes_persist_strategy_and_suborder_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let store = Arc::new(
        TursoTapeStore::open_local(directory.path().join("backtest-multi-identities.db")).await?,
    );
    let replay = ReplaySpec::new(
        Arc::new(ScriptedHistory { ticks: vec![1] }),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    );
    let run = BacktestRun::new(
        RunId::new("backtest-multi-identities")?,
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
    let RunReport::Backtest(report) = app
        .wait_for(RunId::new("backtest-multi-identities")?)
        .await?
    else {
        return Err("expected a backtest report".into());
    };
    assert_eq!(report.metrics.rejected, 0);

    let scope = OwnerScope::new(
        PortfolioId::new("research")?,
        RunId::new("backtest-multi-identities")?,
    );
    let decisions = store.read_decisions(&scope).await?;
    assert_eq!(decisions.len(), 2);
    let mut decision_ids = decisions
        .iter()
        .map(|decision| decision.identity.correlation_id.clone())
        .collect::<Vec<_>>();
    decision_ids.sort_unstable();
    assert!(decision_ids.windows(2).all(|pair| pair[0] != pair[1]));

    let mut suborder_ids = Vec::new();
    for strategy in ["alpha", "beta"] {
        let decision = decisions
            .iter()
            .find(|decision| {
                decision
                    .identity
                    .correlation_id
                    .contains(&format!(":{strategy}:market:"))
            })
            .ok_or("missing strategy-scoped decision")?;
        let verdicts = decision.payload["decision"]["risk"]
            .as_array()
            .ok_or("missing backtest risk verdicts")?;
        assert_eq!(verdicts.len(), 2);
        assert_eq!(verdicts[0]["action_index"], 0);
        assert_eq!(verdicts[1]["action_index"], 1);
        assert!(
            verdicts
                .iter()
                .all(|verdict| verdict["verdict"]["kind"] == "accepted")
        );
        suborder_ids.extend(verdicts.iter().map(|verdict| {
            format!(
                "{}:{}",
                decision.identity.correlation_id, verdict["action_index"]
            )
        }));
    }
    suborder_ids.sort_unstable();
    assert_eq!(suborder_ids.len(), 4);
    assert!(suborder_ids.windows(2).all(|pair| pair[0] != pair[1]));
    drop(store);
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

#[path = "backtest_tests/recorded_receipt.rs"]
mod recorded_receipt;

#[path = "backtest_tests/recorded_oracle.rs"]
mod recorded_oracle;

#[path = "backtest_tests/recorded_oracle_eof.rs"]
mod recorded_oracle_eof;

#[path = "backtest_tests/recorded_reference_source.rs"]
mod recorded_reference_source;

#[path = "backtest_tests/recorded_account.rs"]
mod recorded_account;
