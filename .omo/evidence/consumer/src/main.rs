use std::{
    error::Error,
    num::NonZeroUsize,
    path::PathBuf,
    sync::{Arc, atomic::{AtomicBool, Ordering}},
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use pmkit::causal::{CausalRecorder, RecorderError};
use pmkit::feed::{FeedMode, MergedFeed, SourceDefinition};
use pmkit_book::Side;
use pmkit_core::{MarketId, PortfolioId, RunId};
use pmkit_data::{DataSourceError, RawPmAccountFrame, RawPmMarketFrame, SourceSignal};
use pmkit_event::{CexReferenceEnvelope, CexReferenceEvent, MarketEvent, PmAccountEvent, SourceEnvelope, StreamMetadata};
use pmkit_exec::{OrderId, PlaceOrder};
use pmkit_market::{Asset, Exchange, Outcome};
use pmkit_polymarket::RawPolymarketFrameAdapter;
use pmkit_store::{CausalDecision, CausalIdentity, IntentOutcome, OwnerScope, PmEnvelope, ReplayCursor, ReplayItem, ReplayPage, StoreError, TapeStore, TursoTapeStore};
use rust_decimal::Decimal;
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    if std::env::args().any(|argument| argument == "--store-fails") {
        return pre_submission_failure().await;
    }
    if std::env::args().any(|argument| argument == "--missing-cache") {
        return missing_cache_replay_gap().await;
    }
    let mut expected = None;
    for (name, mode) in [
        ("backtest", FeedMode::Backtest),
        ("paper", FeedMode::Paper),
        ("live", FeedMode::LiveFixture),
    ] {
        let snapshot = run_mode(name, mode).await?;
        if let Some(expected) = &expected {
            if snapshot != *expected {
                return Err("mode snapshot mismatch".into());
            }
        } else {
            expected = Some(snapshot);
        }
    }
    println!("todo7 consumer: backtest/paper/live snapshots match");
    Ok(())
}

async fn run_mode(name: &str, mode: FeedMode) -> Result<Vec<String>, Box<dyn Error>> {
    let path = database_path(name)?;
    let store = Arc::new(TursoTapeStore::open_local(&path).await?);
    let scope = scope(name)?;
    let adapter = RawPolymarketFrameAdapter::new(store.clone(), scope.clone(), "fixture");
    let market_text = br#"{"event_type":"book","seq":1}"#.to_vec();
    let market = adapter.market(
        RawPmMarketFrame { metadata: metadata("market", 10, 1), text: market_text.clone() },
        |_| Ok(book()?),
    ).await?;
    let account_text = br#"{"event_type":"order","seq":2}"#.to_vec();
    adapter.account(
        RawPmAccountFrame { portfolio: PortfolioId::new("alice")?, metadata: metadata("account", 10, 2), text: account_text.clone() },
        |_| Ok(PmAccountEvent::OrderAck { strategy: None, order_id: "order-1".into(), timestamp_ms: 10 }),
    ).await?;
    store.store_decision(&CausalDecision {
        identity: CausalIdentity { scope: scope.clone(), correlation_id: "fixture:1".into(), source_timestamp_ms: 10, ingest_sequence: 1 },
        payload: json!({"kind":"no_action"}),
    }).await?;
    let page = store.read_envelopes(&scope, None, NonZeroUsize::new(8).ok_or("nonzero")?).await?;
    let stored: Vec<&PmEnvelope> = page.items.iter().map(|item| match item {
        ReplayItem::Envelope(envelope) => Ok(envelope),
        ReplayItem::Gap(_) => Err("unexpected replay gap"),
    }).collect::<Result<_, _>>()?;
    if stored.len() != 2 || stored[0].raw_frame != market_text || stored[1].raw_frame != account_text {
        return Err("raw adapter bytes or canonical PM order changed".into());
    }
    let facts = MergedFeed::from_fixture(mode, vec![
        SourceDefinition::finite("pm", vec![SourceSignal::Data(Box::new(SourceEnvelope::PmMarket(market))), SourceSignal::Watermark(20), SourceSignal::Eof]),
        SourceDefinition::finite("cex", vec![cex(), SourceSignal::Watermark(20), SourceSignal::Eof]),
    ], Some(20)).collect().await?;
    store.delete_database()?;
    Ok(facts.into_iter().map(|fact| format!("{fact:?}")).collect())
}

async fn pre_submission_failure() -> Result<(), Box<dyn Error>> {
    let submitted = Arc::new(AtomicBool::new(false));
    let recorder = CausalRecorder::new(&FailingStore);
    let identity = CausalIdentity { scope: scope("failure")?, correlation_id: "before-submit".into(), source_timestamp_ms: 10, ingest_sequence: 1 };
    let intent = recorder.intent(&identity, 0, &PlaceOrder { market: MarketId::new("btc-5m")?, outcome: Outcome::Up, side: Side::Buy, price: Decimal::new(50, 2), qty: Decimal::ONE, post_only: false });
    let result = recorder.submit(&intent, {
        let submitted = Arc::clone(&submitted);
        move || async move { submitted.store(true, Ordering::Relaxed); Ok(OrderId("must-not-submit".into())) }
    }).await;
    if !matches!(result, Err(RecorderError::Store(_))) || submitted.load(Ordering::Relaxed) {
        return Err("configured pre-submit store failure did not abort submission".into());
    }
    println!("todo7 consumer: typed pre-submission storage failure aborted submission");
    Ok(())
}

fn scope(name: &str) -> Result<OwnerScope, Box<dyn Error>> { Ok(OwnerScope::new(PortfolioId::new("alice")?, RunId::new(name)?)) }
fn metadata(source: &str, timestamp: i64, sequence: i64) -> StreamMetadata { StreamMetadata { schema_version: 1, source_id: source.into(), source_time_ms: timestamp, canonical_source_rank: 0, receipt_time_ms: timestamp, connection_id: "fixture".into(), connection_epoch: 1, frame_sequence: sequence, ingest_sequence: u64::try_from(sequence).unwrap_or_default() } }
fn book() -> Result<MarketEvent, DataSourceError> { Ok(MarketEvent::BookUpdate { market: MarketId::new("btc-5m").map_err(|_| DataSourceError::NotAvailable)?, outcome: Outcome::Up, bids: vec![(Decimal::new(49, 2), Decimal::ONE)], asks: vec![(Decimal::new(51, 2), Decimal::ONE)], timestamp_ms: 10 }) }
fn cex() -> SourceSignal { SourceSignal::Data(Box::new(SourceEnvelope::CexReference(CexReferenceEnvelope { metadata: metadata("binance", 10, 7), fact: CexReferenceEvent::Trade { asset: Asset::Btc, exchange: Exchange::Binance, aggregate_trade_id: 7, price: Decimal::new(100_000, 2), qty: Decimal::ONE, is_buyer_maker: false, timestamp_ms: 10 } }))) }
fn database_path(name: &str) -> Result<PathBuf, Box<dyn Error>> { Ok(std::env::temp_dir().join(format!("pmkit-todo7-{name}-{}.db", SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()))) }

struct FailingStore;
#[async_trait]
impl TapeStore for FailingStore {
    async fn store_envelope(&self, _: &PmEnvelope) -> Result<(), StoreError> { Err(failure()) }
    async fn read_envelopes(&self, _: &OwnerScope, _: Option<ReplayCursor>, _: NonZeroUsize) -> Result<ReplayPage, StoreError> { Err(failure()) }
    async fn store_decision(&self, _: &CausalDecision) -> Result<(), StoreError> { Err(failure()) }
    async fn store_intent_pending(&self, _: &CausalIdentity, _: &serde_json::Value) -> Result<(), StoreError> { Err(failure()) }
    async fn transition_intent(&self, _: &CausalIdentity, _: IntentOutcome) -> Result<(), StoreError> { Err(failure()) }
}
fn failure() -> StoreError { StoreError::Storage { message: "fixture failure".into() } }

async fn missing_cache_replay_gap() -> Result<(), Box<dyn Error>> {
    let cache = pmkit_data::VerifiedBinanceArchiveCache::new(
        std::env::temp_dir().join("pmkit-missing-cache-test"),
        pmkit_data::CachePolicy::Bounded { max_bytes: 1024 * 1024 },
    );
    let result = cache.replay(Asset::Btc, "2099-01-01".parse()?).await;
    let Err(DataSourceError::ReplayGap { .. }) = result else {
        return Err(format!("expected ReplayGap, got: {result:?}").into());
    };
    println!("todo8 consumer: missing-cache yields ReplayGap");
    Ok(())
}
