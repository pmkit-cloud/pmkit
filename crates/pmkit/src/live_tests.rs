use crate::{
    Pmkit, StartError, live,
    test_support::{BuyFactory, config, risk},
};
use async_trait::async_trait;
use pmkit_book::Side;
use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
use pmkit_data::{
    DataSourceError, LiveAccountDataSource, LiveCexDataSource, LiveDataSource, SourceSignal,
};
use pmkit_event::{
    CexReferenceEnvelope, CexReferenceEvent, Liquidity, MarketEvent, PmAccountEnvelope,
    PmAccountEvent, SourceEnvelope, StrategyFact, StreamMetadata,
};
use pmkit_exec::{ExecError, ExecutionSnapshot, Executor, OrderId, PlaceOrder};
use pmkit_market::{Asset, Exchange, Outcome};
use pmkit_runtime::{LiveOrderPolicy, StrategyRegistration};
use pmkit_spec::LiveRun;
use pmkit_store::{OwnerScope, TapeStore, TursoTapeStore};
use pmkit_strategy::{
    Actions, Strategy, StrategyContext, StrategyError, StrategyFactory, StrategyInitError,
};
use rust_decimal::Decimal;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::mpsc::Sender;

#[path = "live_loss_tests.rs"]
mod live_loss_tests;
#[path = "live_tape_tests.rs"]
mod live_tape_tests;

struct RecordingExec;

#[derive(Default)]
struct ReferenceExec {
    submissions: AtomicUsize,
}

struct ReferenceLive;

struct ReferenceStrategy {
    calls: Arc<AtomicUsize>,
    nonempty_books: Arc<AtomicUsize>,
}

struct ReferenceFactory {
    calls: Arc<AtomicUsize>,
    nonempty_books: Arc<AtomicUsize>,
}

#[derive(Default)]
struct RejectedExec {
    submissions: AtomicUsize,
}

#[async_trait]
impl Executor for RecordingExec {
    async fn preflight(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(ExecutionSnapshot::default())
    }

    async fn reconcile(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(ExecutionSnapshot::default())
    }

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

#[async_trait]
impl Executor for ReferenceExec {
    async fn preflight(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(ExecutionSnapshot::default())
    }

    async fn reconcile(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(ExecutionSnapshot::default())
    }

    async fn submit(&self, _order: &PlaceOrder, _now_ms: i64) -> Result<OrderId, ExecError> {
        self.submissions.fetch_add(1, Ordering::Relaxed);
        Ok(OrderId("reference-order".to_owned()))
    }

    async fn cancel(&self, _order_id: &OrderId) -> Result<(), ExecError> {
        Ok(())
    }

    async fn cancel_all(&self) -> Result<(), ExecError> {
        Ok(())
    }
}

impl Strategy for ReferenceStrategy {
    fn on_event(&mut self, context: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        if !matches!(context.fact, StrategyFact::Reference(_)) {
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
        Ok(Actions::place(PlaceOrder {
            market: context.market.clone(),
            outcome: Outcome::Up,
            side: Side::Buy,
            price: Decimal::new(46, 2),
            qty: Decimal::ONE,
            post_only: false,
            tif: pmkit_exec::TimeInForce::Gtc,
        }))
    }
}

impl StrategyFactory for ReferenceFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(ReferenceStrategy {
            calls: Arc::clone(&self.calls),
            nonempty_books: Arc::clone(&self.nonempty_books),
        }))
    }
}

#[async_trait]
impl Executor for RejectedExec {
    async fn preflight(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(ExecutionSnapshot::default())
    }

    async fn reconcile(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(ExecutionSnapshot::default())
    }

    async fn submit(&self, _order: &PlaceOrder, _now_ms: i64) -> Result<OrderId, ExecError> {
        self.submissions.fetch_add(1, Ordering::Relaxed);
        Err(ExecError::Rejected {
            reason: "venue rejected order".to_owned(),
        })
    }

    async fn cancel(&self, _order_id: &OrderId) -> Result<(), ExecError> {
        Ok(())
    }

    async fn cancel_all(&self) -> Result<(), ExecError> {
        Ok(())
    }
}

#[derive(Default)]
struct CapacityExec {
    releases_capacity: bool,
    reconciles: AtomicUsize,
    snapshots: AtomicUsize,
    submits: AtomicUsize,
}

#[async_trait]
impl Executor for CapacityExec {
    async fn preflight(&self) -> Result<ExecutionSnapshot, ExecError> {
        self.snapshots.fetch_add(1, Ordering::Relaxed);
        Ok(ExecutionSnapshot {
            open_orders: vec![OrderId("existing".to_owned())],
        })
    }

    async fn reconcile(&self) -> Result<ExecutionSnapshot, ExecError> {
        self.snapshots.fetch_add(1, Ordering::Relaxed);
        let reconcile = self.reconciles.fetch_add(1, Ordering::Relaxed);
        if self.releases_capacity && reconcile > 0 {
            return Ok(ExecutionSnapshot::default());
        }
        Ok(ExecutionSnapshot {
            open_orders: vec![OrderId("existing".to_owned())],
        })
    }

    async fn submit(&self, _order: &PlaceOrder, _now_ms: i64) -> Result<OrderId, ExecError> {
        self.submits.fetch_add(1, Ordering::Relaxed);
        Ok(OrderId("new".to_owned()))
    }

    async fn cancel(&self, _order_id: &OrderId) -> Result<(), ExecError> {
        Ok(())
    }

    async fn cancel_all(&self) -> Result<(), ExecError> {
        Ok(())
    }
}

#[derive(Default)]
struct TransportExec {
    reconciles: AtomicUsize,
}

struct SlowReconcileExec;

#[async_trait]
impl Executor for SlowReconcileExec {
    async fn preflight(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(ExecutionSnapshot::default())
    }

    async fn reconcile(&self) -> Result<ExecutionSnapshot, ExecError> {
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(ExecutionSnapshot::default())
    }

    async fn submit(&self, _order: &PlaceOrder, _now_ms: i64) -> Result<OrderId, ExecError> {
        Ok(OrderId("slow-reconcile-order".to_owned()))
    }

    async fn cancel(&self, _order_id: &OrderId) -> Result<(), ExecError> {
        Ok(())
    }

    async fn cancel_all(&self) -> Result<(), ExecError> {
        Ok(())
    }
}

#[derive(Default)]
struct ShutdownExec {
    cancels: AtomicUsize,
    cancel_all_calls: AtomicUsize,
}

#[async_trait]
impl Executor for ShutdownExec {
    async fn preflight(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(ExecutionSnapshot::default())
    }

    async fn reconcile(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(ExecutionSnapshot::default())
    }

    async fn submit(&self, _order: &PlaceOrder, _now_ms: i64) -> Result<OrderId, ExecError> {
        Ok(OrderId("owned-order".to_owned()))
    }

    async fn cancel(&self, _order_id: &OrderId) -> Result<(), ExecError> {
        self.cancels.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn cancel_all(&self) -> Result<(), ExecError> {
        self.cancel_all_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl Executor for TransportExec {
    async fn preflight(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(ExecutionSnapshot::default())
    }

    async fn reconcile(&self) -> Result<ExecutionSnapshot, ExecError> {
        self.reconciles.fetch_add(1, Ordering::Relaxed);
        Ok(ExecutionSnapshot::default())
    }

    async fn submit(&self, _order: &PlaceOrder, _now_ms: i64) -> Result<OrderId, ExecError> {
        Err(ExecError::Transport {
            message: "response lost".to_owned(),
        })
    }

    async fn cancel(&self, _order_id: &OrderId) -> Result<(), ExecError> {
        Ok(())
    }

    async fn cancel_all(&self) -> Result<(), ExecError> {
        Ok(())
    }
}

struct LiveWithFill;

struct LiveWithBook;

struct LiveWithDuplicatePartialFill;

struct MismatchedAccountSource;

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
impl LiveCexDataSource for ReferenceLive {
    async fn subscribe_reference(&self, sink: Sender<SourceSignal>) -> Result<(), DataSourceError> {
        sink.send(SourceSignal::Data(Box::new(SourceEnvelope::CexReference(
            CexReferenceEnvelope {
                metadata: StreamMetadata {
                    schema_version: 1,
                    source_id: "binance-live".into(),
                    source_time_ms: 2,
                    canonical_source_rank: 1,
                    receipt_time_ms: 2,
                    connection_id: "binance-live".into(),
                    connection_epoch: 0,
                    frame_sequence: 7,
                    ingest_sequence: 7,
                },
                fact: CexReferenceEvent::Trade {
                    asset: Asset::Btc,
                    exchange: Exchange::Binance,
                    aggregate_trade_id: 7,
                    price: Decimal::from(42),
                    qty: Decimal::ONE,
                    is_buyer_maker: false,
                    timestamp_ms: 2,
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
impl LiveDataSource for LiveWithFill {
    async fn subscribe(
        &self,
        market: MarketId,
        outcome: Outcome,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        if outcome == Outcome::Up {
            sink.send(SourceSignal::market_event(MarketEvent::BookUpdate {
                market: market.clone(),
                outcome,
                bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
                asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
                timestamp_ms: 1,
            }))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
            sink.send(SourceSignal::market_event(MarketEvent::Fill {
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
impl LiveDataSource for LiveWithBook {
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
            .map_err(|_| DataSourceError::SinkClosed)
    }
}

#[async_trait]
impl LiveDataSource for LiveWithDuplicatePartialFill {
    async fn subscribe(
        &self,
        market: MarketId,
        outcome: Outcome,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        if outcome == Outcome::Up {
            sink.send(SourceSignal::market_event(MarketEvent::BookUpdate {
                market: market.clone(),
                outcome,
                bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
                asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
                timestamp_ms: 1,
            }))
            .await
            .map_err(|_| DataSourceError::SinkClosed)?;
            for timestamp_ms in [2, 2] {
                sink.send(SourceSignal::market_event(MarketEvent::Fill {
                    strategy: None,
                    order_id: "owned-order".to_owned(),
                    market: market.clone(),
                    outcome,
                    price: Decimal::new(50, 2),
                    size: Decimal::from(3),
                    side: Side::Buy,
                    fee: Decimal::ZERO,
                    liquidity: Liquidity::Taker,
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
    let report = live::drive(&live_run()?, &config()?).await?;
    assert_eq!(report.events_processed, 2);
    assert_eq!(report.fills, 1);
    assert_eq!(report.rejected, 0);
    assert!(report.exposure.portfolio_notional > Decimal::ZERO);
    Ok(())
}

#[tokio::test]
async fn live_run_routes_reference_facts_through_the_normal_order_pipeline()
-> Result<(), Box<dyn std::error::Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let nonempty_books = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(ReferenceExec::default());
    let run = LiveRun::new(
        RunId::new("live-reference")?,
        PortfolioId::new("alice")?,
        executor.clone(),
        Arc::new(LiveWithBook),
        risk()?,
    )
    .reference_data(Arc::new(ReferenceLive))
    .strategy(StrategyRegistration::new(
        StrategyId::new("reference-buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(ReferenceFactory {
            calls: Arc::clone(&calls),
            nonempty_books: Arc::clone(&nonempty_books),
        }),
    ));

    let report = live::drive(&run, &config()?).await?;

    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(nonempty_books.load(Ordering::Relaxed), 0);
    assert_eq!(executor.submissions.load(Ordering::Relaxed), 1);
    assert_eq!(report.rejected, 0);
    Ok(())
}

#[tokio::test]
async fn live_reference_actions_cannot_bypass_the_risk_gate()
-> Result<(), Box<dyn std::error::Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(ReferenceExec::default());
    let mut limits = risk()?;
    limits.max_order_notional = pmkit_money::Money::ZERO;
    let run = LiveRun::new(
        RunId::new("live-reference-risk")?,
        PortfolioId::new("alice")?,
        executor.clone(),
        Arc::new(LiveWithBook),
        limits,
    )
    .reference_data(Arc::new(ReferenceLive))
    .strategy(StrategyRegistration::new(
        StrategyId::new("reference-buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(ReferenceFactory {
            calls: Arc::clone(&calls),
            nonempty_books: Arc::new(AtomicUsize::new(0)),
        }),
    ));

    let report = live::drive(&run, &config()?).await?;

    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(executor.submissions.load(Ordering::Relaxed), 0);
    assert_eq!(report.rejected, 1);
    assert_eq!(report.exposure.portfolio_notional, Decimal::ZERO);
    Ok(())
}

#[tokio::test]
#[allow(clippy::significant_drop_tightening)]
async fn live_run_rejects_mismatched_account_owner() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a public account source emits an envelope owned by another portfolio.
    let run = LiveRun::new(
        RunId::new("live-owner-check")?,
        PortfolioId::new("alice")?,
        Arc::new(RecordingExec),
        Arc::new(LiveWithBook),
        risk()?,
    )
    .account_data(Arc::new(MismatchedAccountSource));
    let scope = OwnerScope::new(run.portfolio().clone(), run.id().clone());
    let dir = tempfile::tempdir()?;

    // When: the live runtime receives the foreign account envelope.
    let (result, page) = {
        let store = TursoTapeStore::open_local(dir.path().join("owner-check.db")).await?;
        let result = live::drive_with_store(&run, &config()?, Some(&store)).await;
        let page = store
            .read_envelopes(&scope, None, NonZeroUsize::MIN)
            .await?;
        store.delete_database()?;
        (result, page)
    };

    // Then: it fails before durable storage, tape, or ledger mutation.
    assert!(matches!(
        result,
        Err(StartError::Source {
            source: DataSourceError::ReplayGap { message },
            ..
        }) if message == "account event owner mallory does not match run alice"
    ));
    assert!(page.items.is_empty());
    Ok(())
}

#[tokio::test]
async fn live_run_without_consent_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let result = Pmkit::builder(config()?).run(live_run()?).start().await;
    assert!(matches!(result, Err(StartError::LiveConsentMissing(_))));
    Ok(())
}

#[tokio::test]
async fn live_run_preflights_and_rejects_at_open_order_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let executor = Arc::new(CapacityExec::default());
    let mut limits = risk()?;
    limits.max_open_orders = NonZeroU32::new(1).ok_or("nonzero")?;
    let run = LiveRun::new(
        RunId::new("live-capacity")?,
        PortfolioId::new("alice")?,
        executor.clone(),
        Arc::new(LiveWithFill),
        limits,
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));

    let report = live::drive(&run, &config()?).await?;

    assert_eq!(executor.snapshots.load(Ordering::Relaxed), 3);
    assert_eq!(executor.submits.load(Ordering::Relaxed), 0);
    assert_eq!(report.rejected, 1);
    Ok(())
}

#[tokio::test]
async fn live_run_counts_one_venue_rejection_once() -> Result<(), Box<dyn std::error::Error>> {
    // Given: one strategy action whose venue rejects its order.
    let executor = Arc::new(RejectedExec::default());
    let run = LiveRun::new(
        RunId::new("live-venue-rejection")?,
        PortfolioId::new("alice")?,
        executor.clone(),
        Arc::new(LiveWithBook),
        risk()?,
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));

    // When: the live driver reaches the executor boundary.
    let report = live::drive(&run, &config()?).await?;

    // Then: one venue rejection is reflected exactly once in public metrics.
    assert_eq!(executor.submissions.load(Ordering::Relaxed), 1);
    assert_eq!(report.rejected, 1);
    assert_eq!(report.metrics.rejected, 1);
    Ok(())
}

#[tokio::test]
async fn live_run_reconciles_capacity_before_rejecting() -> Result<(), Box<dyn std::error::Error>> {
    let executor = Arc::new(CapacityExec {
        releases_capacity: true,
        ..CapacityExec::default()
    });
    let mut limits = risk()?;
    limits.max_open_orders = NonZeroU32::new(1).ok_or("nonzero")?;
    let run = LiveRun::new(
        RunId::new("live-reconciled-capacity")?,
        PortfolioId::new("alice")?,
        executor.clone(),
        Arc::new(LiveWithFill),
        limits,
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));

    let report = live::drive(&run, &config()?).await?;

    assert_eq!(executor.submits.load(Ordering::Relaxed), 1);
    assert_eq!(report.rejected, 0);
    Ok(())
}

#[tokio::test]
async fn live_run_stops_after_transport_uncertainty() -> Result<(), Box<dyn std::error::Error>> {
    let executor = Arc::new(TransportExec::default());
    let run = LiveRun::new(
        RunId::new("live-transport")?,
        PortfolioId::new("alice")?,
        executor.clone(),
        Arc::new(LiveWithFill),
        risk()?,
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));

    let result = live::drive(&run, &config()?).await;

    assert!(matches!(result, Err(StartError::ExecutionState { .. })));
    assert_eq!(executor.reconciles.load(Ordering::Relaxed), 2);
    Ok(())
}

#[tokio::test]
async fn live_run_rejects_slow_reconciliation() -> Result<(), Box<dyn std::error::Error>> {
    let run = LiveRun::new(
        RunId::new("live-slow-reconcile")?,
        PortfolioId::new("alice")?,
        Arc::new(SlowReconcileExec),
        Arc::new(LiveWithFill),
        risk()?,
    );
    let mut runtime = config()?;
    runtime.shutdown.reconciliation_timeout = Duration::from_millis(1);

    let result = live::drive(&run, &runtime).await;

    assert!(matches!(
        result,
        Err(StartError::ExecutionState {
            source: ExecError::Transport { message },
            ..
        }) if message == "reconciliation timed out"
    ));
    Ok(())
}

#[tokio::test]
async fn live_run_cancels_owned_orders_on_shutdown() -> Result<(), Box<dyn std::error::Error>> {
    let executor = Arc::new(ShutdownExec::default());
    let run = LiveRun::new(
        RunId::new("live-cancel-owned")?,
        PortfolioId::new("alice")?,
        executor.clone(),
        Arc::new(LiveWithBook),
        risk()?,
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));
    let mut runtime = config()?;
    runtime.shutdown.live_orders = LiveOrderPolicy::CancelOwned;

    let report = live::drive(&run, &runtime).await?;

    assert_eq!(report.events_processed, 1);
    assert_eq!(report.exposure.portfolio_notional, Decimal::ZERO);
    assert_eq!(executor.cancels.load(Ordering::Relaxed), 1);
    assert_eq!(executor.cancel_all_calls.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
async fn live_report_keeps_exact_open_order_reservation_when_leaving_orders()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: one accepted ten-share order at a fifty-cent limit.
    let executor = Arc::new(ShutdownExec::default());
    let run = LiveRun::new(
        RunId::new("live-leave")?,
        PortfolioId::new("alice")?,
        executor.clone(),
        Arc::new(LiveWithBook),
        risk()?,
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));
    let mut runtime = config()?;
    runtime.shutdown.live_orders = LiveOrderPolicy::Leave;

    // When: shutdown deliberately leaves the owned order open.
    let report = live::drive(&run, &runtime).await?;

    // Then: the public report retains exactly the open order's 10 × 0.50 reservation.
    assert_eq!(report.exposure.portfolio_notional, Decimal::from(5));
    assert_eq!(executor.cancels.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
async fn duplicate_partial_fill_does_not_decrement_live_reservation_twice()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: one ten-share reservation and two identical three-share venue fills.
    let run = LiveRun::new(
        RunId::new("live-duplicate-partial")?,
        PortfolioId::new("alice")?,
        Arc::new(ShutdownExec::default()),
        Arc::new(LiveWithDuplicatePartialFill),
        risk()?,
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));
    let mut runtime = config()?;
    runtime.shutdown.live_orders = LiveOrderPolicy::Leave;

    // When: the live driver receives both deliveries.
    let report = live::drive(&run, &runtime).await?;

    // Then: one 3 × 0.45 position plus one 7 × 0.50 reservation remains.
    assert_eq!(report.fills, 1);
    assert_eq!(report.exposure.portfolio_notional, Decimal::new(485, 2));
    Ok(())
}

#[tokio::test]
async fn live_run_cancel_all_requires_explicit_policy() -> Result<(), Box<dyn std::error::Error>> {
    let executor = Arc::new(ShutdownExec::default());
    let run = LiveRun::new(
        RunId::new("live-cancel-all")?,
        PortfolioId::new("alice")?,
        executor.clone(),
        Arc::new(LiveWithFill),
        risk()?,
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));
    let mut runtime = config()?;
    runtime.shutdown.live_orders = LiveOrderPolicy::CancelAllExplicit;

    live::drive(&run, &runtime).await?;

    assert_eq!(executor.cancels.load(Ordering::Relaxed), 0);
    assert_eq!(executor.cancel_all_calls.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
#[allow(clippy::significant_drop_tightening)]
async fn live_transport_failure_leaves_recoverable_pending_intent()
-> Result<(), Box<dyn std::error::Error>> {
    // Given: a live run with durable storage and a venue whose submit is transport-uncertain.
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("pmkit-live-pending.db");
    let store = TursoTapeStore::open_local(&path).await?;
    let run = LiveRun::new(
        RunId::new("live-store-transport")?,
        PortfolioId::new("alice")?,
        Arc::new(TransportExec::default()),
        Arc::new(LiveWithFill),
        risk()?,
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("buyer")?,
        MarketId::new("btc-5m")?,
        Arc::new(BuyFactory),
    ));

    // When: the run drives through the merged feed with storage configured.
    let result = live::drive_with_store(&run, &config()?, Some(&store)).await;

    // Then: it stops on transport uncertainty and one durable pending intent remains for recovery.
    assert!(matches!(result, Err(StartError::ExecutionState { .. })));
    let scope = OwnerScope::new(
        PortfolioId::new("alice")?,
        RunId::new("live-store-transport")?,
    );
    let pending = store.read_pending_intents(&scope).await?;
    assert_eq!(pending.len(), 1);
    store.delete_database()?;
    Ok(())
}

#[tokio::test]
#[allow(clippy::significant_drop_tightening)]
async fn live_run_records_a_decision_with_storage() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a store-backed live run whose venue accepts the strategy's order.
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("pmkit-live-decision.db");
    let store = TursoTapeStore::open_local(&path).await?;

    // When: the live driver runs with storage configured.
    let result = live::drive_with_store(&live_run()?, &config()?, Some(&store)).await;

    // Then: exactly one causal decision is recorded for the single book event.
    assert!(result.is_ok());
    let scope = OwnerScope::new(PortfolioId::new("alice")?, RunId::new("live")?);
    let decisions = store.read_decisions(&scope).await?;
    assert_eq!(decisions.len(), 1);
    store.delete_database()?;
    Ok(())
}
