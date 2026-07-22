use crate::{
    Pmkit, StartError, live,
    test_support::{BuyFactory, config, risk},
};
use async_trait::async_trait;
use pmkit_book::Side;
use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
use pmkit_data::{DataSourceError, LiveDataSource, SourceSignal};
use pmkit_event::{Liquidity, MarketEvent};
use pmkit_exec::{ExecError, ExecutionSnapshot, Executor, OrderId, PlaceOrder};
use pmkit_market::Outcome;
use pmkit_runtime::{LiveOrderPolicy, StrategyRegistration};
use pmkit_spec::LiveRun;
use rust_decimal::Decimal;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::mpsc::Sender;

#[path = "live_loss_tests.rs"]
mod live_loss_tests;
#[path = "live_tape_tests.rs"]
mod live_tape_tests;

struct RecordingExec;

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
    let report = live::drive(&live_run()?, &config()?).await?;
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
        Arc::new(LiveWithFill),
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

    assert_eq!(report.events_processed, 2);
    assert_eq!(executor.cancels.load(Ordering::Relaxed), 1);
    assert_eq!(executor.cancel_all_calls.load(Ordering::Relaxed), 0);
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
