use crate::{
    Pmkit, StartError, live,
    test_support::{BuyFactory, config, risk},
};
use async_trait::async_trait;
use pmkit_book::Side;
use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
use pmkit_data::{DataSourceError, LiveDataSource};
use pmkit_event::{Liquidity, MarketEvent};
use pmkit_exec::{ExecError, ExecutionSnapshot, Executor, OrderId, PlaceOrder};
use pmkit_market::Outcome;
use pmkit_money::Money;
use pmkit_runtime::{RiskLimits, StrategyRegistration};
use pmkit_spec::LiveRun;
use rust_decimal::Decimal;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc::Sender;

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
    let report = live::drive(&live_run()?).await?;
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

    let report = live::drive(&run).await?;

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

    let report = live::drive(&run).await?;

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

    let result = live::drive(&run).await;

    assert!(matches!(result, Err(StartError::ExecutionState { .. })));
    assert_eq!(executor.reconciles.load(Ordering::Relaxed), 2);
    Ok(())
}

#[test]
fn risk_gate_enforces_order_and_position_notional() -> Result<(), Box<dyn std::error::Error>> {
    let limits = RiskLimits {
        max_order_notional: Money::usdc(10),
        max_position_notional: Money::usdc(8),
        max_open_orders: NonZeroU32::new(5).ok_or("nonzero")?,
        max_loss: Money::usdc(100),
    };
    let market = MarketId::new("btc-5m")?;
    let order = |qty: i64| PlaceOrder {
        market: market.clone(),
        outcome: Outcome::Up,
        side: Side::Buy,
        price: Decimal::ONE,
        qty: Decimal::from(qty),
        post_only: false,
    };
    assert!(live::passes_risk(&order(5), &limits, &[]));
    assert!(!live::passes_risk(&order(15), &limits, &[]));
    let held = [pmkit_book::Position {
        outcome: Outcome::Up,
        qty: Decimal::from(5),
        avg_entry: Decimal::ONE,
        unrealized_pnl: Decimal::ZERO,
    }];
    assert!(!live::passes_risk(&order(5), &limits, &held));
    Ok(())
}
