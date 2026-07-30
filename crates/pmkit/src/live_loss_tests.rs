use super::CapacityExec;
use crate::{
    live,
    test_support::{config, risk},
};
use async_trait::async_trait;
use pmkit_book::Side;
use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
use pmkit_data::{DataSourceError, LiveDataSource, SourceSignal};
use pmkit_event::{Liquidity, MarketEvent};
use pmkit_exec::{PlaceOrder, TimeInForce};
use pmkit_market::Outcome;
use pmkit_money::Money;
use pmkit_runtime::{RiskLimits, StrategyRegistration};
use pmkit_spec::LiveRun;
use pmkit_strategy::{
    Actions, Strategy, StrategyContext, StrategyError, StrategyFactory, StrategyInitError,
};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc::Sender;

struct LossThenRecovery;

#[async_trait]
impl LiveDataSource for LossThenRecovery {
    async fn subscribe(
        &self,
        market: MarketId,
        outcome: Outcome,
        sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        if outcome == Outcome::Up {
            for (price, timestamp_ms) in [
                (Decimal::new(60, 2), 1),
                (Decimal::new(40, 2), 3),
                (Decimal::new(60, 2), 4),
            ] {
                sink.send(SourceSignal::market_event(MarketEvent::BookUpdate {
                    market: market.clone(),
                    outcome,
                    bids: vec![(price, Decimal::from(10))],
                    asks: vec![(price, Decimal::from(10))],
                    timestamp_ms,
                }))
                .await
                .map_err(|_| DataSourceError::SinkClosed)?;
                if timestamp_ms == 1 {
                    sink.send(SourceSignal::market_event(MarketEvent::Fill {
                        strategy: None,
                        order_id: "venue-1".to_owned(),
                        market: market.clone(),
                        outcome,
                        price,
                        size: Decimal::from(10),
                        side: Side::Buy,
                        fee: Decimal::ZERO,
                        liquidity: Liquidity::Taker,
                        timestamp_ms: 2,
                    }))
                    .await
                    .map_err(|_| DataSourceError::SinkClosed)?;
                }
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

struct OrderAfterLoss;

impl Strategy for OrderAfterLoss {
    fn on_event(&mut self, ctx: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        if ctx.now.as_millis() < 3 {
            return Ok(Actions::none());
        }
        Ok(Actions::place(PlaceOrder {
            market: ctx.market.clone(),
            outcome: Outcome::Up,
            side: Side::Buy,
            price: Decimal::ONE,
            qty: Decimal::ONE,
            post_only: false,
            tif: TimeInForce::Gtc,
        }))
    }
}

struct OrderAfterLossFactory;

impl StrategyFactory for OrderAfterLossFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(OrderAfterLoss))
    }
}

#[tokio::test]
async fn live_run_latches_loss_limit_after_marked_drawdown()
-> Result<(), Box<dyn std::error::Error>> {
    let executor = Arc::new(CapacityExec::default());
    let mut limits = risk()?;
    limits.max_loss = Money::usdc(2);
    let run = LiveRun::new(
        RunId::new("live-loss")?,
        PortfolioId::new("alice")?,
        executor.clone(),
        Arc::new(LossThenRecovery),
        limits,
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("loss-checker")?,
        MarketId::new("btc-5m")?,
        Arc::new(OrderAfterLossFactory),
    ));

    let report = live::drive(&run, &config()?).await?;

    assert_eq!(executor.submits.load(Ordering::Relaxed), 0);
    assert_eq!(report.rejected, 2);
    Ok(())
}

#[test]
fn risk_gate_rejects_a_marked_loss_at_the_limit() -> Result<(), Box<dyn std::error::Error>> {
    let limits = RiskLimits {
        max_order_notional: Money::usdc(10),
        max_position_notional: Money::usdc(10),
        max_portfolio_notional: Money::usdc(20),
        max_market_notional: Money::usdc(10),
        max_strategy_notional: Money::usdc(10),
        max_open_orders: NonZeroU32::new(5).ok_or("nonzero")?,
        max_loss: Money::usdc(2),
        max_daily_loss: Money::usdc(2),
    };
    let market = MarketId::new("btc-5m")?;
    let mut positions = HashMap::from([(
        market.clone(),
        vec![pmkit_book::Position {
            outcome: Outcome::Up,
            qty: Decimal::from(10),
            avg_entry: Decimal::new(60, 2),
            unrealized_pnl: Decimal::ZERO,
        }],
    )]);
    let marks = HashMap::from([((market.clone(), Outcome::Up), Decimal::new(40, 2))]);
    let portfolio_pnl = live::mark_positions(&mut positions, &marks);
    let order = PlaceOrder {
        market: market.clone(),
        outcome: Outcome::Up,
        side: Side::Buy,
        price: Decimal::ONE,
        qty: Decimal::ONE,
        post_only: false,
        tif: TimeInForce::Gtc,
    };

    assert_eq!(portfolio_pnl, Some(Decimal::new(-2, 0)));
    assert!(!live::passes_risk(
        &order,
        &limits,
        positions.get(&market).ok_or("market positions")?,
        portfolio_pnl,
    ));
    Ok(())
}

#[test]
fn risk_gate_enforces_order_and_position_notional() -> Result<(), Box<dyn std::error::Error>> {
    let limits = RiskLimits {
        max_order_notional: Money::usdc(10),
        max_position_notional: Money::usdc(8),
        max_portfolio_notional: Money::usdc(20),
        max_market_notional: Money::usdc(8),
        max_strategy_notional: Money::usdc(8),
        max_open_orders: NonZeroU32::new(5).ok_or("nonzero")?,
        max_loss: Money::usdc(100),
        max_daily_loss: Money::usdc(100),
    };
    let market = MarketId::new("btc-5m")?;
    let order = |qty: i64| PlaceOrder {
        market: market.clone(),
        outcome: Outcome::Up,
        side: Side::Buy,
        price: Decimal::ONE,
        qty: Decimal::from(qty),
        post_only: false,
        tif: TimeInForce::Gtc,
    };
    assert!(live::passes_risk(
        &order(5),
        &limits,
        &[],
        Some(Decimal::ZERO),
    ));
    assert!(!live::passes_risk(
        &order(15),
        &limits,
        &[],
        Some(Decimal::ZERO),
    ));
    let held = [pmkit_book::Position {
        outcome: Outcome::Up,
        qty: Decimal::from(5),
        avg_entry: Decimal::ONE,
        unrealized_pnl: Decimal::ZERO,
    }];
    assert!(!live::passes_risk(
        &order(5),
        &limits,
        &held,
        Some(Decimal::ZERO),
    ));
    Ok(())
}

#[test]
fn aggregated_risk_enforces_portfolio_market_strategy_and_daily_limits()
-> Result<(), Box<dyn std::error::Error>> {
    let mut limits = risk()?;
    limits.max_portfolio_notional = Money::usdc(10);
    limits.max_market_notional = Money::usdc(8);
    limits.max_strategy_notional = Money::usdc(6);
    limits.max_daily_loss = Money::usdc(2);
    let order = PlaceOrder {
        market: MarketId::new("btc-5m")?,
        outcome: Outcome::Up,
        side: Side::Buy,
        price: Decimal::ONE,
        qty: Decimal::from(2),
        post_only: false,
        tif: TimeInForce::Gtc,
    };
    let exposure = |portfolio, market, strategy, daily_pnl| live::TestRiskExposure {
        portfolio_notional: Decimal::from(portfolio),
        market_notional: Decimal::from(market),
        strategy_notional: Decimal::from(strategy),
        daily_pnl: Decimal::from(daily_pnl),
        open_orders: 0,
    };

    assert!(live::test_passes_aggregated_risk(
        &order,
        &limits,
        &[],
        exposure(7, 5, 3, 0),
    ));
    assert!(!live::test_passes_aggregated_risk(
        &order,
        &limits,
        &[],
        exposure(9, 5, 3, 0),
    ));
    assert!(!live::test_passes_aggregated_risk(
        &order,
        &limits,
        &[],
        exposure(7, 7, 3, 0),
    ));
    assert!(!live::test_passes_aggregated_risk(
        &order,
        &limits,
        &[],
        exposure(7, 5, 5, 0),
    ));
    assert!(!live::test_passes_aggregated_risk(
        &order,
        &limits,
        &[],
        exposure(7, 5, 3, -2),
    ));
    Ok(())
}
