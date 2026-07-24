use async_trait::async_trait;
use pmkit_core::MarketId;
use pmkit_data::{
    DataSourceError, HistoricalDataSource, LiveDataSource, ReplayQuery, SourceSignal,
};
use pmkit_exec::{ExecError, ExecutionSnapshot, Executor, OrderId, PlaceOrder};
use pmkit_market::Outcome;
use pmkit_money::Money;
use pmkit_runtime::RiskLimits;
use pmkit_strategy::{
    Actions, Strategy, StrategyContext, StrategyError, StrategyFactory, StrategyInitError,
};
use std::num::NonZeroU32;
use tokio::sync::mpsc::Sender;

pub struct NoHistory;

#[async_trait]
impl HistoricalDataSource for NoHistory {
    async fn replay(
        &self,
        _query: ReplayQuery,
        _sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        Ok(())
    }
}

pub struct NoLive;

#[async_trait]
impl LiveDataSource for NoLive {
    async fn subscribe(
        &self,
        _market: MarketId,
        _outcome: Outcome,
        _sink: Sender<SourceSignal>,
    ) -> Result<(), DataSourceError> {
        Ok(())
    }
}

pub struct NoExec;

#[async_trait]
impl Executor for NoExec {
    async fn preflight(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(ExecutionSnapshot::default())
    }

    async fn reconcile(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(ExecutionSnapshot::default())
    }

    async fn submit(&self, _order: &PlaceOrder, _now_ms: i64) -> Result<OrderId, ExecError> {
        Ok(OrderId("x".to_owned()))
    }

    async fn cancel(&self, _order_id: &OrderId) -> Result<(), ExecError> {
        Ok(())
    }

    async fn cancel_all(&self) -> Result<(), ExecError> {
        Ok(())
    }
}

pub struct FlatStrategy;

impl Strategy for FlatStrategy {
    fn on_event(&mut self, _ctx: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        Ok(Actions::none())
    }
}

pub struct FlatFactory;

impl StrategyFactory for FlatFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(FlatStrategy))
    }
}

pub fn risk() -> Result<RiskLimits, Box<dyn std::error::Error>> {
    Ok(RiskLimits {
        max_order_notional: Money::usdc(100),
        max_position_notional: Money::usdc(1_000),
        max_portfolio_notional: Money::usdc(5_000),
        max_market_notional: Money::usdc(2_000),
        max_strategy_notional: Money::usdc(1_000),
        max_open_orders: NonZeroU32::new(10).ok_or("nonzero")?,
        max_loss: Money::usdc(500),
        max_daily_loss: Money::usdc(500),
    })
}
