use pmkit_book::Side;
use pmkit_exec::PlaceOrder;
use pmkit_market::Outcome;
use pmkit_money::Money;
use pmkit_runtime::{RiskLimits, RuntimeConfig, ShutdownConfig};
use pmkit_strategy::{
    Actions, Strategy, StrategyContext, StrategyError, StrategyFactory, StrategyInitError,
};
use rust_decimal::Decimal;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

struct BuyOnce {
    placed: bool,
}

impl Strategy for BuyOnce {
    fn on_event(&mut self, ctx: StrategyContext<'_>) -> Result<Actions, StrategyError> {
        if self.placed {
            return Ok(Actions::none());
        }
        self.placed = true;
        Ok(Actions::place(PlaceOrder {
            market: ctx.market.clone(),
            outcome: Outcome::Up,
            side: Side::Buy,
            price: Decimal::new(50, 2),
            qty: Decimal::from(10),
            post_only: false,
        }))
    }
}

pub struct BuyFactory;

impl StrategyFactory for BuyFactory {
    fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
        Ok(Box::new(BuyOnce { placed: false }))
    }
}

pub fn config() -> Result<RuntimeConfig, Box<dyn std::error::Error>> {
    Ok(RuntimeConfig {
        backtest_concurrency: NonZeroUsize::new(1).ok_or("nonzero")?,
        startup_timeout: Duration::from_secs(30),
        shutdown: ShutdownConfig {
            live_orders: pmkit_runtime::LiveOrderPolicy::CancelOwned,
            reconciliation_timeout: Duration::from_secs(30),
            tape_flush_timeout: Duration::from_secs(10),
        },
        manifest_dir: "./runs".into(),
    })
}

pub fn risk() -> Result<RiskLimits, Box<dyn std::error::Error>> {
    Ok(RiskLimits {
        max_order_notional: Money::usdc(100),
        max_position_notional: Money::usdc(1_000),
        max_open_orders: NonZeroU32::new(10).ok_or("nonzero")?,
        max_loss: Money::usdc(500),
    })
}
