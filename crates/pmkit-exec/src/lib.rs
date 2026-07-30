//! The executor trait — the single live/paper/backtest mode boundary.
//!
//! Live routes to a venue, paper simulates fills, and backtest replays
//! historical fills. Every implementor sits below the same risk gate, logging,
//! and order-ownership tracking. Venue-specific operations (collateral split or
//! merge, signing) belong in venue adapters, not this neutral trait.

use pmkit_book::Side;
use pmkit_core::MarketId;
use pmkit_market::Outcome;
use rust_decimal::Decimal;
use thiserror::Error;

/// A venue order identifier returned on submission.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderId(pub String);

/// Authoritative execution state used to seed and reconcile runtime risk.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutionSnapshot {
    /// Every order currently open at the executor.
    pub open_orders: Vec<OrderId>,
}

/// Venue-provided execution details attached to an order status.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrderStatusDetails {
    /// Quantity filled at the venue, when reported.
    pub filled_qty: Option<Decimal>,
    /// Order price reported by the venue, when reported.
    pub price: Option<Decimal>,
    /// Fee charged by the venue, when reported.
    pub fee: Option<Decimal>,
    /// Venue settlement reference, when reported.
    pub settlement_reference: Option<String>,
}

/// Authoritative status and available execution details for one venue order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderStatus {
    /// The order remains open at the venue.
    Open(OrderStatusDetails),
    /// The venue accepted or filled the order.
    Accepted(OrderStatusDetails),
    /// The venue rejected the order.
    Rejected(OrderStatusDetails),
    /// The venue cancelled the order.
    Cancelled(OrderStatusDetails),
    /// The venue could not determine the final status.
    Unknown(OrderStatusDetails),
}

/// How long an order stays working before the venue removes it.
///
/// On short-lived markets an order that outlives its strategy is an orphan
/// that can fill long after the process died. Every mode honors the same
/// contract: the order stops being fillable at `expire_at_ms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimeInForce {
    /// Good-til-cancelled: the order rests until filled or cancelled.
    #[default]
    Gtc,
    /// Good-til-date: the order stops being fillable at `expire_at_ms`
    /// (Unix epoch milliseconds, event time).
    Gtd {
        /// The instant, in Unix epoch milliseconds, after which the order
        /// must no longer fill.
        expire_at_ms: i64,
    },
}

/// An order to place on a single market outcome.
#[derive(Debug, Clone)]
pub struct PlaceOrder {
    /// Exact market identity.
    pub market: MarketId,
    /// Outcome to trade.
    pub outcome: Outcome,
    /// Buy or sell.
    pub side: Side,
    /// Limit price.
    pub price: Decimal,
    /// Quantity of shares.
    pub qty: Decimal,
    /// Whether the order must rest as a maker (post-only).
    pub post_only: bool,
    /// How long the order stays working.
    pub tif: TimeInForce,
}

/// Venue market limits enforced before an order reaches a book or the venue.
///
/// This is the single definition of the limit rules: live, paper and backtest
/// all call [`MarketLimits::check`], so parity between modes is guaranteed by
/// construction instead of by keeping copies in sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketLimits {
    /// Minimum order size in **shares** (Polymarket `orderMinSize`), not a
    /// dollar notional: reading 5 shares as $5 and dividing by the price
    /// inflates the minimum and silently starves the strategy.
    pub min_order_size: Decimal,
    /// Venue price increment: valid prices are multiples of it inside
    /// `[tick, 1 - tick]`.
    pub tick_size: Decimal,
}

impl MarketLimits {
    /// Checks an order against the venue limits.
    ///
    /// # Errors
    ///
    /// Returns the violated limit; callers map it to their rejection channel.
    pub fn check(&self, order: &PlaceOrder) -> Result<(), LimitViolation> {
        if order.qty < self.min_order_size {
            return Err(LimitViolation::BelowMinOrderSize {
                qty: order.qty,
                min_order_size: self.min_order_size,
            });
        }
        let max_price = Decimal::ONE - self.tick_size;
        if order.price < self.tick_size
            || order.price > max_price
            || !(order.price % self.tick_size).is_zero()
        {
            return Err(LimitViolation::OffTickGrid {
                price: order.price,
                tick_size: self.tick_size,
            });
        }
        Ok(())
    }
}

/// A venue market limit an order violates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum LimitViolation {
    /// The order is smaller than the venue minimum.
    #[error("order size {qty} is below the venue minimum of {min_order_size} shares")]
    BelowMinOrderSize {
        /// The rejected order size, in shares.
        qty: Decimal,
        /// The venue minimum, in shares.
        min_order_size: Decimal,
    },
    /// The price is off the venue tick grid or outside its bounds.
    #[error(
        "price {price} is off the venue tick grid: prices are multiples of {tick_size} within \
         [{tick_size}, {max_price}]",
        max_price = Decimal::ONE - *tick_size
    )]
    OffTickGrid {
        /// The rejected limit price.
        price: Decimal,
        /// The venue price increment.
        tick_size: Decimal,
    },
}

/// An execution error.
#[derive(Debug, Error)]
pub enum ExecError {
    /// The venue rejected the order.
    #[error("order rejected: {reason}")]
    Rejected {
        /// Human-readable rejection reason.
        reason: String,
    },
    /// A transport or venue communication failure.
    #[error("execution transport error: {message}")]
    Transport {
        /// Human-readable error detail.
        message: String,
    },
    /// The referenced order was not found.
    #[error("order not found: {order_id}")]
    NotFound {
        /// The missing order id.
        order_id: String,
    },
}

/// The only mode boundary: live routes to a venue, paper simulates fills, and
/// backtest replays historical fills.
#[async_trait::async_trait]
pub trait Executor: Send + Sync {
    /// Returns authoritative execution state before order placement begins.
    ///
    /// # Errors
    ///
    /// Returns [`ExecError`] when state cannot be established safely.
    async fn preflight(&self) -> Result<ExecutionSnapshot, ExecError>;

    /// Refreshes authoritative execution state while the runtime is active.
    ///
    /// # Errors
    ///
    /// Returns [`ExecError`] when state cannot be reconciled safely.
    async fn reconcile(&self) -> Result<ExecutionSnapshot, ExecError>;

    /// Queries one venue order during restart recovery.
    async fn query_status(&self, _order_id: &OrderId) -> Result<OrderStatus, ExecError> {
        Err(ExecError::Transport {
            message: "venue order status query is not configured".into(),
        })
    }

    /// Submits a single order and returns its venue id.
    ///
    /// `now_ms` is the current event timestamp (system clock for live,
    /// simulated time for paper and backtest).
    ///
    /// # Errors
    ///
    /// Returns [`ExecError`] if the venue rejects the order or transport fails.
    async fn submit(&self, order: &PlaceOrder, now_ms: i64) -> Result<OrderId, ExecError>;

    /// Submits multiple orders, returning one id per order in input order.
    ///
    /// # Errors
    ///
    /// Returns [`ExecError`] on the first order that fails.
    async fn submit_batch(
        &self,
        orders: &[PlaceOrder],
        now_ms: i64,
    ) -> Result<Vec<OrderId>, ExecError> {
        let mut ids = Vec::with_capacity(orders.len());
        for order in orders {
            ids.push(self.submit(order, now_ms).await?);
        }
        Ok(ids)
    }

    /// Cancels a single order by venue id.
    ///
    /// # Errors
    ///
    /// Returns [`ExecError`] if the cancel fails.
    async fn cancel(&self, order_id: &OrderId) -> Result<(), ExecError>;

    /// Cancels multiple orders.
    ///
    /// # Errors
    ///
    /// Returns [`ExecError`] on the first cancel that fails.
    async fn cancel_batch(&self, order_ids: &[OrderId]) -> Result<(), ExecError> {
        for id in order_ids {
            self.cancel(id).await?;
        }
        Ok(())
    }

    /// Cancels every open order (emergency stop).
    ///
    /// # Errors
    ///
    /// Returns [`ExecError`] if the venue cancel-all fails.
    async fn cancel_all(&self) -> Result<(), ExecError>;
}

#[cfg(test)]
mod tests {
    use super::{
        ExecError, ExecutionSnapshot, Executor, LimitViolation, MarketLimits, OrderId, PlaceOrder,
        TimeInForce,
    };
    use pmkit_book::Side;
    use pmkit_core::MarketId;
    use pmkit_market::Outcome;
    use rust_decimal::Decimal;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct MockExecutor {
        next: AtomicU64,
    }

    #[async_trait::async_trait]
    impl Executor for MockExecutor {
        async fn preflight(&self) -> Result<ExecutionSnapshot, ExecError> {
            Ok(ExecutionSnapshot::default())
        }

        async fn reconcile(&self) -> Result<ExecutionSnapshot, ExecError> {
            Ok(ExecutionSnapshot::default())
        }

        async fn submit(&self, _order: &PlaceOrder, _now_ms: i64) -> Result<OrderId, ExecError> {
            let n = self.next.fetch_add(1, Ordering::Relaxed);
            Ok(OrderId(format!("order-{n}")))
        }

        async fn cancel(&self, _order_id: &OrderId) -> Result<(), ExecError> {
            Ok(())
        }

        async fn cancel_all(&self) -> Result<(), ExecError> {
            Ok(())
        }
    }

    fn order() -> Result<PlaceOrder, pmkit_core::EmptyIdError> {
        Ok(PlaceOrder {
            market: MarketId::new("btc-5m")?,
            outcome: Outcome::Up,
            side: Side::Buy,
            price: Decimal::new(50, 2),
            qty: Decimal::from(10),
            post_only: true,
            tif: TimeInForce::Gtc,
        })
    }

    #[tokio::test]
    async fn submit_batch_uses_default_and_returns_ids() -> Result<(), Box<dyn std::error::Error>> {
        let exec = MockExecutor {
            next: AtomicU64::new(0),
        };
        let orders = [order()?, order()?];
        let ids = exec.submit_batch(&orders, 0).await?;
        assert_eq!(
            ids,
            vec![OrderId("order-0".to_owned()), OrderId("order-1".to_owned())]
        );
        exec.cancel_batch(&ids).await?;
        exec.cancel_all().await?;
        Ok(())
    }

    fn limits() -> MarketLimits {
        MarketLimits {
            min_order_size: Decimal::from(5),
            tick_size: Decimal::new(1, 2),
        }
    }

    #[test]
    fn sub_minimum_order_violates_with_shares_semantics() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut small = order()?;
        small.qty = Decimal::from(3);
        let Err(violation) = limits().check(&small) else {
            return Err("expected a violation".into());
        };
        assert!(matches!(
            violation,
            LimitViolation::BelowMinOrderSize { .. }
        ));
        assert!(
            violation
                .to_string()
                .contains("below the venue minimum of 5 shares")
        );
        Ok(())
    }

    #[test]
    fn off_tick_price_violates_with_grid_semantics() -> Result<(), Box<dyn std::error::Error>> {
        let mut off_grid = order()?;

        // 0.455 sits between the 0.01 grid points.
        off_grid.price = Decimal::new(455, 3);
        let Err(violation) = limits().check(&off_grid) else {
            return Err("expected a violation".into());
        };
        assert!(matches!(violation, LimitViolation::OffTickGrid { .. }));
        assert!(violation.to_string().contains("off the venue tick grid"));

        // The bounds are exclusive of 0 and 1 even though both sit on the grid.
        off_grid.price = Decimal::ZERO;
        assert!(limits().check(&off_grid).is_err());
        off_grid.price = Decimal::ONE;
        assert!(limits().check(&off_grid).is_err());
        Ok(())
    }

    #[test]
    fn conforming_orders_pass_including_boundaries() -> Result<(), Box<dyn std::error::Error>> {
        let mut conforming = order()?;
        conforming.qty = Decimal::from(5);
        for cents in [1_i64, 50, 99] {
            conforming.price = Decimal::new(cents, 2);
            assert!(limits().check(&conforming).is_ok());
        }
        Ok(())
    }
}
