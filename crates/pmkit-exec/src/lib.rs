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
    use super::{ExecError, ExecutionSnapshot, Executor, OrderId, PlaceOrder};
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
}
