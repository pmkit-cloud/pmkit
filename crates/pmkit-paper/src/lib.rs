//! Paper executor for `PMKit`.
//!
//! [`PaperExecutor`] implements the [`Executor`] trait against an in-memory
//! [`SimEngine`]. Orders are simulated, never sent to a venue; resulting fills
//! are delivered as [`MarketEvent`]s on a channel the runtime owns. Feed book
//! updates through [`PaperExecutor::update_book`] so resting makers can fill.

use std::sync::{Mutex, PoisonError};

use async_trait::async_trait;
use pmkit_book::OrderBookL2;
use pmkit_core::MarketId;
use pmkit_event::MarketEvent;
use pmkit_exec::{ExecError, ExecutionSnapshot, Executor, OrderId, PlaceOrder};
use pmkit_market::Outcome;
use pmkit_sim::{MarketCategory, SimEngine, SimulationConfig};
use tokio::sync::mpsc::Sender;

/// An executor that simulates fills instead of routing to a venue.
#[derive(Debug)]
pub struct PaperExecutor {
    engine: Mutex<SimEngine>,
    fills: Sender<MarketEvent>,
}

impl PaperExecutor {
    /// Creates a paper executor that delivers fills to `fills`, minting order
    /// ids as `"{id_prefix}-{n}"` and charging taker fees for `category`.
    #[must_use]
    pub fn new(
        fills: Sender<MarketEvent>,
        id_prefix: impl Into<String>,
        category: MarketCategory,
    ) -> Self {
        Self {
            engine: Mutex::new(SimEngine::new(id_prefix, 0, category)),
            fills,
        }
    }

    /// Creates a paper executor with explicit simulation inputs.
    #[must_use]
    pub fn with_config(
        fills: Sender<MarketEvent>,
        id_prefix: impl Into<String>,
        category: MarketCategory,
        config: SimulationConfig,
    ) -> Self {
        Self {
            engine: Mutex::new(SimEngine::with_config(id_prefix, 0, category, config)),
            fills,
        }
    }

    /// Applies a book update, delivering any fills it triggers on resting
    /// maker orders.
    ///
    /// # Errors
    ///
    /// Returns [`ExecError::Transport`] if the fill receiver has closed.
    pub async fn update_book(
        &self,
        market: &MarketId,
        outcome: Outcome,
        book: OrderBookL2,
    ) -> Result<(), ExecError> {
        let drained = {
            let mut engine = self.engine.lock().unwrap_or_else(PoisonError::into_inner);
            engine.update_book(market, outcome, book);
            engine.drain_fills()
        };
        self.deliver(drained).await
    }

    async fn deliver(&self, fills: Vec<MarketEvent>) -> Result<(), ExecError> {
        for fill in fills {
            self.fills
                .send(fill)
                .await
                .map_err(|_| ExecError::Transport {
                    message: "paper fill receiver closed".to_owned(),
                })?;
        }
        Ok(())
    }
}

#[async_trait]
impl Executor for PaperExecutor {
    async fn preflight(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(self.snapshot())
    }

    async fn reconcile(&self) -> Result<ExecutionSnapshot, ExecError> {
        Ok(self.snapshot())
    }

    async fn submit(&self, order: &PlaceOrder, now_ms: i64) -> Result<OrderId, ExecError> {
        let (id, drained) = {
            let mut engine = self.engine.lock().unwrap_or_else(PoisonError::into_inner);
            let id = engine.submit(order, now_ms);
            (id, engine.drain_fills())
        };
        self.deliver(drained).await?;
        id.ok_or_else(|| ExecError::Rejected {
            reason: "no fill available".to_owned(),
        })
    }

    async fn cancel(&self, order_id: &OrderId) -> Result<(), ExecError> {
        self.engine
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cancel(order_id);
        Ok(())
    }

    async fn cancel_all(&self) -> Result<(), ExecError> {
        self.engine
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cancel_all();
        Ok(())
    }
}

impl PaperExecutor {
    fn snapshot(&self) -> ExecutionSnapshot {
        ExecutionSnapshot {
            open_orders: self
                .engine
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .resting_order_ids(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PaperExecutor;
    use pmkit_book::{OrderBookL2, Side};
    use pmkit_core::MarketId;
    use pmkit_event::{Liquidity, MarketEvent};
    use pmkit_exec::{Executor, PlaceOrder};
    use pmkit_market::Outcome;
    use pmkit_sim::MarketCategory;
    use rust_decimal::Decimal;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn taker_order_delivers_a_fill() -> Result<(), Box<dyn std::error::Error>> {
        let (tx, mut rx) = mpsc::channel(8);
        let paper = PaperExecutor::new(tx, "paper", MarketCategory::Crypto);

        let market = MarketId::new("btc-5m")?;
        paper
            .update_book(
                &market,
                Outcome::Up,
                OrderBookL2 {
                    bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
                    asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
                    timestamp_ms: 0,
                    last_trade_price: None,
                },
            )
            .await?;

        let order = PlaceOrder {
            market: market.clone(),
            outcome: Outcome::Up,
            side: Side::Buy,
            price: Decimal::new(50, 2),
            qty: Decimal::from(10),
            post_only: false,
        };
        let id = paper.submit(&order, 100).await?;
        assert!(id.0.starts_with("paper-"));

        let fill = rx.recv().await.ok_or("expected a fill event")?;
        let MarketEvent::Fill {
            liquidity, size, ..
        } = fill
        else {
            return Err("expected a fill".into());
        };
        assert_eq!(liquidity, Liquidity::Taker);
        assert_eq!(size, Decimal::from(10));
        Ok(())
    }

    #[tokio::test]
    async fn preflight_reports_resting_orders() -> Result<(), Box<dyn std::error::Error>> {
        let (tx, _rx) = mpsc::channel(8);
        let paper = PaperExecutor::new(tx, "paper", MarketCategory::Crypto);
        let market = MarketId::new("btc-5m")?;
        paper
            .update_book(
                &market,
                Outcome::Up,
                OrderBookL2 {
                    bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
                    asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
                    timestamp_ms: 0,
                    last_trade_price: None,
                },
            )
            .await?;
        let order = PlaceOrder {
            market,
            outcome: Outcome::Up,
            side: Side::Buy,
            price: Decimal::new(45, 2),
            qty: Decimal::from(10),
            post_only: true,
        };
        let id = paper.submit(&order, 100).await?;

        let snapshot = paper.preflight().await?;

        assert_eq!(snapshot.open_orders, vec![id]);
        Ok(())
    }
}
