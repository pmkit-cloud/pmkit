//! Conservative fill-simulation engine for `PMKit` paper and backtest runs.
//!
//! The engine holds per-outcome order books and resting maker orders. Taker
//! orders fill immediately by walking the book; maker orders rest and fill when
//! a later book update crosses them. Fills are emitted as
//! [`MarketEvent::Fill`], never touching a real venue.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use pmkit_book::{OrderBookL2, Side};
use pmkit_core::MarketId;
use pmkit_event::{Liquidity, MarketEvent};
use pmkit_exec::{OrderId, PlaceOrder};
use pmkit_market::Outcome;
use pmkit_math::fees::taker_fee_order;
use pmkit_math::fill::walk_book;
use rust_decimal::Decimal;

pub use pmkit_math::fees::MarketCategory;

type BookKey = (MarketId, Outcome);

#[derive(Debug, Clone)]
struct RestingOrder {
    order_id: String,
    order: PlaceOrder,
    timestamp_ms: i64,
}

/// A conservative fill-simulation engine shared by paper and backtest modes.
#[derive(Debug)]
pub struct SimEngine {
    books: HashMap<BookKey, OrderBookL2>,
    resting: HashMap<String, RestingOrder>,
    pending_fills: Vec<MarketEvent>,
    next_id: AtomicU64,
    id_prefix: String,
    category: MarketCategory,
}

impl SimEngine {
    /// Creates an engine that mints order ids as `"{id_prefix}-{n}"` from
    /// `start_id`, charging taker fees for `category`.
    #[must_use]
    pub fn new(id_prefix: impl Into<String>, start_id: u64, category: MarketCategory) -> Self {
        Self {
            books: HashMap::new(),
            resting: HashMap::new(),
            pending_fills: Vec::new(),
            next_id: AtomicU64::new(start_id),
            id_prefix: id_prefix.into(),
            category,
        }
    }

    /// Replaces the book for one market outcome and fills any crossed makers.
    pub fn update_book(&mut self, market: &MarketId, outcome: Outcome, book: OrderBookL2) {
        self.books.insert((market.clone(), outcome), book);
        self.check_resting_fills(market, outcome);
    }

    /// Submits an order. Post-only orders rest; others take liquidity. Returns
    /// the minted order id, or `None` when there is no book or no fill.
    pub fn submit(&mut self, order: &PlaceOrder, now_ms: i64) -> Option<OrderId> {
        let order_id = self.next_order_id();
        if order.post_only {
            self.submit_maker(order_id, order, now_ms)
        } else {
            self.submit_taker(order_id, order, now_ms)
        }
    }

    /// Cancels a resting order, returning the notional it freed.
    pub fn cancel(&mut self, order_id: &OrderId) -> Option<Decimal> {
        self.resting
            .remove(&order_id.0)
            .map(|r| r.order.qty * r.order.price)
    }

    /// Cancels every resting order, returning the total notional freed.
    pub fn cancel_all(&mut self) -> Decimal {
        let freed = self.resting_committed();
        self.resting.clear();
        freed
    }

    /// Returns the total resting maker notional.
    #[must_use]
    pub fn resting_committed(&self) -> Decimal {
        self.resting
            .values()
            .map(|r| r.order.qty * r.order.price)
            .sum()
    }

    /// Returns the number of resting maker orders.
    #[must_use]
    pub fn resting_count(&self) -> usize {
        self.resting.len()
    }

    /// Drains and returns the fills accumulated since the last call.
    pub fn drain_fills(&mut self) -> Vec<MarketEvent> {
        std::mem::take(&mut self.pending_fills)
    }

    fn submit_maker(
        &mut self,
        order_id: String,
        order: &PlaceOrder,
        now_ms: i64,
    ) -> Option<OrderId> {
        let would_cross = {
            let book = self.books.get(&(order.market.clone(), order.outcome))?;
            match order.side {
                Side::Buy => book.best_ask().is_some_and(|(ask, _)| order.price >= ask),
                Side::Sell => book.best_bid().is_some_and(|(bid, _)| order.price <= bid),
            }
        };
        if would_cross {
            return None;
        }
        self.resting.insert(
            order_id.clone(),
            RestingOrder {
                order_id: order_id.clone(),
                order: order.clone(),
                timestamp_ms: now_ms,
            },
        );
        Some(OrderId(order_id))
    }

    fn submit_taker(
        &mut self,
        order_id: String,
        order: &PlaceOrder,
        now_ms: i64,
    ) -> Option<OrderId> {
        let (vwap, fill_qty) = {
            let book = self.books.get(&(order.market.clone(), order.outcome))?;
            let (levels, is_buy) = match order.side {
                Side::Buy => (&book.asks, true),
                Side::Sell => (&book.bids, false),
            };
            walk_book(levels, order.qty, order.price, is_buy)?
        };
        let fee = taker_fee_order(fill_qty, vwap, self.category);
        self.pending_fills.push(MarketEvent::Fill {
            strategy: None,
            order_id: order_id.clone(),
            market: order.market.clone(),
            outcome: order.outcome,
            price: vwap,
            size: fill_qty,
            side: order.side,
            fee,
            liquidity: Liquidity::Taker,
            timestamp_ms: now_ms,
        });
        Some(OrderId(order_id))
    }

    fn next_order_id(&self) -> String {
        format!(
            "{}-{}",
            self.id_prefix,
            self.next_id.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn check_resting_fills(&mut self, market: &MarketId, outcome: Outcome) {
        let to_fill: Vec<String> = {
            let Some(book) = self.books.get(&(market.clone(), outcome)) else {
                return;
            };
            self.resting
                .values()
                .filter(|r| r.order.market == *market && r.order.outcome == outcome)
                .filter(|r| match r.order.side {
                    Side::Buy => book.best_ask().is_some_and(|(ask, _)| ask <= r.order.price),
                    Side::Sell => book.best_bid().is_some_and(|(bid, _)| bid >= r.order.price),
                })
                .map(|r| r.order_id.clone())
                .collect()
        };

        for order_id in to_fill {
            if let Some(resting) = self.resting.remove(&order_id) {
                self.pending_fills.push(MarketEvent::Fill {
                    strategy: None,
                    order_id: resting.order_id,
                    market: resting.order.market.clone(),
                    outcome: resting.order.outcome,
                    price: resting.order.price,
                    size: resting.order.qty,
                    side: resting.order.side,
                    fee: Decimal::ZERO,
                    liquidity: Liquidity::Maker,
                    timestamp_ms: resting.timestamp_ms,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MarketCategory, SimEngine};
    use pmkit_book::{OrderBookL2, Side};
    use pmkit_core::MarketId;
    use pmkit_event::{Liquidity, MarketEvent};
    use pmkit_exec::PlaceOrder;
    use pmkit_market::Outcome;
    use rust_decimal::Decimal;

    fn ask_book() -> OrderBookL2 {
        OrderBookL2 {
            bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
            asks: vec![(Decimal::new(46, 2), Decimal::from(50))],
            timestamp_ms: 0,
            last_trade_price: None,
        }
    }

    fn order(
        side: Side,
        price: Decimal,
        post_only: bool,
    ) -> Result<PlaceOrder, pmkit_core::EmptyIdError> {
        Ok(PlaceOrder {
            market: MarketId::new("btc-5m")?,
            outcome: Outcome::Up,
            side,
            price,
            qty: Decimal::from(10),
            post_only,
        })
    }

    #[test]
    fn taker_buy_fills_immediately() -> Result<(), Box<dyn std::error::Error>> {
        let mut engine = SimEngine::new("paper", 0, MarketCategory::Crypto);
        engine.update_book(&MarketId::new("btc-5m")?, Outcome::Up, ask_book());
        let id = engine.submit(&order(Side::Buy, Decimal::new(50, 2), false)?, 100);
        assert!(id.is_some());

        let fills = engine.drain_fills();
        assert_eq!(fills.len(), 1);
        let MarketEvent::Fill {
            liquidity, size, ..
        } = &fills[0]
        else {
            return Err("expected a fill".into());
        };
        assert_eq!(*liquidity, Liquidity::Taker);
        assert_eq!(*size, Decimal::from(10));
        Ok(())
    }

    #[test]
    fn maker_rests_then_fills_when_crossed() -> Result<(), Box<dyn std::error::Error>> {
        let mut engine = SimEngine::new("paper", 0, MarketCategory::Crypto);
        engine.update_book(&MarketId::new("btc-5m")?, Outcome::Up, ask_book());

        // Post-only buy below the ask rests without crossing.
        let id = engine.submit(&order(Side::Buy, Decimal::new(45, 2), true)?, 100);
        assert!(id.is_some());
        assert_eq!(engine.resting_count(), 1);
        assert!(engine.drain_fills().is_empty());

        // A new book whose ask drops to the resting price fills the maker.
        let crossed = OrderBookL2 {
            bids: vec![(Decimal::new(44, 2), Decimal::from(50))],
            asks: vec![(Decimal::new(45, 2), Decimal::from(50))],
            timestamp_ms: 1,
            last_trade_price: None,
        };
        engine.update_book(&MarketId::new("btc-5m")?, Outcome::Up, crossed);

        let fills = engine.drain_fills();
        assert_eq!(fills.len(), 1);
        let MarketEvent::Fill { liquidity, fee, .. } = &fills[0] else {
            return Err("expected a fill".into());
        };
        assert_eq!(*liquidity, Liquidity::Maker);
        assert_eq!(*fee, Decimal::ZERO);
        assert_eq!(engine.resting_count(), 0);
        Ok(())
    }
}
