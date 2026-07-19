//! Order-book, position, and sizing primitives for prediction markets.
//!
//! Neutral domain types: an order book, a side, and a position are universal
//! trading concepts, not venue-specific. Venue token identifiers live in
//! adapters, never here.

use pmkit_market::Outcome;
use rust_decimal::Decimal;

pub mod book;
pub mod risk;

/// Order side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    /// Buy / bid side.
    Buy,
    /// Sell / ask side.
    Sell,
}

/// Level 2 order book for a single market outcome token.
#[derive(Debug, Clone, Default)]
pub struct OrderBookL2 {
    /// Bid price levels sorted descending (highest bid first).
    pub bids: Vec<(Decimal, Decimal)>,
    /// Ask price levels sorted ascending (lowest ask first).
    pub asks: Vec<(Decimal, Decimal)>,
    /// Timestamp of the last book update, in milliseconds.
    pub timestamp_ms: i64,
    /// Last trade price, if any trade has printed.
    pub last_trade_price: Option<Decimal>,
}

impl OrderBookL2 {
    /// Returns the best (highest) bid level, if any.
    #[must_use]
    pub fn best_bid(&self) -> Option<(Decimal, Decimal)> {
        self.bids.first().copied()
    }

    /// Returns the best (lowest) ask level, if any.
    #[must_use]
    pub fn best_ask(&self) -> Option<(Decimal, Decimal)> {
        self.asks.first().copied()
    }

    /// Returns the simple mid price `(best_bid + best_ask) / 2`, if both sides exist.
    #[must_use]
    pub fn mid_price(&self) -> Option<Decimal> {
        let (bid, _) = self.best_bid()?;
        let (ask, _) = self.best_ask()?;
        Some((bid + ask) / Decimal::TWO)
    }

    /// Returns the top-of-book spread `best_ask - best_bid`, if both sides exist.
    #[must_use]
    pub fn spread(&self) -> Option<Decimal> {
        let (bid, _) = self.best_bid()?;
        let (ask, _) = self.best_ask()?;
        Some(ask - bid)
    }

    /// Returns the order-book imbalance `(bid_qty - ask_qty) / (bid_qty + ask_qty)`.
    ///
    /// Positive means more bid depth (buying pressure); zero when the book is empty.
    #[must_use]
    pub fn obi(&self) -> Decimal {
        let (_, bid_qty) = self.best_bid().unwrap_or((Decimal::ZERO, Decimal::ZERO));
        let (_, ask_qty) = self.best_ask().unwrap_or((Decimal::ZERO, Decimal::ZERO));
        let total = bid_qty + ask_qty;
        if total.is_zero() {
            return Decimal::ZERO;
        }
        (bid_qty - ask_qty) / total
    }
}

/// A held position in a market outcome.
#[derive(Debug, Clone)]
pub struct Position {
    /// Which outcome is held.
    pub outcome: Outcome,
    /// Quantity of shares held.
    pub qty: Decimal,
    /// Average entry price.
    pub avg_entry: Decimal,
    /// Unrealized profit and loss at the last mark.
    pub unrealized_pnl: Decimal,
}
