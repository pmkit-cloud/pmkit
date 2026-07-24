//! Deterministic test utilities for `PMKit` strategies.
//!
//! Strategy authors get three things:
//!
//! - **Builders** for order books, normalized facts (ticks, PM trades, CEX
//!   reference trades, account fills), and positions, with fixed inputs so a
//!   test is fully deterministic.
//! - A single-market [`Harness`] that drives a [`Strategy`] with a book and
//!   positions and returns the [`Actions`] it produced.
//! - **Assertions** over an action set (`assert_no_actions`, `assert_placed`,
//!   `assert_cancels_all`, [`placed_orders`]).
//!
//! Everything is synchronous, allocation-light, and free of hidden clocks: the
//! logical time of an event is exactly its own timestamp.

use pmkit_book::{OrderBookL2, Position, Side};
use pmkit_core::MarketId;
use pmkit_event::{CexReferenceEvent, Liquidity, MarketEvent, PmAccountEvent, StrategyFact};
use pmkit_exec::PlaceOrder;
use pmkit_market::{Asset, Exchange, Outcome};
use pmkit_strategy::{Action, Actions, LogicalTimestamp, Strategy, StrategyContext, StrategyError};
use rust_decimal::Decimal;

/// Builds a deterministic L2 book from `(price, qty)` bid and ask levels.
///
/// Levels are used verbatim; the caller orders them (bids high-first, asks
/// low-first) to match the runtime contract.
#[must_use]
pub fn book(bids: &[(Decimal, Decimal)], asks: &[(Decimal, Decimal)]) -> OrderBookL2 {
    OrderBookL2 {
        bids: bids.to_vec(),
        asks: asks.to_vec(),
        timestamp_ms: 0,
        last_trade_price: None,
    }
}

/// A periodic timer tick fact.
#[must_use]
pub const fn tick(timestamp_ms: i64) -> StrategyFact {
    StrategyFact::Market(MarketEvent::Tick { timestamp_ms })
}

/// A PM last-trade fact for `market` / `outcome`.
#[must_use]
#[allow(clippy::similar_names)]
pub fn last_trade(
    market: &MarketId,
    outcome: Outcome,
    price: Decimal,
    side: Side,
    size: Decimal,
    timestamp_ms: i64,
) -> StrategyFact {
    StrategyFact::Market(MarketEvent::LastTrade {
        market: market.clone(),
        outcome,
        price,
        side,
        size,
        timestamp_ms,
    })
}

/// A CEX reference trade fact (for example a Binance aggregate trade).
#[must_use]
pub const fn reference_trade(
    asset: Asset,
    exchange: Exchange,
    aggregate_trade_id: u64,
    price: Decimal,
    qty: Decimal,
    is_buyer_maker: bool,
    timestamp_ms: i64,
) -> StrategyFact {
    StrategyFact::Reference(CexReferenceEvent::Trade {
        asset,
        exchange,
        aggregate_trade_id,
        price,
        qty,
        is_buyer_maker,
        timestamp_ms,
    })
}

/// A PM authenticated-account fill fact (unattributed strategy).
#[must_use]
#[allow(clippy::too_many_arguments, clippy::similar_names)]
pub fn account_fill(
    order_id: impl Into<String>,
    market: &MarketId,
    outcome: Outcome,
    price: Decimal,
    size: Decimal,
    side: Side,
    fee: Decimal,
    liquidity: Liquidity,
    timestamp_ms: i64,
) -> StrategyFact {
    StrategyFact::Account(PmAccountEvent::Fill {
        strategy: None,
        order_id: order_id.into(),
        market: market.clone(),
        outcome,
        price,
        size,
        side,
        fee,
        liquidity,
        timestamp_ms,
    })
}

/// A held position with no marked profit and loss.
#[must_use]
pub const fn position(outcome: Outcome, qty: Decimal, avg_entry: Decimal) -> Position {
    Position {
        outcome,
        qty,
        avg_entry,
        unrealized_pnl: Decimal::ZERO,
    }
}

/// Reads the logical timestamp carried by any strategy fact.
#[must_use]
pub const fn fact_timestamp(fact: &StrategyFact) -> i64 {
    match fact {
        StrategyFact::Market(event) => event.timestamp_ms(),
        StrategyFact::Account(
            PmAccountEvent::Fill { timestamp_ms, .. }
            | PmAccountEvent::OrderAck { timestamp_ms, .. }
            | PmAccountEvent::OrderCancelled { timestamp_ms, .. }
            | PmAccountEvent::OrderRejected { timestamp_ms, .. }
            | PmAccountEvent::OrderStatus { timestamp_ms, .. }
            | PmAccountEvent::Settlement { timestamp_ms, .. },
        )
        | StrategyFact::Reference(CexReferenceEvent::Trade { timestamp_ms, .. }) => *timestamp_ms,
    }
}

/// A deterministic single-market harness for driving a [`Strategy`].
#[derive(Debug)]
pub struct Harness {
    market: MarketId,
    book: OrderBookL2,
    positions: Vec<Position>,
}

impl Harness {
    /// Creates a harness for `market` with an empty book and no positions.
    #[must_use]
    pub fn new(market: MarketId) -> Self {
        Self {
            market,
            book: OrderBookL2::default(),
            positions: Vec::new(),
        }
    }

    /// Sets the current book.
    #[must_use]
    pub fn with_book(mut self, book: OrderBookL2) -> Self {
        self.book = book;
        self
    }

    /// Sets the current positions.
    #[must_use]
    pub fn with_positions(mut self, positions: Vec<Position>) -> Self {
        self.positions = positions;
        self
    }

    /// The market this harness trades.
    #[must_use]
    pub const fn market(&self) -> &MarketId {
        &self.market
    }

    /// Feeds one fact at logical time equal to the fact's own timestamp and
    /// returns the produced actions.
    ///
    /// # Errors
    ///
    /// Propagates any [`StrategyError`] the strategy returns.
    pub fn feed(
        &self,
        strategy: &mut dyn Strategy,
        fact: &StrategyFact,
    ) -> Result<Actions, StrategyError> {
        strategy.on_event(StrategyContext {
            fact,
            market: &self.market,
            book: &self.book,
            positions: &self.positions,
            now: LogicalTimestamp::from_millis(fact_timestamp(fact)),
        })
    }
}

/// Returns the placed orders in `actions`, in order.
#[must_use]
pub fn placed_orders(actions: &Actions) -> Vec<&PlaceOrder> {
    actions
        .as_slice()
        .iter()
        .filter_map(|action| match action {
            Action::Place(order) => Some(order),
            _ => None,
        })
        .collect()
}

/// Asserts the action set is empty.
///
/// # Panics
///
/// Panics if any action is present.
pub fn assert_no_actions(actions: &Actions) {
    assert!(
        actions.is_empty(),
        "expected no actions, got {:?}",
        actions.as_slice()
    );
}

/// Asserts exactly one placed order matching `side`, `outcome`, and `price`.
///
/// # Panics
///
/// Panics if there is not exactly one placed order, or it does not match.
pub fn assert_placed(actions: &Actions, side: Side, outcome: Outcome, price: Decimal) {
    let placed = placed_orders(actions);
    assert_eq!(
        placed.len(),
        1,
        "expected exactly one placed order, got {placed:?}"
    );
    let order = placed[0];
    assert_eq!(order.side, side, "unexpected side");
    assert_eq!(order.outcome, outcome, "unexpected outcome");
    assert_eq!(order.price, price, "unexpected price");
}

/// Asserts the action set contains a cancel-all.
///
/// # Panics
///
/// Panics if no [`Action::CancelAll`] is present.
pub fn assert_cancels_all(actions: &Actions) {
    assert!(
        actions
            .as_slice()
            .iter()
            .any(|action| matches!(action, Action::CancelAll)),
        "expected a CancelAll, got {:?}",
        actions.as_slice()
    );
}

#[cfg(test)]
mod tests;
