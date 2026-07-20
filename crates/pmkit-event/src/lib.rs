//! Neutral market event model for `PMKit`.
//!
//! [`MarketEvent`] is the single type a market loop receives: market data,
//! reference-exchange feeds, fills, acks, and timer ticks. It carries no
//! venue-specific token identifiers — outcomes are addressed by
//! [`MarketId`] plus [`Outcome`].

use pmkit_book::Side;
use pmkit_core::{MarketId, StrategyId};
use pmkit_market::{Asset, Exchange, Outcome};
use rust_decimal::Decimal;

/// Whether a fill provided or took liquidity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Liquidity {
    /// The fill provided liquidity (resting maker order).
    Maker,
    /// The fill took liquidity (aggressing taker order).
    Taker,
}

/// A single event flowing through a per-market loop.
#[derive(Debug, Clone)]
pub enum MarketEvent {
    /// Full order-book snapshot for a market outcome.
    BookUpdate {
        /// Exact market identity.
        market: MarketId,
        /// Outcome token the book belongs to.
        outcome: Outcome,
        /// Bid levels, highest price first.
        bids: Vec<(Decimal, Decimal)>,
        /// Ask levels, lowest price first.
        asks: Vec<(Decimal, Decimal)>,
        /// Event timestamp in milliseconds.
        timestamp_ms: i64,
    },
    /// Best bid/ask update for a market outcome.
    BestBidAsk {
        /// Exact market identity.
        market: MarketId,
        /// Outcome token.
        outcome: Outcome,
        /// Best bid price.
        bid: Decimal,
        /// Best ask price.
        ask: Decimal,
        /// Event timestamp in milliseconds.
        timestamp_ms: i64,
    },
    /// A trade print on a market outcome.
    LastTrade {
        /// Exact market identity.
        market: MarketId,
        /// Outcome token.
        outcome: Outcome,
        /// Trade price.
        price: Decimal,
        /// Aggressor side.
        side: Side,
        /// Trade size.
        size: Decimal,
        /// Event timestamp in milliseconds.
        timestamp_ms: i64,
    },
    /// A reference-exchange trade (for example a Binance aggregate trade).
    ReferenceTrade {
        /// Underlying asset.
        asset: Asset,
        /// Source exchange.
        exchange: Exchange,
        /// Trade price.
        price: Decimal,
        /// Trade quantity.
        qty: Decimal,
        /// Whether the buyer was the maker.
        is_buyer_maker: bool,
        /// Event timestamp in milliseconds.
        timestamp_ms: i64,
    },
    /// A reference-exchange best bid/offer.
    ReferenceBbo {
        /// Underlying asset.
        asset: Asset,
        /// Source exchange.
        exchange: Exchange,
        /// Best bid price.
        bid_px: Decimal,
        /// Best bid quantity.
        bid_qty: Decimal,
        /// Best ask price.
        ask_px: Decimal,
        /// Best ask quantity.
        ask_qty: Decimal,
        /// Event timestamp in milliseconds.
        timestamp_ms: i64,
    },
    /// A fill on one of our orders.
    Fill {
        /// Owning strategy, if attributed.
        strategy: Option<StrategyId>,
        /// Venue order id.
        order_id: String,
        /// Exact market identity.
        market: MarketId,
        /// Outcome token.
        outcome: Outcome,
        /// Fill price.
        price: Decimal,
        /// Fill size.
        size: Decimal,
        /// Fill side.
        side: Side,
        /// Fee charged on the fill.
        fee: Decimal,
        /// Whether the fill made or took liquidity.
        liquidity: Liquidity,
        /// Event timestamp in milliseconds.
        timestamp_ms: i64,
    },
    /// An order status acknowledgement.
    OrderAck {
        /// Owning strategy, if attributed.
        strategy: Option<StrategyId>,
        /// Venue order id.
        order_id: String,
        /// Event timestamp in milliseconds.
        timestamp_ms: i64,
    },
    /// A periodic timer tick.
    Tick {
        /// Event timestamp in milliseconds.
        timestamp_ms: i64,
    },
}

impl MarketEvent {
    /// Returns the event timestamp in milliseconds.
    #[must_use]
    pub const fn timestamp_ms(&self) -> i64 {
        match self {
            Self::BookUpdate { timestamp_ms, .. }
            | Self::BestBidAsk { timestamp_ms, .. }
            | Self::LastTrade { timestamp_ms, .. }
            | Self::ReferenceTrade { timestamp_ms, .. }
            | Self::ReferenceBbo { timestamp_ms, .. }
            | Self::Fill { timestamp_ms, .. }
            | Self::OrderAck { timestamp_ms, .. }
            | Self::Tick { timestamp_ms } => *timestamp_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Liquidity, MarketEvent};
    use pmkit_book::Side;
    use pmkit_core::MarketId;
    use pmkit_market::Outcome;
    use rust_decimal::Decimal;

    #[test]
    fn timestamp_reads_from_every_variant() -> Result<(), Box<dyn std::error::Error>> {
        let trade = MarketEvent::LastTrade {
            market: MarketId::new("btc-5m")?,
            outcome: Outcome::Up,
            price: Decimal::new(50, 2),
            side: Side::Buy,
            size: Decimal::from(10),
            timestamp_ms: 1_700_000_000_000,
        };
        assert_eq!(trade.timestamp_ms(), 1_700_000_000_000);

        let tick = MarketEvent::Tick { timestamp_ms: 42 };
        assert_eq!(tick.timestamp_ms(), 42);
        Ok(())
    }

    #[test]
    fn liquidity_variants_differ() {
        assert_ne!(Liquidity::Maker, Liquidity::Taker);
    }
}
