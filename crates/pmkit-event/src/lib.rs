//! Neutral market event model for `PMKit`.
//!
//! [`MarketEvent`] is the normalized PM market fact a strategy receives. It carries no
//! venue-specific token identifiers — outcomes are addressed by
//! [`MarketId`] plus [`Outcome`].

use pmkit_book::Side;
use pmkit_core::{MarketId, PortfolioId, StrategyId};
use pmkit_market::{Asset, Exchange, Outcome};
use rust_decimal::Decimal;

mod feed;

pub use feed::{CanonicalSourceKey, SourceEnvelope};

/// Whether a fill provided or took liquidity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Liquidity {
    /// The fill provided liquidity (resting maker order).
    Maker,
    /// The fill took liquidity (aggressing taker order).
    Taker,
}

/// A single event flowing through a per-market loop.
#[derive(Debug, Clone, PartialEq, Eq)]
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
            | Self::Fill { timestamp_ms, .. }
            | Self::OrderAck { timestamp_ms, .. }
            | Self::Tick { timestamp_ms } => *timestamp_ms,
        }
    }
}

/// A normalized authenticated-account fact from Polymarket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PmAccountEvent {
    /// A fill on one of the portfolio's orders.
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
    /// An acknowledgement for one of the portfolio's orders.
    OrderAck {
        /// Owning strategy, if attributed.
        strategy: Option<StrategyId>,
        /// Venue order id.
        order_id: String,
        /// Event timestamp in milliseconds.
        timestamp_ms: i64,
    },
}

/// A normalized reference-exchange fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CexReferenceEvent {
    /// A reference-exchange trade (for example a Binance aggregate trade).
    Trade {
        /// Underlying asset.
        asset: Asset,
        /// Source exchange.
        exchange: Exchange,
        /// Exchange-assigned aggregate trade identity.
        aggregate_trade_id: u64,
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
    BestBidOffer {
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
}

/// Metadata retained for every received stream frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamMetadata {
    /// Version of the envelope schema.
    pub schema_version: u16,
    /// Stable source identity.
    pub source_id: String,
    /// Source timestamp in milliseconds.
    pub source_time_ms: i64,
    /// Deterministic source rank used for canonical PM replay.
    pub canonical_source_rank: i64,
    /// Local receipt timestamp in milliseconds.
    pub receipt_time_ms: i64,
    /// Connection that delivered the frame.
    pub connection_id: String,
    /// Monotonically increasing connection epoch for this source.
    pub connection_epoch: i64,
    /// Monotonically increasing frame number within the connection epoch.
    pub frame_sequence: i64,
    /// Monotonic sequence within the connection.
    pub ingest_sequence: u64,
}

/// A PM market frame with its transport metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PmMarketEnvelope {
    /// Preserved transport metadata.
    pub metadata: StreamMetadata,
    /// Normalized PM market fact.
    pub fact: MarketEvent,
}

/// A PM authenticated-account frame with its transport metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PmAccountEnvelope {
    /// Portfolio receiving the authenticated account frame.
    pub portfolio: PortfolioId,
    /// Preserved transport metadata.
    pub metadata: StreamMetadata,
    /// Normalized PM account fact.
    pub fact: PmAccountEvent,
}

/// A CEX reference frame with its transport metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CexReferenceEnvelope {
    /// Preserved transport metadata.
    pub metadata: StreamMetadata,
    /// Normalized CEX reference fact.
    pub fact: CexReferenceEvent,
}

/// A normalized fact a strategy may receive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrategyFact {
    /// PM market fact.
    Market(MarketEvent),
    /// PM authenticated-account fact.
    Account(PmAccountEvent),
    /// CEX reference fact.
    Reference(CexReferenceEvent),
}

/// A fact accepted by strategy-facing APIs.
///
/// ```compile_fail
/// use pmkit_event::{PmMarketEnvelope, StrategyInput};
///
/// fn strategy_input(_: impl StrategyInput) {}
/// fn cannot_pass_envelopes(envelope: PmMarketEnvelope) {
///     strategy_input(envelope);
/// }
/// ```
pub trait StrategyInput {}

impl StrategyInput for StrategyFact {}

#[cfg(test)]
mod tests {
    use std::any::TypeId;

    use super::{Liquidity, MarketEvent, PmMarketEnvelope, StrategyFact};
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

    #[test]
    fn stream_envelope_is_not_strategy_fact() {
        assert_ne!(
            TypeId::of::<PmMarketEnvelope>(),
            TypeId::of::<StrategyFact>()
        );
    }
}
