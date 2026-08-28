//! Polymarket venue adapter for `PMKit`.
//!
//! This crate is the venue boundary: it depends on the Polymarket client SDK
//! and maps between neutral `PMKit` domain types and Polymarket-specific types.
//! Neutral core crates never depend on this adapter.

use pmkit_book::Side;
use pmkit_core::MarketId;
use pmkit_exec::PlaceOrder;
use pmkit_market::Outcome;
use polymarket_client_sdk_v2::clob::types::Side as VenueSide;
use polymarket_client_sdk_v2::types::U256;
use rust_decimal::Decimal;

mod account;
mod discovery;
mod execution;
mod gamma_discovery;
mod historical;
mod live;
mod public_protocol;
mod public_tape;
mod public_tape_contract;
mod rtds;
mod subscription;

#[cfg(test)]
mod discovery_subscription_tests;
#[cfg(test)]
mod gamma_discovery_tests;
#[cfg(test)]
mod public_event_surface_tests;
#[cfg(test)]
mod public_protocol_validation_tests;

#[cfg(test)]
mod public_tape_tests;

pub use account::PolymarketUserData;
pub use discovery::{DiscoveryError, GammaPageRequest, discover_subscription_snapshot};
pub use execution::PolymarketExecutor;
pub use gamma_discovery::GammaDiscovery;
pub use historical::PolymarketHistoricalData;
pub use live::{
    PolymarketFrameAdapter, PolymarketLiveData, RawFrameAdapterError, RawPolymarketFrameAdapter,
    parse_market_frame,
};
pub use public_protocol::{
    PublicInboundFrame, PublicMarketEvent, PublicOutboundFrame, PublicProtocolError,
    decode_public_inbound, encode_public_outbound,
};
pub use public_tape::{PublicTapeImportError, PublicTapeImportReport, PublicTapeImporter};
pub use public_tape_contract::{certify_v2_public_market_input, certify_v2_public_market_source};
pub use rtds::{
    POLYMARKET_RTDS_ENDPOINT, POLYMARKET_RTDS_HEARTBEAT_INTERVAL, POLYMARKET_RTDS_SOURCE_ID,
    POLYMARKET_RTDS_TOPIC, PolymarketRtdsLive, PolymarketRtdsParseError,
    parse_polymarket_rtds_twap, parse_polymarket_rtds_twap_bytes, polymarket_rtds_subscription,
};
pub use subscription::{
    DiscoverySnapshot, GammaMarket, GammaOutcome, RecurringFamily, ReplicaSubscriptionPlan,
    SubscriptionShard,
};

/// Maps a neutral `PMKit` [`Side`] to the Polymarket venue side.
#[must_use]
pub const fn to_venue_side(side: Side) -> VenueSide {
    match side {
        Side::Buy => VenueSide::Buy,
        Side::Sell => VenueSide::Sell,
    }
}

/// Maps a Polymarket venue side to a neutral `PMKit` [`Side`].
///
/// Returns `None` for an unrecognised venue side.
#[must_use]
pub const fn from_venue_side(side: VenueSide) -> Option<Side> {
    match side {
        VenueSide::Buy => Some(Side::Buy),
        VenueSide::Sell => Some(Side::Sell),
        _ => None,
    }
}

/// Resolves between a neutral market outcome and its Polymarket token ids.
#[derive(Debug, Clone)]
pub struct MarketTokens {
    market: MarketId,
    up: U256,
    down: U256,
}

impl MarketTokens {
    /// Creates a resolver for a market's up and down outcome tokens.
    #[must_use]
    pub const fn new(market: MarketId, up: U256, down: U256) -> Self {
        Self { market, up, down }
    }

    /// Returns the market these tokens belong to.
    #[must_use]
    pub const fn market(&self) -> &MarketId {
        &self.market
    }

    /// Returns the venue token id for an outcome.
    #[must_use]
    pub const fn token(&self, outcome: Outcome) -> U256 {
        match outcome {
            Outcome::Up => self.up,
            Outcome::Down => self.down,
        }
    }

    /// Returns the outcome for a venue token id, if it belongs to this market.
    #[must_use]
    pub fn outcome(&self, token: &U256) -> Option<Outcome> {
        if *token == self.up {
            Some(Outcome::Up)
        } else if *token == self.down {
            Some(Outcome::Down)
        } else {
            None
        }
    }
}

/// The venue-specific inputs for one CLOB order, ready to sign and post.
#[derive(Debug, Clone)]
pub struct VenueOrderInputs {
    /// The outcome token to trade.
    pub token_id: U256,
    /// Buy or sell.
    pub side: VenueSide,
    /// Limit price.
    pub price: Decimal,
    /// Order size in shares.
    pub size: Decimal,
    /// Whether the order must rest as a maker.
    pub post_only: bool,
}

/// Maps a neutral order to Polymarket CLOB order inputs using a token resolver.
///
/// Returns `None` when the order belongs to a different market.
#[must_use]
pub fn venue_order_inputs(order: &PlaceOrder, tokens: &MarketTokens) -> Option<VenueOrderInputs> {
    (order.market == *tokens.market()).then(|| VenueOrderInputs {
        token_id: tokens.token(order.outcome),
        side: to_venue_side(order.side),
        price: order.price,
        size: order.qty,
        post_only: order.post_only,
    })
}

#[cfg(test)]
mod tests {
    use super::{MarketTokens, from_venue_side, to_venue_side, venue_order_inputs};
    use pmkit_book::Side;
    use pmkit_core::{EmptyIdError, MarketId};
    use pmkit_exec::{PlaceOrder, TimeInForce};
    use pmkit_market::Outcome;
    use polymarket_client_sdk_v2::clob::types::Side as VenueSide;
    use polymarket_client_sdk_v2::types::U256;
    use rust_decimal::Decimal;

    #[test]
    fn side_round_trips() {
        assert_eq!(from_venue_side(to_venue_side(Side::Buy)), Some(Side::Buy));
        assert_eq!(from_venue_side(to_venue_side(Side::Sell)), Some(Side::Sell));
    }

    #[test]
    fn unknown_venue_side_is_none() {
        assert_eq!(from_venue_side(VenueSide::Unknown), None);
    }

    #[test]
    fn tokens_resolve_both_directions() -> Result<(), EmptyIdError> {
        let tokens = MarketTokens::new(
            MarketId::new("btc-5m")?,
            U256::from(1_u64),
            U256::from(2_u64),
        );
        assert_eq!(tokens.token(Outcome::Up), U256::from(1_u64));
        assert_eq!(tokens.token(Outcome::Down), U256::from(2_u64));
        assert_eq!(tokens.outcome(&U256::from(1_u64)), Some(Outcome::Up));
        assert_eq!(tokens.outcome(&U256::from(2_u64)), Some(Outcome::Down));
        assert_eq!(tokens.outcome(&U256::from(9_u64)), None);
        Ok(())
    }

    #[test]
    fn order_maps_to_venue_inputs() -> Result<(), Box<dyn std::error::Error>> {
        let tokens = MarketTokens::new(
            MarketId::new("btc-5m")?,
            U256::from(1_u64),
            U256::from(2_u64),
        );
        let order = PlaceOrder {
            market: MarketId::new("btc-5m")?,
            outcome: Outcome::Down,
            side: Side::Sell,
            price: Decimal::new(55, 2),
            qty: Decimal::from(20),
            post_only: true,
            tif: TimeInForce::Gtc,
        };
        let Some(inputs) = venue_order_inputs(&order, &tokens) else {
            return Err("expected matching market tokens".into());
        };
        assert_eq!(inputs.token_id, U256::from(2_u64));
        assert_eq!(inputs.price, Decimal::new(55, 2));
        assert_eq!(inputs.size, Decimal::from(20));
        assert!(inputs.post_only);

        let other_market = PlaceOrder {
            market: MarketId::new("eth-5m")?,
            ..order
        };
        assert!(venue_order_inputs(&other_market, &tokens).is_none());
        Ok(())
    }
}
