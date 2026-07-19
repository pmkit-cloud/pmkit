//! Polymarket venue adapter for `PMKit`.
//!
//! This crate is the venue boundary: it depends on the Polymarket client SDK
//! and maps between neutral `PMKit` domain types and Polymarket-specific types.
//! Neutral core crates never depend on this adapter.

use pmkit_book::Side;
use polymarket_client_sdk_v2::clob::types::Side as VenueSide;

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

#[cfg(test)]
mod tests {
    use super::{from_venue_side, to_venue_side};
    use pmkit_book::Side;
    use polymarket_client_sdk_v2::clob::types::Side as VenueSide;

    #[test]
    fn side_round_trips() {
        assert_eq!(from_venue_side(to_venue_side(Side::Buy)), Some(Side::Buy));
        assert_eq!(from_venue_side(to_venue_side(Side::Sell)), Some(Side::Sell));
    }

    #[test]
    fn unknown_venue_side_is_none() {
        assert_eq!(from_venue_side(VenueSide::Unknown), None);
    }
}
