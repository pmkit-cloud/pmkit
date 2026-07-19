//! Order-book and position helper functions.

use pmkit_market::Outcome;
use rust_decimal::Decimal;

use crate::{OrderBookL2, Position};

/// Lowest ask price in the book, scanning all levels.
#[inline]
#[must_use]
pub fn best_ask(book: &OrderBookL2) -> Option<Decimal> {
    book.asks.iter().map(|(px, _)| *px).min()
}

/// Highest bid price in the book, scanning all levels.
#[inline]
#[must_use]
pub fn best_bid(book: &OrderBookL2) -> Option<Decimal> {
    book.bids.iter().map(|(px, _)| *px).max()
}

/// Volume-weighted mid price using top-of-book quantities.
///
/// `mid = (bid × ask_qty + ask × bid_qty) / (bid_qty + ask_qty)`
#[must_use]
pub fn mid_price(book: &OrderBookL2) -> Option<Decimal> {
    let (best_bid_px, bid_qty) = book.bids.iter().copied().max_by_key(|(px, _)| *px)?;
    let (best_ask_px, ask_qty) = book.asks.iter().copied().min_by_key(|(px, _)| *px)?;
    let total_qty = bid_qty + ask_qty;
    if total_qty > Decimal::ZERO {
        Some((best_bid_px * ask_qty + best_ask_px * bid_qty) / total_qty)
    } else {
        Some((best_bid_px + best_ask_px) / Decimal::TWO)
    }
}

/// Bid-ask spread, scanning all levels.
#[inline]
#[must_use]
pub fn spread(book: &OrderBookL2) -> Option<Decimal> {
    let bid = book.bids.iter().map(|(px, _)| *px).max()?;
    let ask = book.asks.iter().map(|(px, _)| *px).min()?;
    Some(ask - bid)
}

/// Sum of held quantity for one outcome across positions.
#[must_use]
pub fn held_qty(positions: &[Position], outcome: Outcome) -> Decimal {
    positions
        .iter()
        .filter(|p| p.outcome == outcome)
        .map(|p| p.qty)
        .sum()
}

/// Returns `(up_qty, down_qty)` totals from a position slice.
#[must_use]
pub fn position_quantities(positions: &[Position]) -> (Decimal, Decimal) {
    let mut up = Decimal::ZERO;
    let mut down = Decimal::ZERO;
    for position in positions {
        match position.outcome {
            Outcome::Up => up += position.qty,
            Outcome::Down => down += position.qty,
        }
    }
    (up, down)
}

#[cfg(test)]
mod tests {
    use super::{best_ask, best_bid, held_qty, mid_price, position_quantities, spread};
    use crate::{OrderBookL2, Position};
    use pmkit_market::Outcome;
    use rust_decimal::Decimal;

    fn book() -> OrderBookL2 {
        OrderBookL2 {
            bids: vec![
                (Decimal::new(44, 2), Decimal::from(10)),
                (Decimal::new(45, 2), Decimal::from(20)),
            ],
            asks: vec![
                (Decimal::new(47, 2), Decimal::from(10)),
                (Decimal::new(46, 2), Decimal::from(30)),
            ],
            timestamp_ms: 0,
            last_trade_price: None,
        }
    }

    fn position(outcome: Outcome, qty: i64) -> Position {
        Position {
            outcome,
            qty: Decimal::from(qty),
            avg_entry: Decimal::new(50, 2),
            unrealized_pnl: Decimal::ZERO,
        }
    }

    #[test]
    fn best_prices_scan_all_levels() {
        assert_eq!(best_bid(&book()), Some(Decimal::new(45, 2)));
        assert_eq!(best_ask(&book()), Some(Decimal::new(46, 2)));
        assert_eq!(spread(&book()), Some(Decimal::new(1, 2)));
    }

    #[test]
    fn volume_weighted_mid_lands_in_spread() -> Result<(), &'static str> {
        let mid = mid_price(&book()).ok_or("expected mid")?;
        assert!(mid > Decimal::new(45, 2));
        assert!(mid < Decimal::new(46, 2));
        Ok(())
    }

    #[test]
    fn position_totals_split_by_outcome() {
        let positions = [
            position(Outcome::Up, 10),
            position(Outcome::Up, 5),
            position(Outcome::Down, 3),
        ];
        assert_eq!(held_qty(&positions, Outcome::Up), Decimal::from(15));
        assert_eq!(
            position_quantities(&positions),
            (Decimal::from(15), Decimal::from(3))
        );
    }
}
