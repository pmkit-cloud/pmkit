//! Risk and sizing utility functions.
//!
//! These are shared tools that strategies can optionally use. They are NOT a
//! shared sizing pipeline — each strategy decides how to combine them.

use rust_decimal::Decimal;

use crate::Side;

/// Full Kelly fraction: `f = max(0, p - q×c/(1-c))`.
///
/// `p_win` is the model probability of winning; `effective_cost` is price plus
/// fee per share.
#[inline]
#[must_use]
pub fn kelly_full(p_win: Decimal, effective_cost: Decimal) -> Decimal {
    if effective_cost >= Decimal::ONE || effective_cost <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let q = Decimal::ONE - p_win;
    (p_win - q * effective_cost / (Decimal::ONE - effective_cost)).max(Decimal::ZERO)
}

/// Snaps a price to the nearest valid tick for the given side.
///
/// Buy floors to the tick; sell ceils to the tick.
#[inline]
#[must_use]
pub fn snap_price_to_tick(price: Decimal, tick: Decimal, side: Side) -> Decimal {
    if tick <= Decimal::ZERO {
        return price;
    }
    match side {
        Side::Buy => (price / tick).floor() * tick,
        Side::Sell => (price / tick).ceil() * tick,
    }
}

/// Clamps a post-only maker price so it does not cross the book.
///
/// A buy price must stay below `best_ask`; a sell price must stay above
/// `best_bid`. Returns `None` when no valid price exists (the spread is closed).
#[must_use]
pub fn clamp_post_only(
    price: Decimal,
    side: Side,
    best_bid: Option<Decimal>,
    best_ask: Option<Decimal>,
    tick: Decimal,
) -> Option<Decimal> {
    match side {
        Side::Buy => {
            let ask = best_ask?;
            let max_bid = ask - tick;
            if max_bid <= Decimal::ZERO {
                return None;
            }
            Some(price.min(max_bid))
        }
        Side::Sell => {
            let bid = best_bid?;
            let min_ask = bid + tick;
            if min_ask >= Decimal::ONE {
                return None;
            }
            Some(price.max(min_ask))
        }
    }
}

/// Drawdown-based penalty: `max(0, 1 - λ × dd²)` where `dd = (peak - current) / peak`.
///
/// `λ = 5` — an aggressive penalty that drops to zero at roughly 45% drawdown.
#[inline]
#[must_use]
pub fn drawdown_penalty(current_balance: Decimal, peak_balance: Decimal) -> Decimal {
    let peak = peak_balance.max(current_balance);
    if peak <= Decimal::ZERO {
        return Decimal::ONE;
    }
    let dd_frac = (peak - current_balance) / peak;
    let lambda = Decimal::new(5, 0);
    (Decimal::ONE - lambda * dd_frac * dd_frac).max(Decimal::ZERO)
}

/// Budget cap: the fraction of bankroll available for this market window.
#[inline]
#[must_use]
pub fn budget_cap(bankroll: Decimal, active_markets: u32, max_fraction: Decimal) -> Decimal {
    if active_markets == 0 {
        return bankroll * max_fraction;
    }
    bankroll * max_fraction / Decimal::from(active_markets)
}

/// VPIN (Volume-Synchronized Probability of Informed Trading).
///
/// `VPIN = |buy_vol - sell_vol| / (buy_vol + sell_vol)`, or zero when there is
/// no volume.
#[inline]
#[must_use]
pub fn vpin(buy_volume: Decimal, sell_volume: Decimal) -> Decimal {
    let total = buy_volume + sell_volume;
    if total <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    (buy_volume - sell_volume).abs() / total
}

/// Order-flow-imbalance factor as a multiplier around `1.0`.
///
/// Positive flow aligned with `is_up_direction` raises the factor; opposing
/// flow lowers it. The result is clamped to `[0.5, cap]`.
#[inline]
#[must_use]
pub fn ofi_factor(
    buy_volume: Decimal,
    sell_volume: Decimal,
    is_up_direction: bool,
    strength: Decimal,
    cap: Decimal,
) -> Decimal {
    let total = buy_volume + sell_volume;
    if total <= Decimal::ZERO {
        return Decimal::ONE;
    }
    let ofi = (buy_volume - sell_volume) / total;
    let aligned = if is_up_direction { ofi } else { -ofi };
    (Decimal::ONE + aligned * strength).clamp(Decimal::new(5, 1), cap)
}

#[cfg(test)]
mod tests {
    use super::{
        budget_cap, clamp_post_only, drawdown_penalty, kelly_full, ofi_factor, snap_price_to_tick,
        vpin,
    };
    use crate::Side;
    use rust_decimal::Decimal;

    #[test]
    fn kelly_zero_for_bad_cost() {
        assert_eq!(kelly_full(Decimal::new(6, 1), Decimal::ONE), Decimal::ZERO);
        assert_eq!(kelly_full(Decimal::new(6, 1), Decimal::ZERO), Decimal::ZERO);
    }

    #[test]
    fn kelly_positive_for_edge() {
        assert!(kelly_full(Decimal::new(6, 1), Decimal::new(45, 2)) > Decimal::ZERO);
    }

    #[test]
    fn snap_buy_floors_and_sell_ceils() {
        assert_eq!(
            snap_price_to_tick(Decimal::new(457, 3), Decimal::new(1, 2), Side::Buy),
            Decimal::new(45, 2)
        );
        assert_eq!(
            snap_price_to_tick(Decimal::new(451, 3), Decimal::new(1, 2), Side::Sell),
            Decimal::new(46, 2)
        );
    }

    #[test]
    fn clamp_post_only_keeps_maker_off_the_cross() {
        let clamped = clamp_post_only(
            Decimal::new(50, 2),
            Side::Buy,
            None,
            Some(Decimal::new(46, 2)),
            Decimal::new(1, 2),
        );
        assert_eq!(clamped, Some(Decimal::new(45, 2)));
    }

    #[test]
    fn drawdown_penalty_bounds() {
        assert_eq!(
            drawdown_penalty(Decimal::from(100), Decimal::from(100)),
            Decimal::ONE
        );
        assert!(drawdown_penalty(Decimal::from(50), Decimal::from(100)) < Decimal::ONE);
    }

    #[test]
    fn budget_cap_splits_across_markets() {
        assert_eq!(
            budget_cap(Decimal::from(100), 4, Decimal::new(2, 1)),
            Decimal::from(5)
        );
    }

    #[test]
    fn vpin_reports_imbalance() {
        assert_eq!(vpin(Decimal::ZERO, Decimal::ZERO), Decimal::ZERO);
        assert_eq!(vpin(Decimal::from(100), Decimal::ZERO), Decimal::ONE);
        assert_eq!(vpin(Decimal::from(50), Decimal::from(50)), Decimal::ZERO);
    }

    #[test]
    fn ofi_factor_neutral_without_flow() {
        assert_eq!(
            ofi_factor(
                Decimal::ZERO,
                Decimal::ZERO,
                true,
                Decimal::ONE,
                Decimal::from(2)
            ),
            Decimal::ONE
        );
    }
}
