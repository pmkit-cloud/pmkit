//! Fair-value pricing models for binary prediction markets.

use rust_decimal::{Decimal, MathematicalOps};

use crate::signals;

/// Default basis volatility added in quadrature to prevent zero-vol edge cases.
#[must_use]
pub fn default_sigma_basis() -> Decimal {
    Decimal::new(2, 3)
}

/// Parameters for the GBM binary pricing model.
#[derive(Debug, Clone, Copy)]
pub struct GbmParams {
    /// Per-second volatility of the underlying.
    pub sigma_1s: Decimal,
    /// Time to expiry in seconds.
    pub t_seconds: Decimal,
    /// Current spot price.
    pub spot: Decimal,
    /// Strike price.
    pub strike: Decimal,
    /// Volatility multiplier applied to `sigma_1s`.
    pub vol_mult: Decimal,
    /// Lower clamp on the resulting probability.
    pub floor: Decimal,
    /// Upper clamp on the resulting probability.
    pub cap: Decimal,
    /// Basis volatility added in quadrature to avoid the zero-vol pathology.
    pub sigma_basis: Decimal,
}

/// Black-Scholes / GBM binary probability `P(UP)` via `N(d2)`.
///
/// `d2 = (ln(S/K) - σ²T/2) / (σ√T)`
#[must_use]
pub fn binary_gbm_p_up(p: &GbmParams) -> Decimal {
    let GbmParams {
        sigma_1s,
        t_seconds,
        spot,
        strike,
        vol_mult,
        floor,
        cap,
        sigma_basis,
    } = *p;
    let sigma_eff = sigma_1s * vol_mult;
    // σ_reg = √(σ_eff² + σ_basis²) — prevents zero-vol pathology
    let sigma_reg = (sigma_eff * sigma_eff + sigma_basis * sigma_basis)
        .sqrt()
        .unwrap_or(sigma_basis);
    let sigma_t = sigma_reg * t_seconds.max(Decimal::ZERO).sqrt().unwrap_or(Decimal::ZERO);
    if sigma_t <= Decimal::ZERO || spot <= Decimal::ZERO || strike <= Decimal::ZERO {
        return Decimal::new(5, 1).clamp(floor, cap);
    }

    let ln_s_k = (spot / strike).ln();
    let d2 = (ln_s_k - sigma_reg * sigma_reg * t_seconds / Decimal::TWO) / sigma_t;
    signals::norm_cdf_approx(d2).clamp(floor, cap)
}

/// LMSR-implied `P(UP)` from bid-side depth via softmax.
///
/// `P(UP) = exp(depth_up/b) / (exp(depth_up/b) + exp(depth_down/b))`
#[must_use]
pub fn lmsr_implied_p_up(depth_up: Decimal, depth_down: Decimal, b: Decimal) -> Option<Decimal> {
    if b <= Decimal::ZERO {
        return None;
    }
    let max_d = depth_up.max(depth_down);
    let exp_up = ((depth_up - max_d) / b)
        .clamp(Decimal::new(-20, 0), Decimal::ZERO)
        .exp();
    let exp_down = ((depth_down - max_d) / b)
        .clamp(Decimal::new(-20, 0), Decimal::ZERO)
        .exp();
    let denom = exp_up + exp_down;
    if denom <= Decimal::ZERO {
        return None;
    }
    Some(exp_up / denom)
}

/// Absolute probability-space gap between two fair values.
#[inline]
#[must_use]
pub fn probability_gap(lhs: Decimal, rhs: Decimal) -> Decimal {
    (lhs - rhs).abs()
}

/// Logit-space gap — more sensitive at the tails than probability-space.
#[inline]
#[must_use]
pub fn logit_diff(lhs: Decimal, rhs: Decimal) -> Decimal {
    (signals::logit(lhs) - signals::logit(rhs)).abs()
}

/// Blend two fair values: `w × book_p + (1-w) × model_p`.
#[inline]
#[must_use]
pub fn blend_probabilities(book_weight: Decimal, book_p: Decimal, model_p: Decimal) -> Decimal {
    book_weight * book_p + (Decimal::ONE - book_weight) * model_p
}

#[cfg(test)]
mod tests {
    use super::{
        GbmParams, binary_gbm_p_up, blend_probabilities, default_sigma_basis, lmsr_implied_p_up,
        logit_diff, probability_gap,
    };
    use rust_decimal::Decimal;

    #[test]
    fn gbm_at_the_money_near_half() {
        let p = binary_gbm_p_up(&GbmParams {
            sigma_1s: Decimal::new(2, 5),
            t_seconds: Decimal::from(60_u64),
            spot: Decimal::from(50_000_u64),
            strike: Decimal::from(50_000_u64),
            vol_mult: Decimal::ONE,
            floor: Decimal::new(1, 2),
            cap: Decimal::new(99, 2),
            sigma_basis: default_sigma_basis(),
        });
        assert!(p > Decimal::new(45, 2));
        assert!(p < Decimal::new(55, 2));
    }

    #[test]
    fn lmsr_equal_depth_is_half() -> Result<(), &'static str> {
        let p = lmsr_implied_p_up(Decimal::from(100_u64), Decimal::from(100_u64), Decimal::ONE)
            .ok_or("equal depth should price")?;
        assert_eq!(p, Decimal::new(5, 1));
        Ok(())
    }

    #[test]
    fn probability_gap_is_absolute() {
        assert_eq!(
            probability_gap(Decimal::new(7, 1), Decimal::new(4, 1)),
            Decimal::new(3, 1)
        );
    }

    #[test]
    fn logit_diff_zero_for_equal() {
        assert_eq!(
            logit_diff(Decimal::new(6, 1), Decimal::new(6, 1)),
            Decimal::ZERO
        );
    }

    #[test]
    fn blend_respects_weight() {
        assert_eq!(
            blend_probabilities(Decimal::new(7, 1), Decimal::new(8, 1), Decimal::new(2, 1)),
            Decimal::new(62, 2)
        );
    }
}
