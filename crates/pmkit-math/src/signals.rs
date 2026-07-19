//! Pure signal computation functions.
//!
//! Every function is stateless — takes `Decimal` inputs, returns `Decimal`.
//! No references to engine state, feeds, or market types.

use rust_decimal::{Decimal, MathematicalOps};

/// Normal CDF approximation. Clamps input to \[-10, 10\].
#[inline]
#[must_use]
pub fn norm_cdf_approx(x: Decimal) -> Decimal {
    x.clamp(Decimal::new(-10, 0), Decimal::new(10, 0))
        .norm_cdf()
}

/// Sigmoid: σ(x) = 1 / (1 + e^{-x}). Clamped to \[-10, 10\].
#[inline]
#[must_use]
pub fn sigmoid(x: Decimal) -> Decimal {
    let clamped = x.clamp(Decimal::new(-10, 0), Decimal::new(10, 0));
    let exp_neg = (Decimal::ZERO - clamped).exp();
    Decimal::ONE / (Decimal::ONE + exp_neg)
}

/// Logit: logit(p) = ln(p / (1 − p)). Clamped to \[ε, 1−ε\].
#[inline]
#[must_use]
pub fn logit(p: Decimal) -> Decimal {
    let eps = Decimal::new(1, 4); // 0.0001
    let p_clamped = p.clamp(eps, Decimal::ONE - eps);
    (p_clamped / (Decimal::ONE - p_clamped)).ln()
}

/// Normalised momentum: `delta / sigma`. Clamped to \[-5, 5\].
#[inline]
#[must_use]
pub fn momentum(delta_dollar: Decimal, sigma_dollar: Decimal) -> Decimal {
    if sigma_dollar < Decimal::new(1, 10) {
        return Decimal::ZERO;
    }
    (delta_dollar / sigma_dollar).clamp(Decimal::new(-5, 0), Decimal::new(5, 0))
}

#[cfg(test)]
mod tests {
    use super::{logit, momentum, norm_cdf_approx, sigmoid};
    use rust_decimal::Decimal;

    #[test]
    fn norm_cdf_half_at_zero() {
        assert_eq!(norm_cdf_approx(Decimal::ZERO), Decimal::new(5, 1));
    }

    #[test]
    fn sigmoid_half_at_zero() {
        assert_eq!(sigmoid(Decimal::ZERO), Decimal::new(5, 1));
    }

    #[test]
    fn logit_zero_at_half() {
        assert_eq!(logit(Decimal::new(5, 1)), Decimal::ZERO);
    }

    #[test]
    fn momentum_zero_when_no_vol() {
        assert_eq!(momentum(Decimal::from(100), Decimal::ZERO), Decimal::ZERO);
    }

    #[test]
    fn momentum_clamped() {
        assert_eq!(
            momentum(Decimal::from(1000), Decimal::ONE),
            Decimal::new(5, 0)
        );
    }
}
