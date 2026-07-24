//! Taker-fee and maker-rebate math for prediction markets.

use rust_decimal::Decimal;

/// Market category, which determines the taker-fee rate and maker rebate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketCategory {
    /// Crypto markets.
    Crypto,
    /// Sports markets.
    Sports,
    /// Finance markets.
    Finance,
    /// Politics markets.
    Politics,
    /// Economics markets.
    Economics,
    /// Culture markets.
    Culture,
    /// Weather markets.
    Weather,
    /// Mentions markets.
    Mentions,
    /// Tech markets.
    Tech,
    /// Geopolitics markets (currently fee-free).
    Geopolitics,
    /// Any other market category.
    Other,
}

impl MarketCategory {
    /// Returns the taker-fee coefficient in basis points for this category.
    #[must_use]
    pub const fn fee_rate_bps(self) -> i32 {
        match self {
            Self::Crypto => 700,
            Self::Sports => 300,
            Self::Finance | Self::Politics | Self::Mentions | Self::Tech => 400,
            Self::Economics | Self::Culture | Self::Weather | Self::Other => 500,
            Self::Geopolitics => 0,
        }
    }

    /// Returns the taker-fee rate coefficient for this category.
    #[must_use]
    pub const fn fee_rate(self) -> Decimal {
        match self {
            Self::Crypto => Decimal::from_parts(7, 0, 0, false, 2),
            Self::Sports => Decimal::from_parts(3, 0, 0, false, 2),
            Self::Finance | Self::Politics | Self::Mentions | Self::Tech => {
                Decimal::from_parts(4, 0, 0, false, 2)
            }
            Self::Economics | Self::Culture | Self::Weather | Self::Other => {
                Decimal::from_parts(5, 0, 0, false, 2)
            }
            Self::Geopolitics => Decimal::ZERO,
        }
    }

    /// Returns the maker-rebate percentage for this category.
    #[must_use]
    pub const fn maker_rebate_pct(self) -> Decimal {
        match self {
            Self::Crypto => Decimal::from_parts(20, 0, 0, false, 2),
            Self::Geopolitics => Decimal::ZERO,
            _ => Decimal::from_parts(25, 0, 0, false, 2),
        }
    }
}

/// Returns the taker fee for a single share at `price` in the given category.
#[inline]
#[must_use]
pub fn taker_fee_per_share(price: Decimal, category: MarketCategory) -> Decimal {
    let raw = category.fee_rate() * price * (Decimal::ONE - price);
    round_fee(raw)
}

/// Returns the taker fee for an order of `size` shares at `price`.
#[inline]
#[must_use]
pub fn taker_fee_order(size: Decimal, price: Decimal, category: MarketCategory) -> Decimal {
    let raw = size * category.fee_rate() * price * (Decimal::ONE - price);
    round_fee(raw)
}

/// Returns the effective per-share cost including the taker fee.
#[inline]
#[must_use]
pub fn effective_cost_per_share(price: Decimal, category: MarketCategory) -> Decimal {
    price + taker_fee_per_share(price, category)
}

#[inline]
fn round_fee(raw: Decimal) -> Decimal {
    raw.round_dp(5)
}

#[cfg(test)]
mod tests {
    use super::{MarketCategory, effective_cost_per_share, taker_fee_order, taker_fee_per_share};
    use rust_decimal::Decimal;

    fn cents(n: i64) -> Decimal {
        Decimal::new(n, 2)
    }

    #[test]
    fn crypto_fee_at_50c() {
        assert_eq!(
            taker_fee_per_share(cents(50), MarketCategory::Crypto),
            Decimal::new(1750, 5)
        );
    }

    #[test]
    fn crypto_fee_at_extremes() {
        assert_eq!(
            taker_fee_per_share(Decimal::ZERO, MarketCategory::Crypto),
            Decimal::ZERO
        );
        assert_eq!(
            taker_fee_per_share(Decimal::ONE, MarketCategory::Crypto),
            Decimal::ZERO
        );
    }

    #[test]
    fn crypto_table_full_verification() -> Result<(), rust_decimal::Error> {
        let cases: &[(i64, &str)] = &[
            (1, "0.07"),
            (5, "0.33"),
            (10, "0.63"),
            (15, "0.89"),
            (20, "1.12"),
            (25, "1.31"),
            (30, "1.47"),
            (35, "1.59"),
            (40, "1.68"),
            (45, "1.73"),
            (50, "1.75"),
            (55, "1.73"),
            (60, "1.68"),
            (65, "1.59"),
            (70, "1.47"),
            (75, "1.31"),
            (80, "1.12"),
            (85, "0.89"),
            (90, "0.63"),
            (95, "0.33"),
            (99, "0.07"),
        ];
        for &(price_cents, expected_str) in cases {
            let price = cents(price_cents);
            let fee_100 = taker_fee_order(Decimal::from(100_u32), price, MarketCategory::Crypto);
            let expected: Decimal = expected_str.parse()?;
            assert_eq!(
                fee_100.round_dp(2),
                expected,
                "crypto fee mismatch at p={price}"
            );
        }
        Ok(())
    }

    #[test]
    fn sports_table_full_verification() -> Result<(), rust_decimal::Error> {
        let cases: &[(i64, &str)] = &[
            (1, "0.03"),
            (5, "0.14"),
            (10, "0.27"),
            (50, "0.75"),
            (90, "0.27"),
            (99, "0.03"),
        ];
        for &(price_cents, expected_str) in cases {
            let price = cents(price_cents);
            let fee_100 = taker_fee_order(Decimal::from(100_u32), price, MarketCategory::Sports);
            let expected: Decimal = expected_str.parse()?;
            assert_eq!(
                fee_100.round_dp(2),
                expected,
                "sports fee mismatch at p={price}"
            );
        }
        Ok(())
    }

    #[test]
    fn finance_table_verification() -> Result<(), rust_decimal::Error> {
        let cases: &[(i64, &str)] = &[(10, "0.36"), (50, "1.00"), (90, "0.36")];
        for &(price_cents, expected_str) in cases {
            let price = cents(price_cents);
            let fee_100 = taker_fee_order(Decimal::from(100_u32), price, MarketCategory::Finance);
            let expected: Decimal = expected_str.parse()?;
            assert_eq!(
                fee_100.round_dp(2),
                expected,
                "finance fee mismatch at p={price}"
            );
        }
        Ok(())
    }

    #[test]
    fn economics_table_verification() -> Result<(), rust_decimal::Error> {
        let cases: &[(i64, &str)] = &[(10, "0.45"), (50, "1.25"), (90, "0.45")];
        for &(price_cents, expected_str) in cases {
            let price = cents(price_cents);
            let fee_100 = taker_fee_order(Decimal::from(100_u32), price, MarketCategory::Economics);
            let expected: Decimal = expected_str.parse()?;
            assert_eq!(
                fee_100.round_dp(2),
                expected,
                "economics fee mismatch at p={price}"
            );
        }
        Ok(())
    }

    #[test]
    fn geopolitics_is_free() {
        assert_eq!(
            taker_fee_per_share(cents(50), MarketCategory::Geopolitics),
            Decimal::ZERO
        );
    }

    #[test]
    fn symmetry() {
        for cat in [
            MarketCategory::Crypto,
            MarketCategory::Sports,
            MarketCategory::Finance,
        ] {
            assert_eq!(
                taker_fee_per_share(cents(30), cat),
                taker_fee_per_share(cents(70), cat),
                "fee not symmetric for {cat:?}"
            );
        }
    }

    #[test]
    fn effective_cost_includes_fee() {
        let price = cents(30);
        let cost = effective_cost_per_share(price, MarketCategory::Crypto);
        assert_eq!(
            cost,
            price + taker_fee_per_share(price, MarketCategory::Crypto)
        );
        assert!(cost > price);
    }

    #[test]
    fn order_fee_single_share_matches_per_share() {
        for p in [10, 20, 30, 40, 50, 60, 70, 80, 90] {
            let price = cents(p);
            assert_eq!(
                taker_fee_per_share(price, MarketCategory::Crypto),
                taker_fee_order(Decimal::ONE, price, MarketCategory::Crypto),
                "mismatch at p={price}"
            );
        }
    }

    #[test]
    fn fee_rounds_to_5dp() {
        let fee = taker_fee_per_share(cents(1), MarketCategory::Crypto);
        assert!(fee.scale() <= 5);
        assert!(fee > Decimal::ZERO);
    }
}
