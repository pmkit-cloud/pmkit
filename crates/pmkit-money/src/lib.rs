//! USDC monetary amount type for `PMKit`.
//!
//! [`Money`] wraps a [`rust_decimal::Decimal`] number of USDC. Construct whole
//! units with [`Money::usdc`] or 6-decimal micro-USDC with [`Money::micros`].

use std::fmt;
use std::ops::{Add, AddAssign, Neg, Sub, SubAssign};

use rust_decimal::Decimal;

/// A USDC monetary amount.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Money(Decimal);

impl Money {
    /// Zero USDC.
    pub const ZERO: Self = Self(Decimal::ZERO);

    /// Creates an amount from whole USDC units.
    #[must_use]
    pub fn usdc(units: i64) -> Self {
        Self(Decimal::from(units))
    }

    /// Creates an amount from 6-decimal micro-USDC (`1 USDC = 1_000_000`).
    #[must_use]
    pub fn micros(raw: i64) -> Self {
        Self(Decimal::new(raw, 6))
    }

    /// Wraps a decimal number of USDC.
    #[must_use]
    pub const fn from_decimal(amount: Decimal) -> Self {
        Self(amount)
    }

    /// Returns the amount as a decimal number of USDC.
    #[must_use]
    pub const fn as_decimal(self) -> Decimal {
        self.0
    }

    /// Returns `true` when the amount is greater than zero.
    #[must_use]
    pub fn is_positive(self) -> bool {
        self.0 > Decimal::ZERO
    }
}

impl fmt::Display for Money {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} USDC", self.0)
    }
}

impl Add for Money {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }
}

impl Sub for Money {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self(self.0 - other.0)
    }
}

impl Neg for Money {
    type Output = Self;

    fn neg(self) -> Self {
        Self(-self.0)
    }
}

impl AddAssign for Money {
    fn add_assign(&mut self, other: Self) {
        self.0 += other.0;
    }
}

impl SubAssign for Money {
    fn sub_assign(&mut self, other: Self) {
        self.0 -= other.0;
    }
}

#[cfg(test)]
mod tests {
    use super::Money;
    use rust_decimal::Decimal;

    #[test]
    fn usdc_and_micros_agree() {
        assert_eq!(Money::usdc(1), Money::micros(1_000_000));
        assert_eq!(Money::usdc(100_000).as_decimal(), Decimal::from(100_000));
    }

    #[test]
    fn arithmetic_and_sign() {
        let sum = Money::usdc(10) + Money::usdc(5);
        assert_eq!(sum, Money::usdc(15));
        assert_eq!(sum - Money::usdc(20), -Money::usdc(5));
        assert!(Money::usdc(1).is_positive());
        assert!(!Money::ZERO.is_positive());

        let mut balance = Money::usdc(3);
        balance += Money::usdc(2);
        balance -= Money::usdc(1);
        assert_eq!(balance, Money::usdc(4));
    }

    #[test]
    fn display_has_currency_suffix() {
        assert_eq!(Money::usdc(42).to_string(), "42 USDC");
    }

    #[test]
    fn ordering_follows_amount() {
        assert!(Money::usdc(1) < Money::usdc(2));
        assert!(Money::micros(500_000) < Money::usdc(1));
    }
}
