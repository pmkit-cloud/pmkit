use std::fmt;

use pmkit_event::Liquidity;
use pmkit_math::fees::MarketCategory;
use rust_decimal::Decimal;

const MIN_MAKER_FEE_BPS: i32 = -10_000;
const MAX_FEE_BPS: i32 = 10_000;

/// Validated maker and taker fee coefficients for simulated fills.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeeModel {
    maker_bps: i32,
    taker_bps: i32,
}

impl FeeModel {
    /// Creates a fee model bounded to a 100% maker rebate or fee coefficient.
    ///
    /// # Errors
    ///
    /// Returns [`FeeModelError`] when either coefficient is outside its safe range.
    pub const fn try_new(maker_bps: i32, taker_bps: i32) -> Result<Self, FeeModelError> {
        if maker_bps < MIN_MAKER_FEE_BPS || maker_bps > MAX_FEE_BPS {
            return Err(FeeModelError::MakerOutOfRange { maker_bps });
        }
        if taker_bps < 0 || taker_bps > MAX_FEE_BPS {
            return Err(FeeModelError::TakerOutOfRange { taker_bps });
        }
        Ok(Self {
            maker_bps,
            taker_bps,
        })
    }

    /// Returns the legacy fee model for one market category.
    #[must_use]
    pub const fn for_category(category: MarketCategory) -> Self {
        Self {
            maker_bps: 0,
            taker_bps: category.fee_rate_bps(),
        }
    }

    /// Returns the maker fee coefficient; negative values are rebates.
    #[must_use]
    pub const fn maker_bps(self) -> i32 {
        self.maker_bps
    }

    /// Returns the taker fee coefficient.
    #[must_use]
    pub const fn taker_bps(self) -> i32 {
        self.taker_bps
    }

    pub(crate) fn fee_order(
        self,
        size: Decimal,
        price: Decimal,
        liquidity: Liquidity,
    ) -> Option<Decimal> {
        let bps = match liquidity {
            Liquidity::Maker => self.maker_bps,
            Liquidity::Taker => self.taker_bps,
        };
        let rate = Decimal::from(bps).checked_div(Decimal::from(10_000))?;
        size.checked_mul(rate)?
            .checked_mul(price)?
            .checked_mul(Decimal::ONE.checked_sub(price)?)
            .map(|fee| fee.round_dp(5))
    }
}

impl Default for FeeModel {
    fn default() -> Self {
        Self::for_category(MarketCategory::Crypto)
    }
}

/// Invalid simulation fee coefficients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeeModelError {
    /// The maker coefficient is below the rebate floor or above the fee ceiling.
    MakerOutOfRange {
        /// Rejected maker coefficient.
        maker_bps: i32,
    },
    /// The taker coefficient is negative or above the fee ceiling.
    TakerOutOfRange {
        /// Rejected taker coefficient.
        taker_bps: i32,
    },
}

impl fmt::Display for FeeModelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MakerOutOfRange { maker_bps } => write!(
                formatter,
                "maker fee {maker_bps} bps is outside {MIN_MAKER_FEE_BPS}..={MAX_FEE_BPS}"
            ),
            Self::TakerOutOfRange { taker_bps } => write!(
                formatter,
                "taker fee {taker_bps} bps is outside 0..={MAX_FEE_BPS}"
            ),
        }
    }
}

impl std::error::Error for FeeModelError {}
