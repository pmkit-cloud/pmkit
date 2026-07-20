//! Pure ownership value types for `PMKit`.

use std::{error::Error, fmt};

/// Identifies which ownership ID was empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyIdError {
    /// A portfolio ID was empty.
    Portfolio,
    /// A market ID was empty.
    Market,
    /// A strategy ID was empty.
    Strategy,
    /// A run ID was empty.
    Run,
}

impl fmt::Display for EmptyIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Portfolio => formatter.write_str("portfolio ID cannot be empty"),
            Self::Market => formatter.write_str("market ID cannot be empty"),
            Self::Strategy => formatter.write_str("strategy ID cannot be empty"),
            Self::Run => formatter.write_str("run ID cannot be empty"),
        }
    }
}

impl Error for EmptyIdError {}

macro_rules! id_type {
    ($name:ident, $variant:ident, $struct_doc:literal, $new_doc:literal) => {
        #[doc = $struct_doc]
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub struct $name(String);

        impl $name {
            #[doc = $new_doc]
            pub fn new(value: impl Into<String>) -> Result<Self, EmptyIdError> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err(EmptyIdError::$variant);
                }
                Ok(Self(value))
            }
        }
    };
}

id_type!(
    PortfolioId,
    Portfolio,
    "A non-empty portfolio identifier.",
    "Creates a portfolio identifier.\n\n# Errors\n\nReturns [`EmptyIdError::Portfolio`] when `value` is empty or whitespace-only."
);

id_type!(
    MarketId,
    Market,
    "A non-empty exact market identifier.",
    "Creates an exact market identifier.\n\n# Errors\n\nReturns [`EmptyIdError::Market`] when `value` is empty or whitespace-only."
);

id_type!(
    StrategyId,
    Strategy,
    "A non-empty strategy identifier.",
    "Creates a strategy identifier.\n\n# Errors\n\nReturns [`EmptyIdError::Strategy`] when `value` is empty or whitespace-only."
);

id_type!(
    RunId,
    Run,
    "A non-empty run identifier.",
    "Creates a run identifier.\n\n# Errors\n\nReturns [`EmptyIdError::Run`] when `value` is empty or whitespace-only."
);

/// Selects an isolated portfolio execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Mode {
    /// Deterministic historical execution.
    Backtest,
    /// Simulated execution over live data.
    Paper,
    /// Real-order execution.
    Live,
}

/// Owns balances, positions, orders, risk, kill state, reconciliation, and executor state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PortfolioKey {
    /// The user-defined portfolio identity.
    pub portfolio_id: PortfolioId,
    /// The isolated execution mode.
    pub mode: Mode,
}

impl PortfolioKey {
    /// Creates a key from a validated portfolio ID and mode.
    #[must_use]
    pub const fn new(portfolio_id: PortfolioId, mode: Mode) -> Self {
        Self { portfolio_id, mode }
    }

    /// Creates a backtest portfolio key.
    ///
    /// # Errors
    ///
    /// Returns [`EmptyIdError::Portfolio`] when `portfolio_id` is empty or whitespace-only.
    pub fn backtest(portfolio_id: impl Into<String>) -> Result<Self, EmptyIdError> {
        PortfolioId::new(portfolio_id).map(|id| Self::new(id, Mode::Backtest))
    }

    /// Creates a paper portfolio key.
    ///
    /// # Errors
    ///
    /// Returns [`EmptyIdError::Portfolio`] when `portfolio_id` is empty or whitespace-only.
    pub fn paper(portfolio_id: impl Into<String>) -> Result<Self, EmptyIdError> {
        PortfolioId::new(portfolio_id).map(|id| Self::new(id, Mode::Paper))
    }

    /// Creates a live portfolio key.
    ///
    /// # Errors
    ///
    /// Returns [`EmptyIdError::Portfolio`] when `portfolio_id` is empty or whitespace-only.
    pub fn live(portfolio_id: impl Into<String>) -> Result<Self, EmptyIdError> {
        PortfolioId::new(portfolio_id).map(|id| Self::new(id, Mode::Live))
    }
}

/// Owns one mutable strategy instance for one portfolio and exact market.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StrategyKey {
    /// The owning portfolio and mode.
    pub portfolio: PortfolioKey,
    /// The exact market identity.
    pub market: MarketId,
    /// The strategy identity within the market.
    pub strategy: StrategyId,
}

impl StrategyKey {
    /// Creates a strategy ownership key.
    #[must_use]
    pub const fn new(portfolio: PortfolioKey, market: MarketId, strategy: StrategyId) -> Self {
        Self {
            portfolio,
            market,
            strategy,
        }
    }
}
