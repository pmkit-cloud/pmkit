//! Market-data domain primitives for crypto up/down prediction markets.
//!
//! These are exchange-neutral reference types shared by feeds, replay, and
//! backtests. They carry no credentials, network, or venue-signing coupling.
//! Venue-specific concepts (order signing, slugs, condition IDs) live in
//! adapters, never here.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Tradeable crypto asset for up/down prediction markets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Asset {
    /// Bitcoin.
    Btc,
    /// Ether.
    Eth,
    /// Solana.
    Sol,
    /// XRP.
    Xrp,
}

impl Asset {
    /// Returns the long slug name used by prediction-market metadata APIs
    /// (for example `"bitcoin"`).
    #[must_use]
    pub const fn full_name(self) -> &'static str {
        match self {
            Self::Btc => "bitcoin",
            Self::Eth => "ethereum",
            Self::Sol => "solana",
            Self::Xrp => "xrp",
        }
    }

    /// Returns the Binance spot symbol (for example `"btcusdt"`).
    #[must_use]
    pub const fn binance_symbol(self) -> &'static str {
        match self {
            Self::Btc => "btcusdt",
            Self::Eth => "ethusdt",
            Self::Sol => "solusdt",
            Self::Xrp => "xrpusdt",
        }
    }

    /// Returns the Bybit spot symbol (for example `"BTCUSDT"`).
    #[must_use]
    pub const fn bybit_symbol(self) -> &'static str {
        match self {
            Self::Btc => "BTCUSDT",
            Self::Eth => "ETHUSDT",
            Self::Sol => "SOLUSDT",
            Self::Xrp => "XRPUSDT",
        }
    }

    /// Returns the OKX instrument id (for example `"BTC-USDT"`).
    #[must_use]
    pub const fn okx_inst_id(self) -> &'static str {
        match self {
            Self::Btc => "BTC-USDT",
            Self::Eth => "ETH-USDT",
            Self::Sol => "SOL-USDT",
            Self::Xrp => "XRP-USDT",
        }
    }

    /// Returns the Coinbase product id (for example `"BTC-USD"`).
    #[must_use]
    pub const fn coinbase_product_id(self) -> &'static str {
        match self {
            Self::Btc => "BTC-USD",
            Self::Eth => "ETH-USD",
            Self::Sol => "SOL-USD",
            Self::Xrp => "XRP-USD",
        }
    }

    /// Returns the Kraken pair (for example `"BTC/USD"`).
    #[must_use]
    pub const fn kraken_pair(self) -> &'static str {
        match self {
            Self::Btc => "BTC/USD",
            Self::Eth => "ETH/USD",
            Self::Sol => "SOL/USD",
            Self::Xrp => "XRP/USD",
        }
    }
}

impl fmt::Display for Asset {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Btc => formatter.write_str("btc"),
            Self::Eth => formatter.write_str("eth"),
            Self::Sol => formatter.write_str("sol"),
            Self::Xrp => formatter.write_str("xrp"),
        }
    }
}

impl FromStr for Asset {
    type Err = ParseAssetError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "btc" | "bitcoin" => Ok(Self::Btc),
            "eth" | "ethereum" => Ok(Self::Eth),
            "sol" | "solana" => Ok(Self::Sol),
            "xrp" => Ok(Self::Xrp),
            _ => Err(ParseAssetError),
        }
    }
}

/// Returned when a string does not name a known [`Asset`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseAssetError;

impl fmt::Display for ParseAssetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("unknown asset")
    }
}

impl std::error::Error for ParseAssetError {}

/// Binary market outcome direction (a prediction-market side, not buy/sell).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Outcome {
    /// The market resolves up.
    Up,
    /// The market resolves down.
    Down,
}

impl Outcome {
    /// Returns the opposite outcome.
    #[must_use]
    pub const fn opposite(self) -> Self {
        match self {
            Self::Up => Self::Down,
            Self::Down => Self::Up,
        }
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Up => formatter.write_str("UP"),
            Self::Down => formatter.write_str("DOWN"),
        }
    }
}

/// Market window duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketDuration {
    /// A five-minute window.
    #[serde(rename = "5m")]
    FiveMin,
    /// A fifteen-minute window.
    #[serde(rename = "15m")]
    FifteenMin,
}

impl MarketDuration {
    /// Returns the window length in seconds.
    #[must_use]
    pub const fn seconds(self) -> i64 {
        match self {
            Self::FiveMin => 300,
            Self::FifteenMin => 900,
        }
    }

    /// Returns the window length in milliseconds.
    #[must_use]
    pub const fn millis(self) -> i64 {
        self.seconds() * 1000
    }
}

impl fmt::Display for MarketDuration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FiveMin => formatter.write_str("5m"),
            Self::FifteenMin => formatter.write_str("15m"),
        }
    }
}

impl FromStr for MarketDuration {
    type Err = ParseDurationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "5m" | "5min" => Ok(Self::FiveMin),
            "15m" | "15min" => Ok(Self::FifteenMin),
            _ => Err(ParseDurationError),
        }
    }
}

/// Returned when a string does not name a known [`MarketDuration`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseDurationError;

impl fmt::Display for ParseDurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("unknown market duration")
    }
}

impl std::error::Error for ParseDurationError {}

/// Reference price source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Exchange {
    /// Binance.
    Binance,
    /// Chainlink oracle.
    Chainlink,
    /// Vatic oracle.
    Vatic,
    /// Bybit.
    Bybit,
    /// Coinbase.
    Coinbase,
    /// OKX.
    Okx,
    /// Kraken.
    Kraken,
}

impl fmt::Display for Exchange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Binance => "Binance",
            Self::Chainlink => "Chainlink",
            Self::Vatic => "Vatic",
            Self::Bybit => "Bybit",
            Self::Coinbase => "Coinbase",
            Self::Okx => "Okx",
            Self::Kraken => "Kraken",
        };
        formatter.write_str(name)
    }
}

#[cfg(test)]
mod tests {
    use super::{Asset, Exchange, MarketDuration, Outcome, ParseAssetError};

    #[test]
    fn asset_parses_symbol_and_full_name() {
        assert_eq!("bitcoin".parse::<Asset>(), Ok(Asset::Btc));
        assert_eq!("BTC".parse::<Asset>(), Ok(Asset::Btc));
        assert_eq!(Asset::Eth.full_name(), "ethereum");
        assert_eq!(Asset::Sol.binance_symbol(), "solusdt");
        assert_eq!("doge".parse::<Asset>(), Err(ParseAssetError));
    }

    #[test]
    fn outcome_opposite_is_involutive() {
        assert_eq!(Outcome::Up.opposite(), Outcome::Down);
        assert_eq!(Outcome::Up.opposite().opposite(), Outcome::Up);
    }

    #[test]
    fn duration_reports_length_and_parses() {
        assert_eq!(MarketDuration::FiveMin.seconds(), 300);
        assert_eq!(MarketDuration::FifteenMin.millis(), 900_000);
        assert_eq!(
            "15min".parse::<MarketDuration>(),
            Ok(MarketDuration::FifteenMin)
        );
    }

    #[test]
    fn enums_round_trip_through_json() -> Result<(), serde_json::Error> {
        assert_eq!(serde_json::to_string(&Asset::Xrp)?, "\"xrp\"");
        assert_eq!(serde_json::from_str::<Asset>("\"xrp\"")?, Asset::Xrp);
        assert_eq!(serde_json::to_string(&MarketDuration::FiveMin)?, "\"5m\"");
        assert_eq!(serde_json::to_string(&Exchange::Okx)?, "\"okx\"");
        Ok(())
    }
}
