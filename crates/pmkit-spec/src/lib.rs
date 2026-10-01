//! Run specifications for `PMKit`.
//!
//! A run is one portfolio in one mode with one or more strategy registrations.
//! [`BacktestRun`], [`PaperRun`], and [`LiveRun`] are the user-facing recipes;
//! [`RunSpec`] is the tagged union the runtime consumes.

use std::fmt;

use pmkit_core::MarketId;

mod backtest;
mod config;
mod live;
mod paper;
mod replay;
mod run_spec;

#[cfg(test)]
mod oracle_tests;
#[cfg(test)]
mod test_support;

pub use backtest::BacktestRun;
pub use config::ConservativeV1Config;
pub use live::LiveRun;
pub use paper::PaperRun;
pub use pmkit_sim::FeeModel;
pub use replay::ReplaySpec;
pub use run_spec::RunSpec;

/// Stable identity used by the `PolyBolt` 60-second TWAP source.
///
/// This is distinct from the run-local binding name and from the retired RTDS
/// source identity, so new and historical tapes cannot be mistaken for one another.
pub const POLYBOLT_ORACLE_SOURCE_ID: &str = "polymarket:polybolt:price.crypto.twap:60";

/// A market-to-asset mapping for one named settlement-oracle source binding.
///
/// Source credentials remain owned by the registered adapter and are not part of a run spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementOracleBinding {
    name: String,
    market: MarketId,
    asset: String,
}

impl SettlementOracleBinding {
    /// Creates a named `PolyBolt` oracle binding for one market and asset symbol.
    #[must_use]
    pub fn new(name: impl Into<String>, market: MarketId, asset: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            market,
            asset: asset.into(),
        }
    }

    /// Returns the explicit run-local source binding name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the stable source identity stamped on `PolyBolt` oracle facts.
    #[must_use]
    pub const fn source_id(&self) -> &'static str {
        POLYBOLT_ORACLE_SOURCE_ID
    }

    /// Returns the PM market mapped to the oracle asset.
    #[must_use]
    pub const fn market(&self) -> &MarketId {
        &self.market
    }

    /// Returns the asset symbol parsed by the runtime as `pmkit_market::Asset`.
    #[must_use]
    pub fn asset(&self) -> &str {
        &self.asset
    }
}

/// A strategy's explicit requirement for a named settlement oracle binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementOracleRequirement {
    name: String,
    market: MarketId,
}

impl SettlementOracleRequirement {
    /// Requires named oracle binding `name` for `market`.
    #[must_use]
    pub fn new(name: impl Into<String>, market: MarketId) -> Self {
        Self {
            name: name.into(),
            market,
        }
    }

    /// Returns the required source binding name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the market requiring settlement reference data.
    #[must_use]
    pub const fn market(&self) -> &MarketId {
        &self.market
    }
}

/// A required settlement-oracle binding is absent from a run specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingSettlementOracleBinding {
    requirement: SettlementOracleRequirement,
}

impl MissingSettlementOracleBinding {
    const fn new(requirement: SettlementOracleRequirement) -> Self {
        Self { requirement }
    }

    /// Returns the missing named binding requirement.
    #[must_use]
    pub const fn requirement(&self) -> &SettlementOracleRequirement {
        &self.requirement
    }
}

impl fmt::Display for MissingSettlementOracleBinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "missing settlement oracle binding `{}` for market `{}`",
            self.requirement.name, self.requirement.market
        )
    }
}

impl std::error::Error for MissingSettlementOracleBinding {}

impl RunSpec {
    /// Returns this run's declared market-to-oracle mappings.
    #[must_use]
    pub fn settlement_oracle_bindings(&self) -> &[SettlementOracleBinding] {
        match self {
            Self::Backtest(run) => run.settlement_oracle_bindings(),
            Self::Paper(run) => run.settlement_oracle_bindings(),
            Self::Live(run) => run.settlement_oracle_bindings(),
        }
    }

    /// Returns settlement-oracle requirements declared by this run's strategies.
    #[must_use]
    pub fn settlement_oracle_requirements(&self) -> &[SettlementOracleRequirement] {
        match self {
            Self::Backtest(run) => run.settlement_oracle_requirements(),
            Self::Paper(run) => run.settlement_oracle_requirements(),
            Self::Live(run) => run.settlement_oracle_requirements(),
        }
    }

    /// Rejects required source names without a matching mapping and source binding.
    ///
    /// Generic runs with no settlement-oracle requirements pass without any oracle
    /// source configured.
    ///
    /// # Errors
    ///
    /// Returns [`MissingSettlementOracleBinding`] if a required market mapping or
    /// named runtime source is absent.
    pub fn validate_settlement_oracle_bindings(
        &self,
    ) -> Result<(), MissingSettlementOracleBinding> {
        for requirement in self.settlement_oracle_requirements() {
            let configured = self.settlement_oracle_bindings().iter().any(|binding| {
                binding.name == requirement.name
                    && binding.market == requirement.market
                    && !binding.name.trim().is_empty()
                    && !binding.asset.trim().is_empty()
            });
            let source_bound = match self {
                Self::Backtest(run) => run
                    .replay()
                    .reference_source_refs()
                    .iter()
                    .any(|(name, _)| name == &requirement.name),
                Self::Paper(run) => run
                    .reference_data_refs()
                    .iter()
                    .any(|(name, _)| name == &requirement.name),
                Self::Live(run) => run
                    .reference_data_refs()
                    .iter()
                    .any(|(name, _)| name == &requirement.name),
            };
            if !configured || !source_bound {
                return Err(MissingSettlementOracleBinding::new(requirement.clone()));
            }
        }
        Ok(())
    }
}
