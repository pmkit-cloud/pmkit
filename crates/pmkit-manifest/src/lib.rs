//! Reproducible run manifests for `PMKit`.
//!
//! [`build_manifest`] captures a run's topology — ids, mode, risk limits,
//! strategy registrations, and the replay window — as a redacted JSON value.
//! It reads only public run configuration and never touches credentials.

use pmkit_run::EvidenceRequirement;
use pmkit_runtime::{RiskLimits, RuntimeConfig, StrategyRegistration};
use pmkit_spec::RunSpec;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

mod compiled_provenance {
    include!(concat!(env!("OUT_DIR"), "/provenance.rs"));
}

/// Schema version for run manifests.
pub const MANIFEST_SCHEMA_VERSION: u16 = 2;

const MANIFEST_SCHEMA_VERSION_V1: u16 = 1;
const REDACTED_MANIFEST_DIR: &str = "<redacted>";

/// Git state captured when the manifest crate was compiled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitProvenance {
    /// Commit checked out at compile time, or `"unknown"` when git was unavailable.
    pub commit: String,
    /// Whether git reported local changes at compile time.
    pub dirty: bool,
}

/// Reproducibility inputs embedded in a compiled manifest builder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    /// Git commit and dirty state.
    pub git: GitProvenance,
    /// SHA-256 of the workspace-root `Cargo.lock` at compile time.
    pub cargo_lock_sha256: String,
    /// Rust compiler identity used to compile the crate.
    pub toolchain: String,
}

impl Provenance {
    /// Returns the provenance embedded by this crate's build script.
    #[must_use]
    pub fn current() -> Self {
        Self {
            git: GitProvenance {
                commit: compiled_provenance::GIT_COMMIT.to_owned(),
                dirty: compiled_provenance::GIT_DIRTY,
            },
            cargo_lock_sha256: compiled_provenance::CARGO_LOCK_SHA256.to_owned(),
            toolchain: compiled_provenance::TOOLCHAIN.to_owned(),
        }
    }
}

/// A fully decoded version-1 run manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestV1 {
    /// Manifest schema version.
    pub schema_version: u16,
    #[serde(flatten)]
    body: ManifestBodyV1,
}

/// A fully decoded version-2 run manifest.
///
/// Version 2 migrates version 1 by adding [`Provenance`]; all version-1 run
/// fields retain their original representation, and readers accept both.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestV2 {
    /// Manifest schema version.
    pub schema_version: u16,
    /// Compile-time reproducibility provenance.
    pub provenance: Provenance,
    #[serde(flatten)]
    body: ManifestBodyV1,
}

/// A decoded manifest from any supported schema version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionedManifest {
    /// A version-1 manifest without compile-time provenance.
    V1(ManifestV1),
    /// A version-2 manifest with compile-time provenance.
    V2(ManifestV2),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
enum ManifestBodyV1 {
    Backtest {
        #[serde(flatten)]
        common: ManifestCommonV1,
        initial_cash: String,
        simulation: SimulationV1,
        replay: ReplayV1,
    },
    Paper {
        #[serde(flatten)]
        common: ManifestCommonV1,
        initial_cash: String,
        simulation: SimulationV1,
    },
    Live {
        #[serde(flatten)]
        common: ManifestCommonV1,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ManifestCommonV1 {
    run: String,
    portfolio: String,
    risk: RiskV1,
    strategies: Vec<StrategyV1>,
    runtime: RuntimeV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RiskV1 {
    max_order_notional: String,
    max_position_notional: String,
    max_portfolio_notional: String,
    max_market_notional: String,
    max_strategy_notional: String,
    #[serde(rename = "max_open_orders")]
    open_orders_limit: u32,
    max_loss: String,
    max_daily_loss: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SimulationV1 {
    activation_latency_ms: serde_json::Number,
    maker_queue_ahead_bps: u16,
    slippage_bps: u16,
    market_impact_bps: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StrategyV1 {
    id: String,
    market: String,
    name: Option<String>,
    version: Option<String>,
    config_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReplayV1 {
    from: String,
    to: String,
    evidence: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RuntimeV1 {
    backtest_concurrency: usize,
    manifest_dir: String,
}

#[derive(Debug, Deserialize)]
struct ManifestVersion {
    schema_version: u16,
}

/// Failure while decoding a run manifest.
#[derive(Debug, Error)]
pub enum ManifestError {
    /// The manifest uses a schema version this reader does not support.
    #[error("unsupported manifest schema version {found}")]
    UnsupportedSchemaVersion {
        /// Version found in the manifest.
        found: u16,
    },
    /// A supported manifest does not match its complete typed schema.
    #[error("malformed manifest: {source}")]
    Malformed {
        /// JSON decoding failure.
        #[source]
        source: serde_json::Error,
    },
}

/// Decodes and validates a versioned run manifest.
///
/// # Errors
///
/// Returns [`ManifestError::UnsupportedSchemaVersion`] for unknown versions and
/// [`ManifestError::Malformed`] when a supported manifest is incomplete or has
/// a field with the wrong type.
pub fn parse_manifest(value: &Value) -> Result<VersionedManifest, ManifestError> {
    let version = ManifestVersion::deserialize(value)
        .map_err(|source| ManifestError::Malformed { source })?;
    match version.schema_version {
        MANIFEST_SCHEMA_VERSION_V1 => ManifestV1::deserialize(value)
            .map(VersionedManifest::V1)
            .map_err(|source| ManifestError::Malformed { source }),
        MANIFEST_SCHEMA_VERSION => ManifestV2::deserialize(value)
            .map(VersionedManifest::V2)
            .map_err(|source| ManifestError::Malformed { source }),
        found => Err(ManifestError::UnsupportedSchemaVersion { found }),
    }
}

/// Builds a redacted reproducibility manifest for `run` under `config`.
#[must_use]
pub fn build_manifest(run: &RunSpec, config: &RuntimeConfig) -> Value {
    build_manifest_with_provenance(run, config, &Provenance::current())
}

/// Builds a manifest with explicitly supplied compile-time provenance.
///
/// This injection seam keeps tests deterministic; production callers should
/// use [`build_manifest`] so the embedded build values are selected.
#[must_use]
pub fn build_manifest_with_provenance(
    run: &RunSpec,
    config: &RuntimeConfig,
    provenance: &Provenance,
) -> Value {
    let runtime = json!({
        "backtest_concurrency": config.backtest_concurrency.get(),
        "manifest_dir": REDACTED_MANIFEST_DIR,
    });
    match run {
        RunSpec::Backtest(backtest) => json!({
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "mode": "backtest",
            "run": backtest.id().to_string(),
            "portfolio": backtest.portfolio().to_string(),
            "initial_cash": backtest.initial_cash().as_decimal().to_string(),
            "risk": risk_json(backtest.risk()),
            "simulation": simulation_json(backtest.simulation()),
            "strategies": strategies_json(backtest.strategies()),
            "provenance": provenance,
            "replay": {
                "from": backtest.replay().from().to_rfc3339(),
                "to": backtest.replay().to().to_rfc3339(),
                "evidence": evidence_str(backtest.replay().evidence()),
            },
            "runtime": runtime,
        }),
        RunSpec::Paper(paper) => json!({
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "mode": "paper",
            "run": paper.id().to_string(),
            "portfolio": paper.portfolio().to_string(),
            "initial_cash": paper.initial_cash().as_decimal().to_string(),
            "risk": risk_json(paper.risk()),
            "simulation": simulation_json(paper.simulation()),
            "strategies": strategies_json(paper.strategies()),
            "provenance": provenance,
            "runtime": runtime,
        }),
        RunSpec::Live(live) => json!({
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "mode": "live",
            "run": live.id().to_string(),
            "portfolio": live.portfolio().to_string(),
            "risk": risk_json(live.risk()),
            "strategies": strategies_json(live.strategies()),
            "provenance": provenance,
            "runtime": runtime,
        }),
    }
}

fn simulation_json(config: &pmkit_spec::ConservativeV1Config) -> Value {
    json!({
        "activation_latency_ms": config.activation_latency.as_millis(),
        "maker_queue_ahead_bps": config.maker_queue_ahead_bps,
        "slippage_bps": config.slippage_bps,
        "market_impact_bps": config.market_impact_bps,
    })
}

fn risk_json(risk: &RiskLimits) -> Value {
    json!({
        "max_order_notional": risk.max_order_notional.as_decimal().to_string(),
        "max_position_notional": risk.max_position_notional.as_decimal().to_string(),
        "max_portfolio_notional": risk.max_portfolio_notional.as_decimal().to_string(),
        "max_market_notional": risk.max_market_notional.as_decimal().to_string(),
        "max_strategy_notional": risk.max_strategy_notional.as_decimal().to_string(),
        "max_open_orders": risk.max_open_orders.get(),
        "max_loss": risk.max_loss.as_decimal().to_string(),
        "max_daily_loss": risk.max_daily_loss.as_decimal().to_string(),
    })
}

fn strategies_json(registrations: &[StrategyRegistration]) -> Value {
    let entries: Vec<Value> = registrations
        .iter()
        .map(|registration| {
            json!({
                "id": registration.id().to_string(),
                "market": registration.market().to_string(),
                "name": registration.name_ref(),
                "version": registration.version_ref(),
                "config_revision": registration.config_revision_ref(),
            })
        })
        .collect();
    Value::Array(entries)
}

const fn evidence_str(evidence: EvidenceRequirement) -> &'static str {
    match evidence {
        EvidenceRequirement::CorroboratedOnly => "corroborated_only",
        EvidenceRequirement::AllowSingleSource => "allow_single_source",
    }
}

#[cfg(test)]
mod tests;
