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

/// Schema version for run manifests.
pub const MANIFEST_SCHEMA_VERSION: u16 = 1;

const REDACTED_MANIFEST_DIR: &str = "<redacted>";

/// A fully decoded version-1 run manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestV1 {
    /// Manifest schema version.
    pub schema_version: u16,
    #[serde(flatten)]
    body: ManifestBodyV1,
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
    /// A version-1 manifest does not match the complete typed schema.
    #[error("malformed version-1 manifest: {source}")]
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
/// [`ManifestError::Malformed`] when a version-1 manifest is incomplete or has
/// a field with the wrong type.
pub fn parse_manifest(value: &Value) -> Result<ManifestV1, ManifestError> {
    let version = ManifestVersion::deserialize(value)
        .map_err(|source| ManifestError::Malformed { source })?;
    match version.schema_version {
        MANIFEST_SCHEMA_VERSION => {
            ManifestV1::deserialize(value).map_err(|source| ManifestError::Malformed { source })
        }
        found => Err(ManifestError::UnsupportedSchemaVersion { found }),
    }
}

/// Builds a redacted reproducibility manifest for `run` under `config`.
#[must_use]
pub fn build_manifest(run: &RunSpec, config: &RuntimeConfig) -> Value {
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
            "runtime": runtime,
        }),
        RunSpec::Live(live) => json!({
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "mode": "live",
            "run": live.id().to_string(),
            "portfolio": live.portfolio().to_string(),
            "risk": risk_json(live.risk()),
            "strategies": strategies_json(live.strategies()),
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
mod tests {
    use super::{ManifestError, build_manifest, parse_manifest};
    use async_trait::async_trait;
    use pmkit_core::{MarketId, PortfolioId, RunId, StrategyId};
    use pmkit_data::{DataSourceError, HistoricalDataSource, ReplayQuery, SourceSignal};
    use pmkit_money::Money;
    use pmkit_run::{EvidenceRequirement, RetrievalWait};
    use pmkit_runtime::{RiskLimits, RuntimeConfig, ShutdownConfig, StrategyRegistration};
    use pmkit_spec::{BacktestRun, ConservativeV1Config, ReplaySpec};
    use pmkit_strategy::{
        Actions, Strategy, StrategyContext, StrategyError, StrategyFactory, StrategyInitError,
    };
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc::Sender;

    struct NoHistory;

    #[async_trait]
    impl HistoricalDataSource for NoHistory {
        async fn replay(
            &self,
            _query: ReplayQuery,
            _sink: Sender<SourceSignal>,
        ) -> Result<(), DataSourceError> {
            Ok(())
        }
    }

    struct FlatStrategy;

    impl Strategy for FlatStrategy {
        fn on_event(&mut self, _ctx: StrategyContext<'_>) -> Result<Actions, StrategyError> {
            Ok(Actions::none())
        }
    }

    struct FlatFactory;

    impl StrategyFactory for FlatFactory {
        fn create(&self) -> Result<Box<dyn Strategy>, StrategyInitError> {
            Ok(Box::new(FlatStrategy))
        }
    }

    fn test_manifest() -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let config = RuntimeConfig {
            backtest_concurrency: NonZeroUsize::new(4).ok_or("nonzero")?,
            startup_timeout: Duration::from_secs(30),
            shutdown: ShutdownConfig {
                live_orders: pmkit_runtime::LiveOrderPolicy::CancelOwned,
                reconciliation_timeout: Duration::from_secs(30),
                tape_flush_timeout: Duration::from_secs(10),
            },
            manifest_dir: std::env::current_dir()?.join("private").join("runs"),
        };
        let replay = ReplaySpec::new(
            Arc::new(NoHistory),
            "2026-01-01T00:00:00Z".parse()?,
            "2026-02-01T00:00:00Z".parse()?,
            EvidenceRequirement::CorroboratedOnly,
            RetrievalWait::ReturnPending,
        );
        let run = BacktestRun::new(
            RunId::new("research")?,
            PortfolioId::new("research")?,
            replay,
            Money::usdc(100_000),
            RiskLimits {
                max_order_notional: Money::usdc(100),
                max_position_notional: Money::usdc(1_000),
                max_portfolio_notional: Money::usdc(5_000),
                max_market_notional: Money::usdc(2_000),
                max_strategy_notional: Money::usdc(1_000),
                max_open_orders: NonZeroU32::new(10).ok_or("nonzero")?,
                max_loss: Money::usdc(500),
                max_daily_loss: Money::usdc(500),
            },
            ConservativeV1Config {
                activation_latency: Duration::ZERO,
                maker_queue_ahead_bps: 0,
                slippage_bps: 0,
                market_impact_bps: 0,
            },
        )
        .strategy(StrategyRegistration::new(
            StrategyId::new("maker")?,
            MarketId::new("btc-5m")?,
            Arc::new(FlatFactory),
        ));

        Ok(build_manifest(&run.into(), &config))
    }

    #[test]
    fn backtest_manifest_captures_topology() -> Result<(), Box<dyn std::error::Error>> {
        let manifest = test_manifest()?;
        assert_eq!(manifest["mode"], "backtest");
        assert_eq!(manifest["run"], "research");
        assert_eq!(manifest["initial_cash"], "100000");
        assert_eq!(manifest["risk"]["max_open_orders"], 10);
        assert_eq!(manifest["strategies"][0]["id"], "maker");
        assert!(manifest["strategies"][0]["name"].is_null());
        assert_eq!(manifest["replay"]["evidence"], "corroborated_only");
        Ok(())
    }

    #[test]
    fn manifest_v1_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        // Given
        let manifest = test_manifest()?;

        // When
        let parsed = parse_manifest(&manifest)?;

        // Then
        assert_eq!(manifest["schema_version"], 1);
        assert_eq!(parsed.schema_version, 1);
        assert_eq!(manifest["runtime"]["manifest_dir"], "<redacted>");
        Ok(())
    }

    #[test]
    fn manifest_rejects_unsupported_and_malformed() -> Result<(), Box<dyn std::error::Error>> {
        // Given
        let mut unsupported = test_manifest()?;
        unsupported["schema_version"] = serde_json::json!(2);
        let mut malformed = test_manifest()?;
        malformed["run"] = serde_json::json!(false);

        // When / Then
        assert!(matches!(
            parse_manifest(&unsupported),
            Err(ManifestError::UnsupportedSchemaVersion { found: 2 })
        ));
        assert!(matches!(
            parse_manifest(&malformed),
            Err(ManifestError::Malformed { .. })
        ));
        Ok(())
    }
}
