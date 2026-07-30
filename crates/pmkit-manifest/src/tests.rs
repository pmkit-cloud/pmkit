use super::{
    GitProvenance, ManifestError, Provenance, VersionedManifest, build_manifest_with_provenance,
    parse_manifest,
};
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

fn test_provenance() -> Provenance {
    Provenance {
        git: GitProvenance {
            commit: "0123456789abcdef".to_owned(),
            dirty: true,
        },
        cargo_lock_sha256: "a".repeat(64),
        toolchain: "rustc test-toolchain".to_owned(),
    }
}

fn test_manifest(provenance: &Provenance) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
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
            fee_model: Some(pmkit_spec::FeeModel::try_new(-100, 200)?),
            min_order_size: None,
            tick_size: None,
        },
    )
    .strategy(StrategyRegistration::new(
        StrategyId::new("maker")?,
        MarketId::new("btc-5m")?,
        Arc::new(FlatFactory),
    ));

    Ok(build_manifest_with_provenance(
        &run.into(),
        &config,
        provenance,
    ))
}

#[test]
fn backtest_manifest_captures_topology() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = test_manifest(&test_provenance())?;
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
    let mut manifest = test_manifest(&test_provenance())?;
    manifest["schema_version"] = serde_json::json!(1);
    let _provenance = manifest
        .as_object_mut()
        .ok_or("manifest must be an object")?
        .remove("provenance");
    let _fee_model = manifest["simulation"]
        .as_object_mut()
        .ok_or("simulation must be an object")?
        .remove("fee_model");

    // When
    let parsed = parse_manifest(&manifest)?;

    // Then
    assert_eq!(manifest["schema_version"], 1);
    let VersionedManifest::V1(parsed) = parsed else {
        return Err("expected version-1 manifest".into());
    };
    assert_eq!(parsed.schema_version, 1);
    assert_eq!(manifest["runtime"]["manifest_dir"], "<redacted>");
    Ok(())
}

#[test]
fn manifest_v2_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    // Given
    let mut manifest = test_manifest(&test_provenance())?;
    manifest["schema_version"] = serde_json::json!(2);
    let _fee_model = manifest["simulation"]
        .as_object_mut()
        .ok_or("simulation must be an object")?
        .remove("fee_model");

    // When
    let parsed = parse_manifest(&manifest)?;

    // Then
    let VersionedManifest::V2(parsed) = parsed else {
        return Err("expected version-2 manifest".into());
    };
    assert_eq!(parsed.schema_version, 2);
    assert_eq!(parsed.provenance, test_provenance());
    Ok(())
}

#[test]
fn manifest_v3_round_trip_records_fee_model() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a manifest built from a custom validated simulation fee model.
    let manifest = test_manifest(&test_provenance())?;

    // When: the current manifest is decoded through the versioned reader.
    let parsed = parse_manifest(&manifest)?;

    // Then: schema v3 records maker rebates and taker fees exactly.
    let VersionedManifest::V3(parsed) = parsed else {
        return Err("expected version-3 manifest".into());
    };
    assert_eq!(parsed.schema_version, 3);
    assert_eq!(manifest["simulation"]["fee_model"]["maker_bps"], -100);
    assert_eq!(manifest["simulation"]["fee_model"]["taker_bps"], 200);
    Ok(())
}

#[test]
fn provenance_captured() -> Result<(), Box<dyn std::error::Error>> {
    // Given
    let provenance = test_provenance();

    // When
    let manifest = test_manifest(&provenance)?;

    // Then
    let lock_hash = manifest["provenance"]["cargo_lock_sha256"]
        .as_str()
        .ok_or("Cargo.lock hash must be a string")?;
    assert_eq!(manifest["schema_version"], 3);
    assert_eq!(manifest["provenance"]["git"]["commit"], "0123456789abcdef");
    assert_eq!(manifest["provenance"]["git"]["dirty"], true);
    assert_eq!(lock_hash.len(), 64);
    assert!(lock_hash.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(manifest["provenance"]["toolchain"], "rustc test-toolchain");
    Ok(())
}

#[test]
fn provenance_absent_git_is_deterministic() -> Result<(), Box<dyn std::error::Error>> {
    // Given
    let provenance = Provenance {
        git: GitProvenance {
            commit: "unknown".to_owned(),
            dirty: false,
        },
        cargo_lock_sha256: "b".repeat(64),
        toolchain: "rustc test-toolchain".to_owned(),
    };

    // When
    let manifest = test_manifest(&provenance)?;

    // Then
    assert_eq!(manifest["provenance"]["git"]["commit"], "unknown");
    assert_eq!(manifest["provenance"]["git"]["dirty"], false);
    Ok(())
}

#[test]
fn manifest_rejects_unsupported_and_malformed() -> Result<(), Box<dyn std::error::Error>> {
    // Given
    let mut unsupported = test_manifest(&test_provenance())?;
    unsupported["schema_version"] = serde_json::json!(4);
    let mut malformed = test_manifest(&test_provenance())?;
    malformed["run"] = serde_json::json!(false);

    // When / Then
    assert!(matches!(
        parse_manifest(&unsupported),
        Err(ManifestError::UnsupportedSchemaVersion { found: 4 })
    ));
    assert!(matches!(
        parse_manifest(&malformed),
        Err(ManifestError::Malformed { .. })
    ));
    Ok(())
}
