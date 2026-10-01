use std::sync::Arc;
use std::time::Duration;

use crate::test_support::{NoCex, NoExec, NoHistory, NoLive, risk};
use crate::{
    BacktestRun, ConservativeV1Config, LiveRun, POLYBOLT_ORACLE_SOURCE_ID, PaperRun, ReplaySpec,
    RunSpec, SettlementOracleBinding,
};
use pmkit_core::{MarketId, PortfolioId, RunId};
use pmkit_run::{EvidenceRequirement, RetrievalWait};

fn paper_run() -> Result<PaperRun, Box<dyn std::error::Error>> {
    Ok(PaperRun::new(
        RunId::new("oracle-paper")?,
        PortfolioId::new("alice")?,
        pmkit_money::Money::usdc(10_000),
        risk()?,
        Arc::new(NoLive),
        ConservativeV1Config {
            activation_latency: Duration::from_millis(100),
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    ))
}

fn market() -> Result<MarketId, Box<dyn std::error::Error>> {
    Ok(MarketId::new("btc-5m")?)
}

fn live_run() -> Result<LiveRun, Box<dyn std::error::Error>> {
    Ok(LiveRun::new(
        RunId::new("oracle-live")?,
        PortfolioId::new("alice")?,
        Arc::new(NoExec),
        Arc::new(NoLive),
        risk()?,
    ))
}

#[test]
fn generic_run_without_oracle_requirement_is_valid() -> Result<(), Box<dyn std::error::Error>> {
    let spec = RunSpec::from(paper_run()?);
    assert!(spec.validate_settlement_oracle_bindings().is_ok());
    Ok(())
}

#[test]
fn required_oracle_binding_absent_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let run = paper_run()?.require_settlement_oracle("twap", market()?);
    let Err(error) = RunSpec::from(run).validate_settlement_oracle_bindings() else {
        return Err("required oracle mapping and source are absent".into());
    };
    assert_eq!(error.requirement().name(), "twap");
    Ok(())
}

#[test]
fn required_oracle_without_named_reference_source_is_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let market = market()?;
    let run = paper_run()?
        .settlement_oracle_named("twap", market.clone(), "BTC")
        .require_settlement_oracle("twap", market);
    assert!(
        RunSpec::from(run)
            .validate_settlement_oracle_bindings()
            .is_err()
    );
    Ok(())
}

#[test]
fn canonical_source_id_and_mismatched_named_source_are_checked()
-> Result<(), Box<dyn std::error::Error>> {
    let market = market()?;
    let binding = SettlementOracleBinding::new("twap", market.clone(), "BTC");
    assert_eq!(binding.source_id(), POLYBOLT_ORACLE_SOURCE_ID);
    assert_eq!(
        binding.source_id(),
        "polymarket:polybolt:price.crypto.twap:60"
    );

    let run = paper_run()?
        .reference_data_named("retired-rtds", Arc::new(NoCex))
        .settlement_oracle_named("twap", market.clone(), "BTC")
        .require_settlement_oracle("twap", market);
    assert!(
        RunSpec::from(run)
            .validate_settlement_oracle_bindings()
            .is_err()
    );
    Ok(())
}

#[test]
fn binding_without_asset_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let market = market()?;
    let run = paper_run()?
        .reference_data_named("twap", Arc::new(NoCex))
        .settlement_oracle_named("twap", market.clone(), "  ")
        .require_settlement_oracle("twap", market);
    assert!(
        RunSpec::from(run)
            .validate_settlement_oracle_bindings()
            .is_err()
    );
    Ok(())
}

#[test]
fn live_oracle_requires_and_accepts_a_registered_named_source()
-> Result<(), Box<dyn std::error::Error>> {
    let market = market()?;
    let missing = live_run()?
        .settlement_oracle_named("twap", market.clone(), "BTC")
        .require_settlement_oracle("twap", market.clone());
    let Err(error) = RunSpec::from(missing).validate_settlement_oracle_bindings() else {
        return Err("live oracle binding without a named source should be rejected".into());
    };
    assert_eq!(error.requirement().name(), "twap");

    let provided = live_run()?
        .reference_data_named("twap", Arc::new(NoCex))
        .settlement_oracle_named("twap", market.clone(), "BTC")
        .require_settlement_oracle("twap", market);
    if let Err(error) = RunSpec::from(provided).validate_settlement_oracle_bindings() {
        return Err(format!("registered live oracle source should validate: {error}").into());
    }
    Ok(())
}

#[test]
fn backtest_oracle_accepts_a_named_credential_free_historical_source()
-> Result<(), Box<dyn std::error::Error>> {
    let market = market()?;
    let replay = ReplaySpec::new(
        Arc::new(NoHistory),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    )
    .reference_source_named("twap", Arc::new(NoHistory));
    let run = BacktestRun::new(
        RunId::new("oracle-backtest")?,
        PortfolioId::new("alice")?,
        replay,
        pmkit_money::Money::usdc(10_000),
        risk()?,
        ConservativeV1Config {
            activation_latency: Duration::from_millis(100),
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .settlement_oracle_named("twap", market.clone(), "BTC")
    .require_settlement_oracle("twap", market);

    if let Err(error) = RunSpec::from(run).validate_settlement_oracle_bindings() {
        return Err(format!("registered historical oracle source should validate: {error}").into());
    }
    Ok(())
}
