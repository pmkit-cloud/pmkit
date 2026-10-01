use super::*;

#[tokio::test]
async fn recorded_capture_eof_rejects_missing_required_oracle_facts()
-> Result<(), Box<dyn std::error::Error>> {
    let market = MarketId::new("btc-5m")?;
    let replay = ReplaySpec::new(
        Arc::new(ScriptedHistory { ticks: Vec::new() }),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    );
    let run = BacktestRun::new(
        RunId::new("bt-recorded-oracle-eof")?,
        PortfolioId::new("research")?,
        replay,
        Money::usdc(1_000),
        risk()?,
        ConservativeV1Config {
            activation_latency: Duration::ZERO,
            maker_queue_ahead_bps: 0,
            slippage_bps: 0,
            market_impact_bps: 0,
            fee_model: None,
            market_limits: None,
        },
    )
    .settlement_oracle_named("settlement", market.clone(), "BTC")
    .require_settlement_oracle("settlement", market.clone())
    .strategy(StrategyRegistration::new(
        StrategyId::new("recorded-oracle-eof")?,
        market,
        Arc::new(BuyFactory),
    ));

    let error = crate::RecordedBacktest::replay_capture(
        &run,
        crate::capture_replay::CaptureReplaySource::new(Cursor::new(Vec::new())),
    )
    .drive(None)
    .await
    .err()
    .ok_or("recorded capture without required oracle facts unexpectedly completed")?;
    let StartError::Source {
        source: DataSourceError::ReplayGap { message },
        ..
    } = error
    else {
        return Err("missing required oracle facts did not fail as a replay gap".into());
    };
    assert!(
        message.starts_with("capture ended without required settlement oracle facts"),
        "unexpected replay gap message: {message}"
    );
    Ok(())
}
