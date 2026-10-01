use super::*;

#[tokio::test]
async fn recorded_backtest_rejects_independent_reference_history()
-> Result<(), Box<dyn std::error::Error>> {
    let replay = ReplaySpec::new(
        Arc::new(ScriptedHistory { ticks: Vec::new() }),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    )
    .reference_source_named("independent-cex", Arc::new(ScriptedReference));
    let run = BacktestRun::new(
        RunId::new("bt-recorded-reference-source")?,
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
    );
    let error = crate::RecordedBacktest::replay_capture(
        &run,
        crate::capture_replay::CaptureReplaySource::new(Cursor::new(Vec::new())),
    )
    .drive(None)
    .await
    .err()
    .ok_or("recorded backtest unexpectedly accepted an independent reference history")?;
    let StartError::Source {
        source: DataSourceError::ReplayGap { message },
        ..
    } = error
    else {
        return Err("independent reference history was not rejected as a replay gap".into());
    };
    assert!(message.contains(
        "recorded backtests cannot combine a capture with independent reference histories"
    ));
    Ok(())
}
