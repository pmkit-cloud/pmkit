use super::*;

const ACCOUNT_SOURCE_ID: &str = "synthetic-account-source";
const ACCOUNT_EVENT_TIME_MS: i64 = 1_700_000_000_123;
const ACCOUNT_RECEIPT_TIME_MS: i64 = ACCOUNT_EVENT_TIME_MS + 25;
const ACCOUNT_RAW_FRAME: &[u8] = br#"{"event_type":"order","id":"synthetic-order-ack","market":"0x0101010101010101010101010101010101010101010101010101010101010101","asset_id":"1","side":"BUY","price":"0.5","type":"PLACEMENT","timestamp":"1700000000123"}"#;

#[tokio::test]
async fn recorded_pm_account_capture_is_rejected_before_strategy_evaluation()
-> Result<(), Box<dyn std::error::Error>> {
    let portfolio = PortfolioId::new("research")?;
    let metadata = pmkit_event::StreamMetadata {
        schema_version: 4,
        source_id: ACCOUNT_SOURCE_ID.to_owned(),
        source_time_ms: ACCOUNT_EVENT_TIME_MS,
        canonical_source_rank: 7,
        receipt_time_ms: ACCOUNT_RECEIPT_TIME_MS,
        connection_id: format!("{ACCOUNT_SOURCE_ID}#0"),
        connection_epoch: 0,
        frame_sequence: 0,
        ingest_sequence: 0,
    };
    let envelope = pmkit_event::PmAccountEnvelope {
        portfolio: portfolio.clone(),
        metadata: metadata.clone(),
        raw_frame: ACCOUNT_RAW_FRAME.to_vec(),
        fact: pmkit_event::PmAccountEvent::OrderAck {
            strategy: None,
            order_id: "synthetic-order-ack".to_owned(),
            timestamp_ms: ACCOUNT_EVENT_TIME_MS,
        },
    };
    let mut writer = RawCaptureWriter::new(Vec::new());
    assert_eq!(
        writer.append(RawCaptureDraft {
            schema_version: metadata.schema_version,
            source_id: metadata.source_id.clone(),
            canonical_source_rank: metadata.canonical_source_rank,
            receipt_time_ms: metadata.receipt_time_ms,
            transport: Some(CaptureTransport {
                source_time_ms: Some(metadata.source_time_ms),
                connection_id: metadata.connection_id.clone(),
                connection_epoch: 0,
                frame_sequence: 0,
                ingest_sequence: 0,
            }),
            normalized_projection: Some(pmkit_tape::account_envelope_json(&envelope)),
            payload: RawCapturePayload::Frame {
                raw_bytes: envelope.raw_frame.clone(),
            },
        })?,
        0
    );

    let strategy_calls = Arc::new(Mutex::new(Vec::new()));
    let replay = ReplaySpec::new(
        Arc::new(ScriptedHistory { ticks: Vec::new() }),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    );
    let run = BacktestRun::new(
        RunId::new("bt-recorded-account")?,
        portfolio,
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
    .strategy(StrategyRegistration::new(
        StrategyId::new("recorded-account-probe")?,
        MarketId::new("account-test-5m")?,
        Arc::new(RecordedFactProbeFactory(Arc::clone(&strategy_calls))),
    ));
    let error = crate::RecordedBacktest::replay_capture(
        &run,
        crate::capture_replay::CaptureReplaySource::new(Cursor::new(writer.into_inner())),
    )
    .drive(None)
    .await
    .err()
    .ok_or("recorded PM account capture unexpectedly completed")?;
    let StartError::Source {
        source: DataSourceError::ReplayGap { message },
        ..
    } = error
    else {
        return Err(format!("expected a replay-gap source error, got {error:?}").into());
    };
    assert!(
        message.contains("recorded PM account facts are unsupported"),
        "unexpected replay-gap message: {message}"
    );
    assert!(
        strategy_calls
            .lock()
            .map_err(|_| "recorded fact probe lock poisoned")?
            .is_empty(),
        "strategy was evaluated before rejecting the recorded account fact"
    );
    Ok(())
}
