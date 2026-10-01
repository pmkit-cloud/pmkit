use super::*;

const ORACLE_BINDING: &str = "settlement";
const ORACLE_TIMESTAMP_MS: i64 = 1_700_000_000_123;
const ORACLE_PRICE: &str = "43210.1234567890123456789";

fn native_polybolt_update() -> Result<RawCaptureDraft, Box<dyn std::error::Error>> {
    let source_id = pmkit_polymarket::POLYBOLT_ORACLE_SOURCE_ID;
    let receipt_time_ms = ORACLE_TIMESTAMP_MS + 5;
    let raw_bytes = serde_json::to_vec(&serde_json::json!({
        "v": 1,
        "channel": pmkit_polymarket::POLYBOLT_TWAP_CHANNEL,
        "seq": 0,
        "ts": ORACLE_TIMESTAMP_MS,
        "snapshot": false,
        "payload": {
            "symbol": "btcusd",
            "source": "chainlink",
            "window_seconds": 60,
            "timestamp": ORACLE_TIMESTAMP_MS,
            "full_accuracy_value": ORACLE_PRICE,
        }
    }))?;
    let record = pmkit_polymarket::PolyBoltDecoder::default().decode(
        pmkit_polymarket::PolyBoltRawFrame {
            receipt_time_ms,
            bytes: raw_bytes.clone(),
        },
        0,
    )?;
    let pmkit_polymarket::PolyBoltEvent::Update { price } = record.event else {
        return Err("expected a native PolyBolt TWAP update".into());
    };
    let metadata = StreamMetadata {
        schema_version: 1,
        source_id: source_id.to_owned(),
        source_time_ms: price.timestamp_ms,
        canonical_source_rank: 2,
        receipt_time_ms,
        connection_id: format!("{source_id}#0:0"),
        connection_epoch: 0,
        frame_sequence: 0,
        ingest_sequence: 0,
    };
    let envelope = CexReferenceEnvelope {
        metadata: metadata.clone(),
        raw_frame: raw_bytes.clone(),
        fact: CexReferenceEvent::Twap60sUpdate {
            asset: Asset::Btc,
            price: price.exact_value,
            timestamp_ms: price.timestamp_ms,
        },
    };
    Ok(RawCaptureDraft {
        schema_version: metadata.schema_version,
        source_id: source_id.to_owned(),
        canonical_source_rank: metadata.canonical_source_rank,
        receipt_time_ms,
        transport: Some(CaptureTransport {
            source_time_ms: Some(metadata.source_time_ms),
            connection_id: metadata.connection_id,
            connection_epoch: 0,
            frame_sequence: 0,
            ingest_sequence: 0,
        }),
        normalized_projection: Some(pmkit_tape::reference_envelope_json(&envelope)),
        payload: RawCapturePayload::Frame { raw_bytes },
    })
}

fn required_oracle_run(
    id: &str,
    market: &MarketId,
    observed: Arc<Mutex<Vec<(StrategyFact, i64)>>>,
) -> Result<BacktestRun, Box<dyn std::error::Error>> {
    let replay = ReplaySpec::new(
        Arc::new(ScriptedHistory { ticks: Vec::new() }),
        "2026-01-01T00:00:00Z".parse()?,
        "2026-02-01T00:00:00Z".parse()?,
        EvidenceRequirement::CorroboratedOnly,
        RetrievalWait::ReturnPending,
    );
    Ok(BacktestRun::new(
        RunId::new(id)?,
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
    .settlement_oracle_named(ORACLE_BINDING, market.clone(), "BTC")
    .require_settlement_oracle(ORACLE_BINDING, market.clone())
    .strategy(StrategyRegistration::new(
        StrategyId::new("recorded-oracle-probe")?,
        market.clone(),
        Arc::new(RecordedFactProbeFactory(observed)),
    )))
}

#[tokio::test]
async fn recorded_polybolt_capture_fulfills_required_oracle_without_credentials()
-> Result<(), Box<dyn std::error::Error>> {
    let market = MarketId::new("btc-5m")?;
    let mut writer = RawCaptureWriter::new(Vec::new());
    writer.append(native_book_capture(
        &market,
        ORACLE_TIMESTAMP_MS - 100,
        ORACLE_TIMESTAMP_MS - 90,
        Decimal::new(4, 1),
        0,
    )?)?;
    writer.append(native_polybolt_update()?)?;

    let observed = Arc::new(Mutex::new(Vec::new()));
    let run = required_oracle_run("bt-recorded-oracle", &market, Arc::clone(&observed))?;
    assert_eq!(
        run.settlement_oracle_bindings()[0].source_id(),
        pmkit_polymarket::POLYBOLT_ORACLE_SOURCE_ID
    );
    let tokens = MarketTokens::new(market.clone(), "1".parse()?, "2".parse()?);
    let report = crate::RecordedBacktest::replay_capture(
        &run,
        crate::capture_replay::CaptureReplaySource::new_with_market_tokens(
            Cursor::new(writer.into_inner()),
            HashMap::from([(market, tokens)]),
        ),
    )
    .drive(None)
    .await?;

    assert_eq!(report.events_processed, 1);
    let expected_price = Decimal::from_str_exact(ORACLE_PRICE)?;
    let observed = observed
        .lock()
        .map_err(|_| "recorded fact probe lock poisoned")?;
    assert!(observed.iter().any(|(fact, at_ms)| matches!(
        fact,
        StrategyFact::Reference(CexReferenceEvent::Twap60sUpdate {
            asset: Asset::Btc,
            price,
            timestamp_ms,
        }) if *price == expected_price
            && *timestamp_ms == ORACLE_TIMESTAMP_MS
            && *at_ms == ORACLE_TIMESTAMP_MS
    )));
    drop(observed);
    Ok(())
}

#[tokio::test]
async fn recorded_local_oracle_gap_stops_before_later_strategy_actions()
-> Result<(), Box<dyn std::error::Error>> {
    let market = MarketId::new("btc-5m")?;
    let mut writer = RawCaptureWriter::new(Vec::new());
    writer.append(native_book_capture(
        &market,
        ORACLE_TIMESTAMP_MS - 100,
        ORACLE_TIMESTAMP_MS - 90,
        Decimal::new(4, 1),
        0,
    )?)?;
    writer.append(RawCaptureDraft {
        schema_version: 3,
        source_id: pmkit_polymarket::POLYBOLT_ORACLE_SOURCE_ID.to_owned(),
        canonical_source_rank: 2,
        receipt_time_ms: ORACLE_TIMESTAMP_MS,
        transport: None,
        normalized_projection: None,
        payload: RawCapturePayload::Gap {
            range: None,
            unresolved_count: Some(1),
            reason: "local oracle capture gap".to_owned(),
            raw_frame: Vec::new(),
        },
    })?;
    writer.append(native_book_capture(
        &market,
        ORACLE_TIMESTAMP_MS + 100,
        ORACLE_TIMESTAMP_MS + 110,
        Decimal::new(6, 1),
        1,
    )?)?;
    let capture = writer.into_inner();

    let mut audit = pmkit_tape::RawCaptureReader::new_audit(Cursor::new(capture.clone()));
    assert!(audit.next_record()?.is_some());
    let gap = audit
        .next_record()?
        .ok_or("missing local oracle gap")?
        .capture;
    assert_eq!(gap.source_id, pmkit_polymarket::POLYBOLT_ORACLE_SOURCE_ID);
    assert!(gap.transport.is_none());
    assert!(gap.normalized_projection.is_none());
    assert!(matches!(
        gap.payload,
        RawCapturePayload::Gap {
            range: None,
            raw_frame,
            ..
        } if raw_frame.is_empty()
    ));
    assert!(audit.next_record()?.is_some());
    assert!(audit.next_record()?.is_none());

    let observed = Arc::new(Mutex::new(Vec::new()));
    let run = required_oracle_run("bt-recorded-oracle-gap", &market, Arc::clone(&observed))?;
    let tokens = MarketTokens::new(market.clone(), "1".parse()?, "2".parse()?);
    let error = crate::RecordedBacktest::replay_capture(
        &run,
        crate::capture_replay::CaptureReplaySource::new_with_market_tokens(
            Cursor::new(capture),
            HashMap::from([(market, tokens)]),
        ),
    )
    .drive(None)
    .await
    .err()
    .ok_or("recorded local oracle gap unexpectedly completed")?;
    let StartError::Source {
        source: DataSourceError::ReplayGap { message },
        ..
    } = error
    else {
        return Err("recorded local oracle gap did not fail as a replay gap".into());
    };
    assert!(message.contains(pmkit_polymarket::POLYBOLT_ORACLE_SOURCE_ID));
    assert_eq!(
        observed
            .lock()
            .map_err(|_| "recorded fact probe lock poisoned")?
            .len(),
        1,
        "the book after the gap must not reach the strategy"
    );
    Ok(())
}
