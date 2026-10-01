use std::error::Error;

use pmkit_data::{DataSourceError, GapRange, GapSequenceDomain, SourceSignal};
use pmkit_event::{CexReferenceEvent, SourceEnvelope};
use pmkit_market::Asset;
use polymarket_client_sdk_v2::auth::{Credentials, Uuid};
use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::{
    POLYBOLT_ORACLE_SOURCE_ID, PolyBoltControl, PolyBoltControlKind, PolyBoltEvent, PolyBoltGap,
    PolyBoltOracleDataSource, PolyBoltOracleSourceError, PolyBoltRawFrame, PolyBoltRecord,
    PolyBoltTwapPrice,
};

use super::{deliver_control, deliver_record};

const SESSION_ID: u64 = 17;

fn price(timestamp_ms: i64, value: &str) -> Result<PolyBoltTwapPrice, rust_decimal::Error> {
    Ok(PolyBoltTwapPrice {
        symbol: "btcusd".to_owned(),
        source: "chainlink".to_owned(),
        window_seconds: 60,
        timestamp_ms,
        exact_value: Decimal::from_str_exact(value)?,
    })
}

fn raw(bytes: &[u8]) -> PolyBoltRawFrame {
    PolyBoltRawFrame {
        receipt_time_ms: 4567,
        bytes: bytes.to_vec(),
    }
}

fn record(
    raw: PolyBoltRawFrame,
    connection_epoch: u64,
    sequence: u64,
    server_timestamp_ms: i64,
    event: PolyBoltEvent,
) -> PolyBoltRecord {
    PolyBoltRecord {
        raw,
        connection_epoch,
        sequence,
        server_timestamp_ms,
        event,
    }
}

fn assert_metadata(
    metadata: &pmkit_event::StreamMetadata,
    connection_epoch: i64,
    frame_sequence: i64,
    ingest_sequence: u64,
    source_time_ms: i64,
) {
    assert_eq!(
        metadata.source_id,
        "polymarket:polybolt:price.crypto.twap:60"
    );
    assert_eq!(metadata.canonical_source_rank, 2);
    assert_eq!(metadata.receipt_time_ms, 4567);
    assert_eq!(
        metadata.connection_id,
        format!("polymarket:polybolt:price.crypto.twap:60#{SESSION_ID}:{connection_epoch}")
    );
    assert_eq!(metadata.connection_epoch, connection_epoch);
    assert_eq!(metadata.frame_sequence, frame_sequence);
    assert_eq!(metadata.ingest_sequence, ingest_sequence);
    assert_eq!(metadata.source_time_ms, source_time_ms);
}

#[tokio::test]
async fn snapshot_projects_every_exact_point_with_one_raw_transport_frame()
-> Result<(), Box<dyn Error>> {
    let raw_bytes = br#"{"v":1,"channel":"price.crypto.twap","seq":90,"ts":500,"snapshot":true,"payload":{"symbol":"btcusd","source":"chainlink","window_seconds":60,"data":[{"timestamp":501,"full_accuracy_value":"1234.0000000000000000001"},{"timestamp":502,"full_accuracy_value":"1235.25"}]}}"#;
    let mut next_ingest_sequence = 0;
    let (sink, mut signals) = mpsc::channel(4);

    deliver_record(
        record(
            raw(raw_bytes),
            3,
            90,
            500,
            PolyBoltEvent::Snapshot {
                prices: vec![
                    price(501, "1234.0000000000000000001")?,
                    price(502, "1235.25")?,
                ],
            },
        ),
        14,
        SESSION_ID,
        &mut next_ingest_sequence,
        &sink,
    )
    .await?;

    for (ingest_sequence, timestamp_ms, expected_price) in
        [(0, 501, "1234.0000000000000000001"), (1, 502, "1235.25")]
    {
        let Some(SourceSignal::Data(envelope)) = signals.recv().await else {
            return Err("expected snapshot point".into());
        };
        let SourceEnvelope::CexReference(envelope) = *envelope else {
            return Err("expected CEX reference envelope".into());
        };
        assert_eq!(envelope.raw_frame, raw_bytes);
        assert_metadata(&envelope.metadata, 3, 14, ingest_sequence, timestamp_ms);
        assert!(matches!(
            envelope.fact,
            CexReferenceEvent::Twap60sSnapshot {
                asset: Asset::Btc,
                price,
                timestamp_ms: actual_timestamp,
            } if price.to_string() == expected_price && actual_timestamp == timestamp_ms
        ));
    }
    assert!(signals.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn update_and_empty_snapshot_preserve_their_exact_frame_metadata()
-> Result<(), Box<dyn Error>> {
    let update_bytes = br#"{"v":1,"channel":"price.crypto.twap","seq":91,"ts":600,"payload":{"symbol":"btcusd","source":"chainlink","window_seconds":60,"timestamp":601,"full_accuracy_value":"1236.1234567890123456789"}}"#;
    let empty_bytes = br#"{"v":1,"channel":"price.crypto.twap","seq":92,"ts":602,"snapshot":true,"payload":{"symbol":"btcusd","source":"chainlink","window_seconds":60,"data":[]}}"#;
    let mut next_ingest_sequence = 0;
    let (sink, mut signals) = mpsc::channel(4);

    deliver_record(
        record(
            raw(update_bytes),
            4,
            91,
            600,
            PolyBoltEvent::Update {
                price: price(601, "1236.1234567890123456789")?,
            },
        ),
        7,
        SESSION_ID,
        &mut next_ingest_sequence,
        &sink,
    )
    .await?;
    let Some(SourceSignal::Data(envelope)) = signals.recv().await else {
        return Err("expected TWAP update".into());
    };
    let SourceEnvelope::CexReference(envelope) = *envelope else {
        return Err("expected CEX reference envelope".into());
    };
    assert_eq!(envelope.raw_frame, update_bytes);
    assert_metadata(&envelope.metadata, 4, 7, 0, 601);
    assert!(matches!(
        envelope.fact,
        CexReferenceEvent::Twap60sUpdate { price, timestamp_ms: 601, asset: Asset::Btc }
            if price.to_string() == "1236.1234567890123456789"
    ));

    deliver_record(
        record(
            raw(empty_bytes),
            4,
            92,
            602,
            PolyBoltEvent::EmptySnapshot {
                symbol: "btcusd".to_owned(),
                source: "chainlink".to_owned(),
                window_seconds: 60,
            },
        ),
        8,
        SESSION_ID,
        &mut next_ingest_sequence,
        &sink,
    )
    .await?;
    let Some(SourceSignal::Heartbeat(heartbeat)) = signals.recv().await else {
        return Err("expected empty-snapshot heartbeat".into());
    };
    assert_eq!(heartbeat.raw_frame, empty_bytes);
    assert_metadata(&heartbeat.metadata, 4, 8, 1, 602);
    Ok(())
}

#[tokio::test]
async fn native_ping_and_pong_become_heartbeats_with_exact_payload_and_receipt()
-> Result<(), Box<dyn Error>> {
    let mut next_ingest_sequence = 5;
    let (sink, mut signals) = mpsc::channel(1);

    for (kind, payload, frame_sequence, ingest_sequence) in [
        (
            PolyBoltControlKind::Ping,
            b"native ping payload".as_slice(),
            23,
            5,
        ),
        (
            PolyBoltControlKind::Pong,
            b"native pong payload".as_slice(),
            24,
            6,
        ),
    ] {
        deliver_control(
            PolyBoltControl {
                raw: raw(payload),
                connection_epoch: 6,
                frame_sequence,
                kind,
            },
            SESSION_ID,
            &mut next_ingest_sequence,
            &sink,
        )
        .await?;

        let Some(SourceSignal::Heartbeat(heartbeat)) = signals.recv().await else {
            return Err("expected native-control heartbeat".into());
        };
        assert_eq!(heartbeat.raw_frame, payload);
        assert_metadata(
            &heartbeat.metadata,
            6,
            i64::try_from(frame_sequence)?,
            ingest_sequence,
            4567,
        );
    }
    Ok(())
}

#[tokio::test]
async fn reported_channel_gap_keeps_its_domain_and_fails_closed() -> Result<(), Box<dyn Error>> {
    let raw_bytes = br#"{"v":1,"channel":"price.crypto.twap","seq":102,"ts":700,"dropped":2,"payload":{"symbol":"btcusd","source":"chainlink","window_seconds":60,"timestamp":701,"full_accuracy_value":"1237.5"}}"#;
    let mut next_ingest_sequence = 0;
    let (sink, mut signals) = mpsc::channel(1);

    let result = deliver_record(
        record(
            raw(raw_bytes),
            8,
            102,
            700,
            PolyBoltEvent::Gap(PolyBoltGap {
                expected_sequence: Some(100),
                observed_sequence: 102,
                dropped: 2,
            }),
        ),
        4,
        SESSION_ID,
        &mut next_ingest_sequence,
        &sink,
    )
    .await;

    assert!(matches!(result, Err(DataSourceError::ReplayGap { .. })));
    let Some(SourceSignal::Gap(gap)) = signals.recv().await else {
        return Err("reported gap must be sent before returning the error".into());
    };
    assert_eq!(gap.source_id, POLYBOLT_ORACLE_SOURCE_ID);
    assert_eq!(gap.detected_at_ms, 4567);
    assert_eq!(gap.raw_frame, raw_bytes);
    assert_eq!(
        gap.range,
        Some(GapRange {
            domain: GapSequenceDomain::Channel,
            first_missing: 100,
            last_missing: 101,
        })
    );
    assert_eq!(gap.unresolved_count, Some(2));
    let Some(metadata) = gap.metadata.as_ref() else {
        return Err("reported gap must retain its transport metadata".into());
    };
    assert_metadata(metadata, 8, 4, 0, 700);
    assert!(signals.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn dense_dropped_count_emits_gap_without_inventing_sequence_bounds()
-> Result<(), Box<dyn Error>> {
    let raw_bytes = br#"{"v":1,"channel":"price.crypto.twap","seq":10,"ts":700,"dropped":2,"payload":{"symbol":"btcusd","source":"chainlink","window_seconds":60,"timestamp":701,"full_accuracy_value":"1237.5"}}"#;
    let mut next_ingest_sequence = 0;
    let (sink, mut signals) = mpsc::channel(1);

    let result = deliver_record(
        record(
            raw(raw_bytes),
            8,
            10,
            700,
            PolyBoltEvent::Gap(PolyBoltGap {
                expected_sequence: Some(10),
                observed_sequence: 10,
                dropped: 2,
            }),
        ),
        4,
        SESSION_ID,
        &mut next_ingest_sequence,
        &sink,
    )
    .await;

    assert!(matches!(result, Err(DataSourceError::ReplayGap { .. })));
    let Some(SourceSignal::Gap(gap)) = signals.recv().await else {
        return Err("unidentifiable dropped interval must emit a gap".into());
    };
    assert_eq!(gap.source_id, POLYBOLT_ORACLE_SOURCE_ID);
    assert_eq!(gap.detected_at_ms, 4567);
    assert_eq!(gap.raw_frame, raw_bytes);
    assert_eq!(gap.range, None);
    assert_eq!(gap.unresolved_count, Some(2));
    let Some(metadata) = gap.metadata.as_ref() else {
        return Err("reported gap must retain its transport metadata".into());
    };
    assert_metadata(metadata, 8, 4, 0, 700);
    assert!(signals.try_recv().is_err());
    Ok(())
}

#[tokio::test]
async fn other_symbol_and_channel_frames_are_forwarded_as_exact_heartbeats()
-> Result<(), Box<dyn Error>> {
    let symbol_bytes = b"exact other-symbol frame";
    let channel_bytes = b"exact other-channel frame";
    let mut next_ingest_sequence = 0;
    let (sink, mut signals) = mpsc::channel(2);

    for (event, bytes, frame_sequence, ingest_sequence) in [
        (
            PolyBoltEvent::OtherSymbol {
                symbol: "ethusd".to_owned(),
            },
            symbol_bytes.as_slice(),
            40,
            0,
        ),
        (
            PolyBoltEvent::OtherChannel {
                channel: "price.other".to_owned(),
            },
            channel_bytes.as_slice(),
            41,
            1,
        ),
    ] {
        deliver_record(
            record(raw(bytes), 9, frame_sequence, 700, event),
            frame_sequence,
            SESSION_ID,
            &mut next_ingest_sequence,
            &sink,
        )
        .await?;

        let Some(SourceSignal::Heartbeat(heartbeat)) = signals.recv().await else {
            return Err("valid unmatched frame must be retained".into());
        };
        assert_eq!(heartbeat.raw_frame, bytes);
        assert_metadata(
            &heartbeat.metadata,
            9,
            i64::try_from(frame_sequence)?,
            ingest_sequence,
            700,
        );
    }
    Ok(())
}

#[test]
fn missing_and_empty_credentials_fail_closed_without_leaking_secrets() -> Result<(), Box<dyn Error>>
{
    let Err(missing) = PolyBoltOracleDataSource::new(None) else {
        return Err("missing credentials must be rejected".into());
    };
    assert_eq!(missing, PolyBoltOracleSourceError::MissingCredentials);
    assert_eq!(
        missing.to_string(),
        "PolyBolt oracle credentials are required"
    );

    for credentials in [
        Credentials::new(
            Uuid::nil(),
            "not-a-secret".to_owned(),
            "not-a-passphrase".to_owned(),
        ),
        Credentials::new(
            Uuid::parse_str("00000000-0000-0000-0000-000000000001")?,
            String::new(),
            "not-a-passphrase".to_owned(),
        ),
        Credentials::new(
            Uuid::parse_str("00000000-0000-0000-0000-000000000001")?,
            "not-a-secret".to_owned(),
            String::new(),
        ),
    ] {
        let Err(error) = PolyBoltOracleDataSource::new(Some(credentials)) else {
            return Err("empty credentials must be rejected".into());
        };
        assert_eq!(error, PolyBoltOracleSourceError::MissingCredentials);
        let displayed = format!("{error}; {error:?}");
        assert!(!displayed.contains("not-a-secret"));
        assert!(!displayed.contains("not-a-passphrase"));
    }

    let credentials = Credentials::new(
        Uuid::parse_str("00000000-0000-0000-0000-000000000002")?,
        "secret-must-not-appear".to_owned(),
        "passphrase-must-not-appear".to_owned(),
    );
    let source = PolyBoltOracleDataSource::new(Some(credentials))?;
    let debug = format!("{source:?}");
    assert!(!debug.contains("secret-must-not-appear"));
    assert!(!debug.contains("passphrase-must-not-appear"));
    Ok(())
}
