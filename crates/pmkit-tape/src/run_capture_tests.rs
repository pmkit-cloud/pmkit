use std::io::Cursor;

use super::{
    CaptureTransport, GapRange, GapSequenceDomain, RawCaptureDraft, RawCaptureError,
    RawCapturePayload, RawCaptureReader, RawCaptureWriter, recover_raw_capture_prefix,
};

fn draft(
    source_id: &str,
    connection_id: &str,
    connection_epoch: u64,
    frame_sequence: u64,
    payload: RawCapturePayload,
) -> RawCaptureDraft {
    RawCaptureDraft {
        schema_version: 1,
        source_id: source_id.to_owned(),
        canonical_source_rank: 0,
        receipt_time_ms: 1_700_000_000_456,
        transport: Some(CaptureTransport {
            source_time_ms: Some(1_700_000_000_123),
            connection_id: connection_id.to_owned(),
            connection_epoch,
            frame_sequence,
            ingest_sequence: frame_sequence,
        }),
        normalized_projection: None,
        payload,
    }
}

fn frame(bytes: &[u8], frame_sequence: u64) -> RawCaptureDraft {
    draft(
        "polymarket:market",
        "pm-connection",
        2,
        frame_sequence,
        RawCapturePayload::Frame {
            raw_bytes: bytes.to_vec(),
        },
    )
}

fn gap(first: u64, last: u64, reporting_sequence: u64) -> RawCaptureDraft {
    draft(
        "polymarket:market",
        "pm-connection",
        2,
        reporting_sequence,
        RawCapturePayload::Gap {
            range: Some(GapRange {
                domain: GapSequenceDomain::Transport,
                first_missing: first,
                last_missing: last,
            }),
            unresolved_count: None,
            reason: "transport sequence discontinuity".to_owned(),
            raw_frame: b"gap reporting frame".to_vec(),
        },
    )
}

fn local_gap(source_id: &str) -> RawCaptureDraft {
    RawCaptureDraft {
        schema_version: 1,
        source_id: source_id.to_owned(),
        canonical_source_rank: 0,
        receipt_time_ms: 1_700_000_000_456,
        transport: None,
        normalized_projection: None,
        payload: RawCapturePayload::Gap {
            range: None,
            unresolved_count: None,
            reason: "locally detected source silence".to_owned(),
            raw_frame: Vec::new(),
        },
    }
}

fn channel_gap(first: u64, last: u64, reporting_sequence: u64) -> RawCaptureDraft {
    draft(
        "polymarket:market",
        "pm-connection",
        2,
        reporting_sequence,
        RawCapturePayload::Gap {
            range: Some(GapRange {
                domain: GapSequenceDomain::Channel,
                first_missing: first,
                last_missing: last,
            }),
            unresolved_count: None,
            reason: "channel sequence discontinuity".to_owned(),
            raw_frame: b"gap reporting frame".to_vec(),
        },
    )
}

#[test]
fn round_trip_preserves_observed_order_metadata_and_opaque_raw_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let mut writer = RawCaptureWriter::new(Vec::new());
    let mut pm_draft = frame(b"\x00PM\xff", 4);
    pm_draft.schema_version = 3;
    pm_draft.canonical_source_rank = 9;
    let transport = pm_draft.transport.as_mut().ok_or("missing transport")?;
    transport.ingest_sequence = 42;
    pm_draft.normalized_projection = Some(serde_json::json!({
        "kind": "best_bid_ask",
        "bid": "0.41",
        "ask": "0.43",
    }));
    assert_eq!(writer.append(pm_draft)?, 0);
    assert_eq!(
        writer.append(draft(
            "binance:aggTrade",
            "cex-connection",
            1,
            0,
            RawCapturePayload::Snapshot {
                raw_bytes: br#"{"type":"snapshot","price":"0.42"}"#.to_vec(),
            },
        ))?,
        1
    );
    assert_eq!(
        writer.append(draft(
            "polybolt:oracle",
            "oracle-connection",
            0,
            0,
            RawCapturePayload::Frame {
                raw_bytes: b"oracle bytes, not normalized JSON".to_vec(),
            },
        ))?,
        2
    );
    writer.flush()?;

    let bytes = writer.into_inner();
    let wire: serde_json::Value = serde_json::from_slice(
        bytes
            .split(|byte| *byte == b'\n')
            .next()
            .ok_or("missing wire record")?,
    )?;
    assert_eq!(wire["body"]["schema_version"], 4);
    assert_eq!(wire["body"]["projection_index"], 0);
    let mut reader = RawCaptureReader::new(Cursor::new(bytes));
    let pm = reader.next_record()?.ok_or("missing PM frame")?;
    assert_eq!(pm.ingest_sequence, 0);
    assert_eq!(pm.projection_index, 0);
    assert_eq!(pm.capture.schema_version, 3);
    assert_eq!(pm.capture.source_id, "polymarket:market");
    assert_eq!(pm.capture.canonical_source_rank, 9);
    assert_eq!(pm.capture.receipt_time_ms, 1_700_000_000_456);
    assert_eq!(
        pm.capture.transport,
        Some(CaptureTransport {
            source_time_ms: Some(1_700_000_000_123),
            connection_id: "pm-connection".to_owned(),
            connection_epoch: 2,
            frame_sequence: 4,
            ingest_sequence: 42,
        })
    );
    assert_eq!(
        pm.capture.normalized_projection,
        Some(serde_json::json!({
            "kind": "best_bid_ask",
            "bid": "0.41",
            "ask": "0.43",
        }))
    );
    assert_eq!(
        pm.capture.payload,
        RawCapturePayload::Frame {
            raw_bytes: b"\x00PM\xff".to_vec(),
        }
    );

    let cex = reader.next_record()?.ok_or("missing CEX snapshot")?;
    assert_eq!(cex.ingest_sequence, 1);
    assert_eq!(cex.projection_index, 0);
    assert_eq!(cex.capture.source_id, "binance:aggTrade");
    assert!(matches!(
        cex.capture.payload,
        RawCapturePayload::Snapshot { .. }
    ));

    let oracle = reader.next_record()?.ok_or("missing oracle frame")?;
    assert_eq!(oracle.ingest_sequence, 2);
    assert_eq!(oracle.projection_index, 0);
    assert_eq!(oracle.capture.source_id, "polybolt:oracle");
    assert_eq!(
        oracle.capture.payload,
        RawCapturePayload::Frame {
            raw_bytes: b"oracle bytes, not normalized JSON".to_vec(),
        }
    );
    assert!(reader.next_record()?.is_none());
    Ok(())
}

#[test]
fn schema_three_tapes_are_rejected_before_legacy_capture_deserialization()
-> Result<(), Box<dyn std::error::Error>> {
    let mut writer = RawCaptureWriter::new(Vec::new());
    writer.append(frame(b"legacy frame", 0))?;
    let mut legacy: serde_json::Value = serde_json::from_slice(&writer.into_inner())?;
    legacy["body"]["schema_version"] = serde_json::json!(3);
    let capture = legacy["body"]["capture"]
        .as_object_mut()
        .ok_or("capture must be an object")?;
    let transport = capture
        .remove("transport")
        .ok_or("missing transport bundle")?;
    let transport = transport.as_object().ok_or("transport must be an object")?;
    for field in [
        "source_time_ms",
        "connection_id",
        "connection_epoch",
        "frame_sequence",
        "ingest_sequence",
    ] {
        capture.insert(
            field.to_owned(),
            transport
                .get(field)
                .cloned()
                .ok_or("missing transport field")?,
        );
    }
    let mut legacy = serde_json::to_vec(&legacy)?;
    legacy.push(b'\n');
    assert!(matches!(
        RawCaptureReader::new(Cursor::new(legacy)).next_record(),
        Err(RawCaptureError::UnsupportedSchemaVersion { found: 3 })
    ));
    Ok(())
}

#[test]
fn same_transport_frame_round_trips_ordered_distinct_projections()
-> Result<(), Box<dyn std::error::Error>> {
    let raw = b"one snapshot frame with three oracle points";
    let point_times = [1_700_000_000_100, 1_700_000_000_200, 1_700_000_000_300];
    let mut writer = RawCaptureWriter::new(Vec::new());
    for (index, point_time) in point_times.into_iter().enumerate() {
        let mut projection = frame(raw, 7);
        let transport = projection.transport.as_mut().ok_or("missing transport")?;
        transport.source_time_ms = Some(point_time);
        projection.receipt_time_ms = 1_700_000_000_456;
        transport.ingest_sequence = 80 + u64::try_from(index)?;
        projection.normalized_projection = Some(serde_json::json!({
            "kind": "oracle_point",
            "point_time_ms": point_time,
        }));
        assert_eq!(writer.append(projection)?, u64::try_from(index)?);
    }

    let mut reader = RawCaptureReader::new(Cursor::new(writer.into_inner()));
    for (index, point_time) in point_times.into_iter().enumerate() {
        let record = reader.next_record()?.ok_or("missing projection")?;
        assert_eq!(record.projection_index, u64::try_from(index)?);
        assert_eq!(
            record
                .capture
                .transport
                .as_ref()
                .map(|transport| transport.frame_sequence),
            Some(7)
        );
        assert_eq!(
            record
                .capture
                .transport
                .as_ref()
                .and_then(|transport| transport.source_time_ms),
            Some(point_time)
        );
        assert_eq!(record.capture.receipt_time_ms, 1_700_000_000_456);
        assert_eq!(
            record.capture.payload,
            RawCapturePayload::Frame {
                raw_bytes: raw.to_vec(),
            }
        );
    }
    assert!(reader.next_record()?.is_none());
    Ok(())
}

#[test]
fn rejects_duplicate_or_mismatched_same_frame_projections() -> Result<(), Box<dyn std::error::Error>>
{
    let mut first = frame(b"same frame", 4);
    first.normalized_projection = Some(serde_json::json!({"fact": "first"}));

    let mut writer = RawCaptureWriter::new(Vec::new());
    writer.append(first.clone())?;
    assert!(matches!(
        writer.append(first.clone()),
        Err(RawCaptureError::MalformedRecord { .. })
    ));

    for mismatch in 0..3 {
        let mut writer = RawCaptureWriter::new(Vec::new());
        writer.append(first.clone())?;
        let mut duplicate = first.clone();
        duplicate.normalized_projection = Some(serde_json::json!({"fact": "second"}));
        match mismatch {
            0 => {
                duplicate.payload = RawCapturePayload::Frame {
                    raw_bytes: b"different frame".to_vec(),
                }
            }
            1 => duplicate.receipt_time_ms += 1,
            _ => {
                duplicate.payload = RawCapturePayload::Snapshot {
                    raw_bytes: b"same frame".to_vec(),
                }
            }
        }
        assert!(matches!(
            writer.append(duplicate),
            Err(RawCaptureError::MalformedRecord { .. })
        ));
    }
    Ok(())
}

#[test]
fn gap_records_keep_the_reporting_frame_and_source_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let mut writer = RawCaptureWriter::new(Vec::new());
    writer.append(frame(b"before-gap", 0))?;
    let mut gap = gap(1, 1, 2);
    gap.schema_version = 4;
    gap.canonical_source_rank = 7;
    gap.transport
        .as_mut()
        .ok_or("missing transport")?
        .ingest_sequence = 8;
    writer.append(gap)?;

    let bytes = writer.into_inner();
    let line = bytes
        .split(|byte| *byte == b'\n')
        .nth(1)
        .ok_or("missing gap record")?;
    let wire: serde_json::Value = serde_json::from_slice(line)?;
    assert_eq!(wire["body"]["ingest_sequence"], 1);
    assert_eq!(wire["body"]["capture"]["schema_version"], 4);
    assert_eq!(wire["body"]["capture"]["canonical_source_rank"], 7);
    assert_eq!(wire["body"]["capture"]["transport"]["ingest_sequence"], 8);
    assert_eq!(wire["body"]["capture"]["transport"]["frame_sequence"], 2);
    assert_eq!(
        wire["body"]["capture"]["transport"]["connection_id"],
        "pm-connection"
    );
    assert_eq!(
        wire["body"]["capture"]["payload"]["range"]["domain"],
        "transport"
    );
    assert_eq!(
        wire["body"]["capture"]["payload"]["raw_frame"],
        serde_json::json!(b"gap reporting frame".to_vec())
    );
    assert!(wire["body"]["capture"]["normalized_projection"].is_null());
    Ok(())
}

#[test]
fn local_gap_without_reporting_bytes_does_not_consume_a_reporting_frame()
-> Result<(), Box<dyn std::error::Error>> {
    let mut writer = RawCaptureWriter::new(Vec::new());
    writer.append(frame(b"before-local-gap", 0))?;
    writer.append(local_gap("polymarket:market"))?;
    writer.append(frame(b"after-local-gap", 1))?;

    let mut audit = RawCaptureReader::new_audit(Cursor::new(writer.into_inner()));
    audit.next_record()?.ok_or("missing first frame")?;
    let decoded = audit.next_record()?.ok_or("missing local gap")?;
    assert!(matches!(
        decoded.capture.payload,
        RawCapturePayload::Gap { raw_frame, .. } if raw_frame.is_empty()
    ));
    assert!(decoded.capture.transport.is_none());
    assert_eq!(
        audit
            .next_record()?
            .ok_or("missing frame after local gap")?
            .capture
            .transport
            .as_ref()
            .map(|transport| transport.frame_sequence),
        Some(1)
    );
    Ok(())
}

#[test]
fn local_gap_does_not_reconcile_a_missing_frame() -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = RawCaptureWriter::new(Vec::new());
    writer.append(frame(b"transport-three-0", 0))?;
    writer.append(frame(b"transport-three-1", 1))?;
    writer.append(frame(b"transport-three-2", 2))?;
    writer.append(frame(b"transport-three-3", 3))?;
    writer.append(local_gap("polymarket:market"))?;
    assert!(matches!(
        writer.append(frame(b"skipped-physical-frame-4", 5)),
        Err(RawCaptureError::SourceSequenceGap {
            expected: 4,
            found: 5,
            ..
        })
    ));
    Ok(())
}

#[test]
fn never_observed_local_gap_does_not_seed_transport_history()
-> Result<(), Box<dyn std::error::Error>> {
    let mut writer = RawCaptureWriter::new(Vec::new());
    writer.append(local_gap("logical:subscription"))?;
    assert_eq!(
        writer.append(draft(
            "logical:subscription",
            "first-real-connection",
            4,
            8,
            RawCapturePayload::Frame {
                raw_bytes: b"first observed frame".to_vec(),
            },
        ))?,
        1
    );
    Ok(())
}

#[test]
fn channel_gap_uses_transport_continuity_not_channel_sequence()
-> Result<(), Box<dyn std::error::Error>> {
    let mut writer = RawCaptureWriter::new(Vec::new());
    for sequence in 0..=3 {
        writer.append(frame(b"physical frame", sequence))?;
    }
    writer.append(channel_gap(100, 100, 4))?;
    writer.append(frame(b"physical frame after channel gap", 5))?;

    let mut audit = RawCaptureReader::new_audit(Cursor::new(writer.into_inner()));
    for _ in 0..4 {
        audit.next_record()?.ok_or("missing prior physical frame")?;
    }
    assert!(matches!(
        audit
            .next_record()?
            .ok_or("missing channel gap")?
            .capture
            .payload,
        RawCapturePayload::Gap {
            range: Some(GapRange {
                domain: GapSequenceDomain::Channel,
                first_missing: 100,
                last_missing: 100,
            }),
            ..
        }
    ));
    assert_eq!(
        audit
            .next_record()?
            .ok_or("missing frame after channel gap")?
            .capture
            .transport
            .as_ref()
            .map(|transport| transport.frame_sequence),
        Some(5)
    );
    Ok(())
}

#[test]
fn channel_gap_reporting_frame_must_be_transport_contiguous()
-> Result<(), Box<dyn std::error::Error>> {
    let mut writer = RawCaptureWriter::new(Vec::new());
    for sequence in 0..=3 {
        writer.append(frame(b"physical frame", sequence))?;
    }
    assert!(matches!(
        writer.append(channel_gap(100, 100, 5)),
        Err(RawCaptureError::SourceSequenceGap {
            expected: 4,
            found: 5,
            ..
        })
    ));
    Ok(())
}

#[test]
fn witnessed_empty_raw_gap_consumes_its_real_reporting_frame()
-> Result<(), Box<dyn std::error::Error>> {
    let mut writer = RawCaptureWriter::new(Vec::new());
    for sequence in 0..=3 {
        writer.append(frame(b"physical frame", sequence))?;
    }
    writer.append(draft(
        "polymarket:market",
        "pm-connection",
        2,
        4,
        RawCapturePayload::Gap {
            range: None,
            unresolved_count: Some(1),
            reason: "source reported one dropped item".to_owned(),
            raw_frame: Vec::new(),
        },
    ))?;
    writer.append(frame(b"physical frame after empty report", 5))?;

    let mut audit = RawCaptureReader::new_audit(Cursor::new(writer.into_inner()));
    for _ in 0..4 {
        audit.next_record()?.ok_or("missing prior physical frame")?;
    }
    let gap_record = audit.next_record()?.ok_or("missing observed gap")?;
    assert!(gap_record.capture.transport.is_some());
    assert!(matches!(
        gap_record.capture.payload,
        RawCapturePayload::Gap {
            range: None,
            unresolved_count: Some(1),
            raw_frame,
            ..
        } if raw_frame.is_empty()
    ));
    assert_eq!(
        audit
            .next_record()?
            .ok_or("missing frame after empty report")?
            .capture
            .transport
            .as_ref()
            .map(|transport| transport.frame_sequence),
        Some(5)
    );
    Ok(())
}

#[test]
fn known_transport_gap_requires_exact_inclusive_reporting_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    for (first, last, reporting) in [(11, 13, 100), (11, 12, 14)] {
        let mut writer = RawCaptureWriter::new(Vec::new());
        writer.append(frame(b"last observed", 10))?;
        assert!(matches!(
            writer.append(gap(first, last, reporting)),
            Err(RawCaptureError::MalformedRecord { .. })
        ));
    }

    let mut writer = RawCaptureWriter::new(Vec::new());
    writer.append(frame(b"last observed", 10))?;
    writer.append(gap(11, 13, 14))?;
    writer.append(frame(b"next contiguous frame", 15))?;
    Ok(())
}

#[test]
fn observed_data_and_control_records_require_transport_metadata() {
    for payload in [
        RawCapturePayload::Frame {
            raw_bytes: Vec::new(),
        },
        RawCapturePayload::Snapshot {
            raw_bytes: Vec::new(),
        },
        RawCapturePayload::Control {
            name: "heartbeat".to_owned(),
            raw_bytes: Vec::new(),
        },
    ] {
        let mut capture = draft("source", "connection", 0, 0, payload);
        capture.transport = None;
        assert!(matches!(
            RawCaptureWriter::new(Vec::new()).append(capture),
            Err(RawCaptureError::MalformedRecord { .. })
        ));
    }
}

#[test]
fn local_gap_rejects_fake_raw_bytes_or_exact_ranges() {
    for payload in [
        RawCapturePayload::Gap {
            range: None,
            unresolved_count: Some(2),
            reason: "local gap".to_owned(),
            raw_frame: vec![1],
        },
        RawCapturePayload::Gap {
            range: Some(GapRange {
                domain: GapSequenceDomain::Channel,
                first_missing: 1,
                last_missing: 2,
            }),
            unresolved_count: None,
            reason: "local gap".to_owned(),
            raw_frame: Vec::new(),
        },
    ] {
        let mut capture = draft("source", "", 0, 0, payload);
        capture.transport = None;
        assert!(matches!(
            RawCaptureWriter::new(Vec::new()).append(capture),
            Err(RawCaptureError::MalformedRecord { .. })
        ));
    }
}

#[test]
fn heartbeat_control_retains_empty_raw_bytes_without_projection()
-> Result<(), Box<dyn std::error::Error>> {
    let mut writer = RawCaptureWriter::new(Vec::new());
    writer.append(draft(
        "polymarket:market",
        "pm-connection",
        2,
        0,
        RawCapturePayload::Control {
            name: "heartbeat".to_owned(),
            raw_bytes: Vec::new(),
        },
    ))?;
    writer.append(frame(b"after heartbeat", 1))?;

    let mut reader = RawCaptureReader::new(Cursor::new(writer.into_inner()));
    let heartbeat = reader.next_record()?.ok_or("missing heartbeat")?;
    assert_eq!(heartbeat.capture.normalized_projection, None);
    assert_eq!(
        heartbeat.capture.payload,
        RawCapturePayload::Control {
            name: "heartbeat".to_owned(),
            raw_bytes: Vec::new(),
        }
    );
    assert_eq!(
        reader
            .next_record()?
            .ok_or("missing frame after heartbeat")?
            .capture
            .transport
            .as_ref()
            .map(|transport| transport.frame_sequence),
        Some(1)
    );
    Ok(())
}

#[test]
fn requires_explicit_source_gaps_and_fails_closed_during_replay()
-> Result<(), Box<dyn std::error::Error>> {
    let mut writer = RawCaptureWriter::new(Vec::new());
    writer.append(frame(b"before-gap", 0))?;
    assert!(matches!(
        writer.append(frame(b"unmarked-loss", 2)),
        Err(RawCaptureError::SourceSequenceGap {
            expected: 1,
            found: 2,
            ..
        })
    ));
    writer.append(gap(1, 1, 2))?;
    writer.append(frame(b"after-gap", 3))?;

    let bytes = writer.into_inner();
    let mut audit = RawCaptureReader::new_audit(Cursor::new(bytes.clone()));
    assert_eq!(
        audit
            .next_record()?
            .ok_or("missing first frame")?
            .ingest_sequence,
        0
    );
    assert!(matches!(
        audit
            .next_record()?
            .ok_or("missing explicit gap")?
            .capture
            .payload,
        RawCapturePayload::Gap {
            range: Some(GapRange {
                domain: GapSequenceDomain::Transport,
                first_missing: 1,
                last_missing: 1,
            }),
            ..
        }
    ));
    assert_eq!(
        audit
            .next_record()?
            .ok_or("missing post-gap frame")?
            .capture
            .transport
            .as_ref()
            .map(|transport| transport.frame_sequence),
        Some(3)
    );
    assert!(audit.next_record()?.is_none());

    let mut reader = RawCaptureReader::new(Cursor::new(bytes));
    assert_eq!(
        reader
            .next_record()?
            .ok_or("missing first frame")?
            .ingest_sequence,
        0
    );
    assert!(matches!(
        reader.next_record(),
        Err(RawCaptureError::Gap {
            ingest_sequence: 1,
            range: Some(GapRange {
                domain: GapSequenceDomain::Transport,
                first_missing: 1,
                last_missing: 1,
            }),
            ..
        })
    ));
    assert!(matches!(
        reader.next_record(),
        Err(RawCaptureError::ReaderPoisoned)
    ));
    Ok(())
}

#[test]
fn rejects_checksum_changes_and_truncated_records_but_recovers_only_partial_tail()
-> Result<(), Box<dyn std::error::Error>> {
    let mut writer = RawCaptureWriter::new(Vec::new());
    writer.append(frame(b"original", 0))?;
    let complete = writer.into_inner();

    let mut changed: serde_json::Value = serde_json::from_slice(&complete)?;
    changed["body"]["capture"]["payload"]["raw_bytes"][0] = serde_json::json!(0);
    let mut changed = serde_json::to_vec(&changed)?;
    changed.push(b'\n');
    assert!(matches!(
        RawCaptureReader::new(Cursor::new(changed)).next_record(),
        Err(RawCaptureError::ChecksumMismatch { ingest_sequence: 0 })
    ));

    let mut crashed = complete.clone();
    crashed.extend_from_slice(b"{\"body\":");
    let recovery = recover_raw_capture_prefix(&crashed)?;
    assert_eq!(recovery.valid_bytes(), complete.len());
    assert_eq!(recovery.discarded_tail_bytes(), 8);
    assert_eq!(recovery.next_ingest_sequence(), 1);
    let mut reader = RawCaptureReader::new(Cursor::new(crashed));
    assert_eq!(
        reader
            .next_record()?
            .ok_or("missing complete frame")?
            .ingest_sequence,
        0
    );
    assert!(matches!(
        reader.next_record(),
        Err(RawCaptureError::IncompleteTail)
    ));

    let mut resumed = RawCaptureWriter::resume(complete, recovery);
    assert!(matches!(
        resumed.append(frame(b"must mark uncertainty", 1)),
        Err(RawCaptureError::RecoveryGapRequired)
    ));
    resumed.append(gap(1, 1, 2))?;
    assert_eq!(resumed.append(frame(b"after-recovery", 3))?, 2);
    let mut recovered_reader = RawCaptureReader::new(Cursor::new(resumed.into_inner()));
    assert_eq!(
        recovered_reader
            .next_record()?
            .ok_or("missing retained frame")?
            .ingest_sequence,
        0
    );
    assert!(matches!(
        recovered_reader.next_record(),
        Err(RawCaptureError::Gap { .. })
    ));
    Ok(())
}

#[test]
fn recovery_rejects_corruption_and_accepts_existing_gaps() -> Result<(), Box<dyn std::error::Error>>
{
    let mut writer = RawCaptureWriter::new(Vec::new());
    writer.append(frame(b"good", 0))?;
    let good = writer.into_inner();

    let mut corrupt: serde_json::Value = serde_json::from_slice(&good)?;
    corrupt["body"]["capture"]["payload"]["raw_bytes"][0] = serde_json::json!(0);
    let mut corrupt = serde_json::to_vec(&corrupt)?;
    corrupt.push(b'\n');
    assert!(matches!(
        recover_raw_capture_prefix(&corrupt),
        Err(RawCaptureError::ChecksumMismatch { .. })
    ));

    let mut with_gap = RawCaptureWriter::new(Vec::new());
    with_gap.append(frame(b"first", 0))?;
    with_gap.append(gap(1, 3, 4))?;
    let prefix = with_gap.into_inner();
    let recovery = recover_raw_capture_prefix(&prefix)?;
    assert_eq!(recovery.valid_bytes(), prefix.len());
    assert_eq!(recovery.next_ingest_sequence(), 2);

    let mut resumed = RawCaptureWriter::resume(prefix, recovery);
    assert_eq!(resumed.append(frame(b"after-report", 5))?, 2);
    let mut audit = RawCaptureReader::new_audit(Cursor::new(resumed.into_inner()));
    audit.next_record()?.ok_or("missing first frame")?;
    assert!(matches!(
        audit.next_record()?.ok_or("missing gap")?.capture.payload,
        RawCapturePayload::Gap { .. }
    ));
    assert_eq!(
        audit
            .next_record()?
            .ok_or("missing frame after recovered gap")?
            .capture
            .transport
            .as_ref()
            .map(|transport| transport.frame_sequence),
        Some(5)
    );
    Ok(())
}
