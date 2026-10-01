//! Contract tests for exact oracle reference event capture.

use pmkit_event::{
    CanonicalSourceKey, CexReferenceEnvelope, CexReferenceEvent, SourceEnvelope, StrategyFact,
    StreamMetadata,
};
use pmkit_market::Asset;
use rust_decimal::Decimal;

fn metadata() -> StreamMetadata {
    StreamMetadata {
        schema_version: 1,
        source_id: "polybolt:oracle".into(),
        source_time_ms: 1_735_689_600_123,
        canonical_source_rank: 2,
        receipt_time_ms: 1_735_689_600_130,
        connection_id: "oracle-connection-3".into(),
        connection_epoch: 3,
        frame_sequence: 42,
        ingest_sequence: 42,
    }
}

#[test]
fn oracle_snapshot_and_update_keep_exact_values_raw_frames_and_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let price: Decimal = "123.4567890123456789012345678".parse()?;
    let snapshot = CexReferenceEvent::Twap60sSnapshot {
        asset: Asset::Btc,
        price,
        timestamp_ms: 1_735_689_600_123,
    };
    let update = CexReferenceEvent::Twap60sUpdate {
        asset: Asset::Btc,
        price,
        timestamp_ms: 1_735_689_600_124,
    };
    assert_ne!(snapshot, update);

    let source = SourceEnvelope::CexReference(CexReferenceEnvelope {
        metadata: metadata(),
        raw_frame: br#"{"kind":"twap_update","value":"123.4567890123456789012345678"}"#.to_vec(),
        fact: update.clone(),
    });

    assert!(matches!(
        source.canonical_key(),
        CanonicalSourceKey::CexReference {
            source_timestamp_ms: 1_735_689_600_123,
            canonical_source_rank: 2,
            ref source_id,
            connection_epoch: 3,
            frame_sequence: 42,
        } if source_id == "polybolt:oracle"
    ));
    let SourceEnvelope::CexReference(envelope) = source else {
        return Err("expected CEX reference envelope".into());
    };
    assert_eq!(
        envelope.raw_frame,
        br#"{"kind":"twap_update","value":"123.4567890123456789012345678"}"#
    );
    assert_eq!(envelope.metadata.receipt_time_ms, 1_735_689_600_130);
    assert_eq!(envelope.metadata.source_time_ms, 1_735_689_600_123);
    assert_eq!(envelope.metadata.source_id, "polybolt:oracle");
    assert_eq!(envelope.metadata.connection_id, "oracle-connection-3");
    assert_eq!(envelope.metadata.connection_epoch, 3);
    assert_eq!(envelope.metadata.frame_sequence, 42);
    assert_eq!(envelope.metadata.ingest_sequence, 42);
    assert_eq!(
        match &envelope.fact {
            CexReferenceEvent::Twap60sUpdate { price, .. } => price.to_string(),
            _ => return Err("expected 60-second TWAP update".into()),
        },
        "123.4567890123456789012345678"
    );
    assert_eq!(
        SourceEnvelope::CexReference(envelope).into_strategy_fact(),
        StrategyFact::Reference(update)
    );

    Ok(())
}
