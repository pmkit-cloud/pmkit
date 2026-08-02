use serde_json::json;

#[test]
fn portable_market_export_rolls_before_the_byte_limit() -> Result<(), Box<dyn std::error::Error>> {
    // Given: two deterministic rows that cannot share a bounded logical segment.
    let rows = vec![
        json!({"event_time_ms": 1_000, "row_ordinal": 0, "payload": {"price": "0.42"}}),
        json!({"event_time_ms": 1_001, "row_ordinal": 1, "payload": {"price": "0.43"}}),
    ];

    // When: the production row roller receives the bounded equivalent of 32 MiB.
    let first = super::roll_rows(&rows, 80)?;
    let second = super::roll_rows(&rows, 80)?;

    // Then: it rolls before the limit and addresses stable ordinal subparts.
    assert_eq!(first.len(), 2);
    assert_eq!(first, second);
    assert_eq!(
        super::segment_id(super::SegmentIdInput {
            source_manifest_sha256: "a",
            series_id: "btc-usd-5m",
            market_id: "market-01",
            minute_start: 0,
            subpart_ordinal: 0
        }),
        super::segment_id(super::SegmentIdInput {
            source_manifest_sha256: "a",
            series_id: "btc-usd-5m",
            market_id: "market-01",
            minute_start: 0,
            subpart_ordinal: 0
        })
    );
    assert_ne!(
        super::segment_id(super::SegmentIdInput {
            source_manifest_sha256: "a",
            series_id: "btc-usd-5m",
            market_id: "market-01",
            minute_start: 0,
            subpart_ordinal: 0
        }),
        super::segment_id(super::SegmentIdInput {
            source_manifest_sha256: "a",
            series_id: "btc-usd-5m",
            market_id: "market-01",
            minute_start: 0,
            subpart_ordinal: 1
        })
    );
    Ok(())
}
