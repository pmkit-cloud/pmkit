//! Public portable market export codec tests.

use pmkit_store::{
    PortableMarketArtifact, PortableMarketExportError, decode_portable_market_export,
    encode_portable_market_export, validate_portable_market_export_artifacts,
};
use serde_json::{Value, json};

const FIXTURE: &[u8] = include_bytes!("fixtures/portable-market-export-v1.json");

#[test]
fn portable_market_export_roundtrip_is_canonical() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a canonical portable export v1 fixture.
    let export = decode_portable_market_export(FIXTURE)?;

    // When: its public contract is encoded again.
    let encoded = encode_portable_market_export(&export)?;

    // Then: consumers receive the exact canonical fixture bytes.
    assert_eq!(encoded, FIXTURE.strip_suffix(b"\n").unwrap_or(FIXTURE));
    Ok(())
}

#[test]
fn portable_market_export_rejects_bad_digest() -> Result<(), Box<dyn std::error::Error>> {
    // Given: a valid fixture whose declared self-digest no longer matches its content.
    let mut invalid: Value = serde_json::from_slice(FIXTURE)?;
    invalid["coverage"] = json!("unobserved");
    let encoded = serde_json::to_vec(&invalid)?;

    // When: a consumer decodes the changed manifest.
    let result = decode_portable_market_export(&encoded);

    // Then: it rejects the stale digest instead of accepting altered coverage.
    assert!(result.is_err());
    Ok(())
}

fn fixture_value() -> Result<Value, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(FIXTURE)?)
}

#[test]
fn portable_market_export_rejects_unsupported_version() -> Result<(), Box<dyn std::error::Error>> {
    let mut invalid = fixture_value()?;
    invalid["schema_version"] = json!(2);
    assert!(decode_portable_market_export(&serde_json::to_vec(&invalid)?).is_err());
    Ok(())
}

#[test]
fn portable_market_export_rejects_non_observed_coverage() -> Result<(), Box<dyn std::error::Error>>
{
    let mut invalid = fixture_value()?;
    invalid["coverage"] = json!("inferred");
    assert!(decode_portable_market_export(&serde_json::to_vec(&invalid)?).is_err());
    Ok(())
}

#[test]
fn portable_market_export_rejects_malformed_bounds() -> Result<(), Box<dyn std::error::Error>> {
    let mut invalid = fixture_value()?;
    invalid["segments"][0]["partition_end_time_ms"] = json!(999);
    assert!(matches!(
        decode_portable_market_export(&serde_json::to_vec(&invalid)?),
        Err(PortableMarketExportError::MalformedDeclaration)
    ));
    Ok(())
}

#[test]
fn portable_market_export_rejects_invalid_declared_sha256() -> Result<(), Box<dyn std::error::Error>>
{
    let mut invalid = fixture_value()?;
    invalid["segments"][0]["sha256"] = json!("not-a-sha256");
    assert!(matches!(
        decode_portable_market_export(&serde_json::to_vec(&invalid)?),
        Err(PortableMarketExportError::InvalidDigest)
    ));
    Ok(())
}

#[test]
fn portable_market_export_rejects_duplicate_segment_ids() -> Result<(), Box<dyn std::error::Error>>
{
    let mut invalid = fixture_value()?;
    let duplicate = invalid["segments"][0].clone();
    invalid["segments"]
        .as_array_mut()
        .ok_or("segments array")?
        .push(duplicate);
    assert!(decode_portable_market_export(&serde_json::to_vec(&invalid)?).is_err());
    Ok(())
}

#[test]
fn portable_market_export_rejects_bad_artifact_length_and_digest()
-> Result<(), Box<dyn std::error::Error>> {
    let export = decode_portable_market_export(FIXTURE)?;
    let truncated = [PortableMarketArtifact {
        segment_id: "segment-01".into(),
        bytes: vec![b'x'],
    }];
    let altered = [PortableMarketArtifact {
        segment_id: "segment-01".into(),
        bytes: vec![b'x'; 68],
    }];
    assert!(validate_portable_market_export_artifacts(&export, &truncated).is_err());
    assert!(validate_portable_market_export_artifacts(&export, &altered).is_err());
    Ok(())
}
