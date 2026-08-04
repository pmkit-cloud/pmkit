use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The only portable market export schema this release can decode.
pub const PORTABLE_MARKET_EXPORT_VERSION: u16 = 1;

/// Typed coverage status for portable exports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PortableMarketCoverage {
    /// Every declared segment is backed by observed coverage.
    #[serde(rename = "observed")]
    Observed,
}

/// A normalized asset identifier in a portable market declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PortableAsset(pub String);

/// A market duration represented in whole seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PortableDurationSeconds(pub u64);

/// A portable, provider-neutral market export manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableMarketExport {
    /// Version of this portable manifest.
    pub schema_version: u16,
    /// Evidence coverage status.
    pub coverage: PortableMarketCoverage,
    /// SHA-256 of the sealed source manifest.
    pub source_manifest_sha256: String,
    /// Ordered immutable segment declarations.
    pub segments: Vec<PortableMarketSegment>,
    /// SHA-256 of this manifest excluding this field.
    pub artifact_sha256: String,
}

/// One immutable minute/market partition in a portable export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableMarketSegment {
    /// Stable declaration identity.
    pub segment_id: String,
    /// Recurring market family identity supplied by structured metadata.
    pub series_id: String,
    /// Optional typed asset identifier.
    pub asset: Option<PortableAsset>,
    /// Optional typed market duration in seconds.
    pub duration_seconds: Option<PortableDurationSeconds>,
    /// Concrete market instance identity.
    pub market_id: String,
    /// Optional discovery snapshot that scoped this market-minute evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_snapshot_sha256: Option<String>,
    /// Concrete condition identity.
    pub condition_id: String,
    /// Ordered outcome-to-token mappings.
    pub outcome_tokens: Vec<PortableOutcomeToken>,
    /// Inclusive market open timestamp in milliseconds.
    pub market_open_time_ms: i64,
    /// Inclusive market close timestamp in milliseconds.
    pub market_close_time_ms: i64,
    /// Inclusive UTC-minute partition start in milliseconds.
    pub partition_start_time_ms: i64,
    /// Inclusive UTC-minute partition end in milliseconds.
    pub partition_end_time_ms: i64,
    /// Stable zero-based ordinal when one market minute spans multiple segments.
    pub subpart_ordinal: u64,
    /// Inclusive first row timestamp in milliseconds.
    pub from_time_ms: i64,
    /// Inclusive last row timestamp in milliseconds.
    pub to_time_ms: i64,
    /// Legacy alias of `from_time_ms` when retained for existing callers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_ts_ms: Option<i64>,
    /// Legacy alias of `to_time_ms` when retained for existing callers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_ts_ms: Option<i64>,
    /// Number of newline-delimited rows.
    pub rows: u64,
    /// Logical artifact byte length.
    pub bytes: u64,
    /// SHA-256 of the logical artifact bytes.
    pub sha256: String,
    /// SHA-256 of the sealed source manifest.
    pub source_manifest_sha256: String,
    /// Stable relative key for the logical artifact.
    pub artifact_key: String,
}

/// One ordered outcome-to-token mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableOutcomeToken {
    /// Outcome label.
    pub outcome: String,
    /// Concrete outcome token identity.
    pub token_id: String,
}

/// Exact bytes for one portable segment declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortableMarketArtifact {
    /// Segment identity addressed by `bytes`.
    pub segment_id: String,
    /// Exact logical newline-delimited bytes.
    pub bytes: Vec<u8>,
}

/// Failure while decoding or validating a portable market export.
#[derive(Debug, Error)]
pub enum PortableMarketExportError {
    /// The manifest is not valid JSON for this contract.
    #[error("portable market export JSON is malformed")]
    MalformedJson,
    /// The manifest version is unsupported.
    #[error("portable market export version {schema_version} is unsupported")]
    UnsupportedSchemaVersion {
        /// Version supplied by the untrusted manifest.
        schema_version: u16,
    },
    /// A declaration violates required bounds or identifiers.
    #[error("portable market export declaration is malformed")]
    MalformedDeclaration,
    /// Segment identities must be unique.
    #[error("portable market export has a duplicate segment id")]
    DuplicateSegmentId,
    /// A declared checksum is not lowercase hexadecimal SHA-256.
    #[error("portable market export has an invalid SHA-256 digest")]
    InvalidDigest,
    /// The manifest self-digest differs from canonical content.
    #[error("portable market export self-digest does not match canonical content")]
    DigestMismatch,
    /// An artifact is absent, duplicated, or undeclared.
    #[error("portable market export artifact identities do not match declarations")]
    ArtifactIdentityMismatch,
    /// Artifact bytes differ from their declared length.
    #[error("portable market export artifact byte length does not match its declaration")]
    ArtifactLengthMismatch,
    /// Artifact bytes differ from their declared digest.
    #[error("portable market export artifact digest does not match its declaration")]
    ArtifactDigestMismatch,
}

/// Encodes a validated export as canonical compact JSON.
///
/// # Errors
///
/// Returns [`PortableMarketExportError`] when the manifest content is invalid.
pub fn encode_portable_market_export(
    export: &PortableMarketExport,
) -> Result<Vec<u8>, PortableMarketExportError> {
    validate_content(export)?;
    let content = encoded_content(export)?;
    let artifact_sha256 = crate::integrity::sha256_hex(&content);
    serde_json::to_vec(&EncodedExport::new(export, &artifact_sha256))
        .map_err(|_| PortableMarketExportError::MalformedJson)
}

/// Decodes and validates a canonical portable export manifest.
///
/// # Errors
///
/// Returns [`PortableMarketExportError`] when untrusted bytes violate the v1 contract.
pub fn decode_portable_market_export(
    bytes: &[u8],
) -> Result<PortableMarketExport, PortableMarketExportError> {
    let export = serde_json::from_slice::<PortableMarketExport>(bytes)
        .map_err(|_| PortableMarketExportError::MalformedJson)?;
    validate_content(&export)?;
    if !is_sha256(&export.artifact_sha256) {
        return Err(PortableMarketExportError::InvalidDigest);
    }
    if crate::integrity::sha256_hex(&encoded_content(&export)?) != export.artifact_sha256 {
        return Err(PortableMarketExportError::DigestMismatch);
    }
    Ok(export)
}

/// Verifies declared byte lengths and SHA-256 values for every artifact.
///
/// # Errors
///
/// Returns [`PortableMarketExportError`] when artifact identities, lengths, or digests differ.
pub fn validate_portable_market_export_artifacts(
    export: &PortableMarketExport,
    artifacts: &[PortableMarketArtifact],
) -> Result<(), PortableMarketExportError> {
    let declared = export
        .segments
        .iter()
        .map(|segment| (segment.segment_id.as_str(), segment))
        .collect::<BTreeMap<_, _>>();
    if declared.len() != artifacts.len() {
        return Err(PortableMarketExportError::ArtifactIdentityMismatch);
    }
    let mut seen = BTreeSet::new();
    for artifact in artifacts {
        let Some(segment) = declared.get(artifact.segment_id.as_str()) else {
            return Err(PortableMarketExportError::ArtifactIdentityMismatch);
        };
        if !seen.insert(artifact.segment_id.as_str()) {
            return Err(PortableMarketExportError::ArtifactIdentityMismatch);
        }
        if u64::try_from(artifact.bytes.len()).ok() != Some(segment.bytes) {
            return Err(PortableMarketExportError::ArtifactLengthMismatch);
        }
        if crate::integrity::sha256_hex(&artifact.bytes) != segment.sha256 {
            return Err(PortableMarketExportError::ArtifactDigestMismatch);
        }
    }
    Ok(())
}

fn validate_content(export: &PortableMarketExport) -> Result<(), PortableMarketExportError> {
    if export.schema_version != PORTABLE_MARKET_EXPORT_VERSION {
        return Err(PortableMarketExportError::UnsupportedSchemaVersion {
            schema_version: export.schema_version,
        });
    }
    if !is_sha256(&export.source_manifest_sha256) {
        return Err(PortableMarketExportError::InvalidDigest);
    }
    let mut segment_ids = BTreeSet::new();
    for segment in &export.segments {
        if !segment_ids.insert(segment.segment_id.as_str()) {
            return Err(PortableMarketExportError::DuplicateSegmentId);
        }
        if !is_sha256(&segment.sha256) || !is_sha256(&segment.source_manifest_sha256) {
            return Err(PortableMarketExportError::InvalidDigest);
        }
        if !valid_segment(segment) {
            return Err(PortableMarketExportError::MalformedDeclaration);
        }
    }
    Ok(())
}

fn valid_segment(segment: &PortableMarketSegment) -> bool {
    [
        &segment.segment_id,
        &segment.series_id,
        &segment.market_id,
        &segment.condition_id,
        &segment.artifact_key,
    ]
    .iter()
    .all(|value| !value.is_empty())
        && segment
            .duration_seconds
            .is_none_or(|duration| duration.0 > 0)
        && segment
            .discovery_snapshot_sha256
            .as_deref()
            .is_none_or(is_sha256)
        && segment.market_open_time_ms <= segment.market_close_time_ms
        && segment.partition_start_time_ms <= segment.partition_end_time_ms
        && segment.from_time_ms <= segment.to_time_ms
        && segment.from_time_ms >= segment.partition_start_time_ms
        && segment.to_time_ms <= segment.partition_end_time_ms
        && segment
            .from_ts_ms
            .is_none_or(|value| value == segment.from_time_ms)
        && segment
            .to_ts_ms
            .is_none_or(|value| value == segment.to_time_ms)
        && segment
            .outcome_tokens
            .iter()
            .all(|mapping| !mapping.outcome.is_empty() && !mapping.token_id.is_empty())
}

pub fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn encoded_content(export: &PortableMarketExport) -> Result<Vec<u8>, PortableMarketExportError> {
    serde_json::to_vec(&ExportContent::from(export))
        .map_err(|_| PortableMarketExportError::MalformedJson)
}

#[derive(Serialize)]
struct ExportContent<'a> {
    schema_version: u16,
    coverage: PortableMarketCoverage,
    source_manifest_sha256: &'a str,
    segments: &'a [PortableMarketSegment],
}

impl<'a> From<&'a PortableMarketExport> for ExportContent<'a> {
    fn from(export: &'a PortableMarketExport) -> Self {
        Self {
            schema_version: export.schema_version,
            coverage: export.coverage,
            source_manifest_sha256: &export.source_manifest_sha256,
            segments: &export.segments,
        }
    }
}

#[derive(Serialize)]
struct EncodedExport<'a> {
    schema_version: u16,
    coverage: PortableMarketCoverage,
    source_manifest_sha256: &'a str,
    segments: &'a [PortableMarketSegment],
    artifact_sha256: &'a str,
}

impl<'a> EncodedExport<'a> {
    fn new(export: &'a PortableMarketExport, artifact_sha256: &'a str) -> Self {
        Self {
            schema_version: export.schema_version,
            coverage: export.coverage,
            source_manifest_sha256: &export.source_manifest_sha256,
            segments: &export.segments,
            artifact_sha256,
        }
    }
}
