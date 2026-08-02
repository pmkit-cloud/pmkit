use serde_json::Value;

use crate::StoreError;

#[derive(Clone, Copy)]
pub(super) struct SegmentIdInput<'a> {
    pub(super) source_manifest_sha256: &'a str,
    pub(super) series_id: &'a str,
    pub(super) market_id: &'a str,
    pub(super) minute_start: i64,
    pub(super) subpart_ordinal: u64,
}

pub(super) fn metadata_is_valid(metadata: &super::MarketMetadata) -> bool {
    !metadata.series_id.is_empty()
        && !metadata.market_id.is_empty()
        && !metadata.condition_id.is_empty()
        && metadata
            .duration_seconds
            .is_none_or(|duration| duration > 0)
        && !metadata.outcome_tokens.is_empty()
        && metadata.open_time_ms <= metadata.close_time_ms
        && metadata
            .outcome_tokens
            .iter()
            .all(|mapping| !mapping.outcome.is_empty() && !mapping.token_id.is_empty())
}

pub(super) fn market_metadata(
    normalized: &Value,
    market_id: &str,
) -> Result<(super::MarketMetadata, bool), StoreError> {
    normalized.get("portable_market").map_or_else(
        || {
            Ok((
                super::MarketMetadata {
                    series_id: market_id.into(),
                    asset: None,
                    duration_seconds: None,
                    market_id: market_id.into(),
                    condition_id: market_id.into(),
                    outcome_tokens: Vec::new(),
                    open_time_ms: 0,
                    close_time_ms: i64::MAX,
                },
                true,
            ))
        },
        |value| {
            serde_json::from_value(value.clone())
                .map(|metadata| (metadata, false))
                .map_err(|_| super::storage_error("portable market metadata is malformed"))
        },
    )
}

pub(super) fn roll_rows(rows: &[Value], byte_limit: usize) -> Result<Vec<Vec<Value>>, StoreError> {
    let mut parts = Vec::new();
    let mut part = Vec::new();
    let mut part_bytes: usize = 0;
    for row in rows {
        let line = serde_json::to_vec(row)
            .map_err(|_| super::storage_error("portable row is not encodable"))?;
        let line_bytes = line
            .len()
            .checked_add(1)
            .ok_or_else(|| super::storage_error("portable row byte length is invalid"))?;
        if line_bytes > byte_limit {
            return Err(super::storage_error(
                "portable row exceeds the segment byte limit",
            ));
        }
        let next_part_bytes = part_bytes
            .checked_add(line_bytes)
            .ok_or_else(|| super::storage_error("portable segment byte length is invalid"))?;
        if !part.is_empty() && next_part_bytes > byte_limit {
            parts.push(part);
            part = Vec::new();
            part_bytes = 0;
        }
        part.push(row.clone());
        part_bytes = part_bytes
            .checked_add(line_bytes)
            .ok_or_else(|| super::storage_error("portable segment byte length is invalid"))?;
    }
    if !part.is_empty() {
        parts.push(part);
    }
    Ok(parts)
}

pub(super) fn encode_rows(rows: &[Value]) -> Result<Vec<u8>, StoreError> {
    rows.iter().try_fold(Vec::new(), |mut bytes, row| {
        let line = serde_json::to_vec(row)
            .map_err(|_| super::storage_error("portable row is not encodable"))?;
        bytes.extend(line);
        bytes.push(b'\n');
        Ok(bytes)
    })
}

pub(super) fn segment_id(input: SegmentIdInput<'_>) -> String {
    crate::integrity::sha256_hex(
        format!(
            "{}\n{}\n{}\n{}\n{}",
            input.source_manifest_sha256,
            input.series_id,
            input.market_id,
            input.minute_start,
            input.subpart_ordinal,
        )
        .as_bytes(),
    )
}
