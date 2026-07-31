use chrono::{NaiveDate, Utc};
use serde::Deserialize;
use serde_json::Value;

use crate::StoreError;

const UTC_DAY_MS: i64 = 86_400_000;
const SEALED_CLOSED_DAY_MANIFEST_VERSION: u16 = 2;

/// An immutable public-tape manifest that certifies a completed UTC day.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedClosedDayManifest {
    document: Value,
    schema_version: u16,
    closed_day: String,
    start_time_ms: i64,
    end_time_ms: i64,
}

impl SealedClosedDayManifest {
    /// Returns the sealed source document used for provenance hashing.
    #[must_use]
    pub const fn document(&self) -> &Value {
        &self.document
    }

    /// Returns the accepted manifest schema version.
    #[must_use]
    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }

    /// Returns the ISO-8601 UTC day certified by this manifest.
    #[must_use]
    pub fn closed_day(&self) -> &str {
        &self.closed_day
    }

    /// Returns whether a source timestamp belongs to the sealed UTC day.
    #[must_use]
    pub const fn contains(&self, timestamp_ms: i64) -> bool {
        timestamp_ms >= self.start_time_ms && timestamp_ms <= self.end_time_ms
    }
}

/// Decodes a sealed public-tape day manifest and its UTC bounds.
///
/// # Errors
///
/// Returns [`StoreError`] when the document is unsealed, malformed, or names an invalid day.
pub fn decode_sealed_closed_day_manifest(
    document: Value,
) -> Result<SealedClosedDayManifest, StoreError> {
    decode_sealed_closed_day_manifest_at(document, Utc::now().date_naive())
}

pub fn decode_sealed_closed_day_manifest_at(
    document: Value,
    utc_today: NaiveDate,
) -> Result<SealedClosedDayManifest, StoreError> {
    let ClosedDayManifestHeader {
        version: schema_version,
        day: closed_day,
        day_seal: DaySeal::Sealed,
    } = ClosedDayManifestHeader::deserialize(&document).map_err(|_| invalid_manifest())?;
    if schema_version != SEALED_CLOSED_DAY_MANIFEST_VERSION {
        return Err(invalid_manifest());
    }
    let day = NaiveDate::parse_from_str(&closed_day, "%Y-%m-%d").map_err(|_| invalid_manifest())?;
    if day.format("%Y-%m-%d").to_string() != closed_day {
        return Err(invalid_manifest());
    }
    if day >= utc_today {
        return Err(invalid_manifest());
    }
    let start_time_ms = day
        .and_hms_opt(0, 0, 0)
        .ok_or_else(invalid_manifest)?
        .and_utc()
        .timestamp_millis();
    let end_time_ms = start_time_ms
        .checked_add(UTC_DAY_MS - 1)
        .ok_or_else(invalid_manifest)?;
    Ok(SealedClosedDayManifest {
        document,
        schema_version,
        closed_day,
        start_time_ms,
        end_time_ms,
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClosedDayManifestHeader {
    version: u16,
    day: String,
    day_seal: DaySeal,
}

#[derive(Debug, Deserialize)]
enum DaySeal {
    #[serde(rename = "sealed")]
    Sealed,
}

fn invalid_manifest() -> StoreError {
    StoreError::Storage {
        message: "sealed closed-day manifest is invalid".into(),
    }
}
