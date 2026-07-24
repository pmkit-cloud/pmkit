use std::io::{self, Write};

use serde::Deserialize;
use serde_json::json;
use thiserror::Error;

/// Schema version for raw JSON-lines tape records.
pub const RAW_TAPE_SCHEMA_VERSION: u16 = 1;

/// One decoded version-1 raw tape record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTapeRecord {
    /// Local Unix receipt time in milliseconds.
    pub receipt_time_ms: i64,
    /// Identity of the connection that received the frame.
    pub connection_id: String,
    /// Exact UTF-8 text frame received from the source.
    pub raw: String,
}

/// Failure while decoding or recovering a raw tape.
#[derive(Debug, Error)]
pub enum RawTapeError {
    /// The final record has no newline and may be partially written.
    #[error("incomplete raw tape tail")]
    IncompleteTail,
    /// The record uses an unsupported schema version.
    #[error("unsupported raw tape schema version {found}")]
    UnsupportedSchemaVersion {
        /// Version found in the record; zero represents the legacy unversioned format.
        found: u16,
    },
    /// A complete record is malformed.
    #[error("malformed raw tape record: {message}")]
    Malformed {
        /// Decode detail.
        message: String,
    },
}

#[derive(Debug, Deserialize)]
struct WireRawTapeRecord {
    schema_version: Option<u16>,
    receipt_time_ms: Option<i64>,
    connection_id: Option<String>,
    raw: Option<String>,
}

/// Decodes one newline-terminated raw tape record.
///
/// # Errors
///
/// Returns a typed error for incomplete, unsupported, or malformed records.
pub fn decode_raw_record(line: &[u8]) -> Result<RawTapeRecord, RawTapeError> {
    if !line.ends_with(b"\n") {
        return Err(RawTapeError::IncompleteTail);
    }
    let wire: WireRawTapeRecord =
        serde_json::from_slice(line).map_err(|error| RawTapeError::Malformed {
            message: error.to_string(),
        })?;
    let found = wire.schema_version.unwrap_or(0);
    if found != RAW_TAPE_SCHEMA_VERSION {
        return Err(RawTapeError::UnsupportedSchemaVersion { found });
    }
    Ok(RawTapeRecord {
        receipt_time_ms: wire
            .receipt_time_ms
            .ok_or_else(|| RawTapeError::Malformed {
                message: "missing receipt_time_ms".to_owned(),
            })?,
        connection_id: wire.connection_id.ok_or_else(|| RawTapeError::Malformed {
            message: "missing connection_id".to_owned(),
        })?,
        raw: wire.raw.ok_or_else(|| RawTapeError::Malformed {
            message: "missing raw".to_owned(),
        })?,
    })
}

/// Returns the validated byte prefix safe to retain after a crash.
///
/// # Errors
///
/// Returns a typed error when any complete record is corrupt or unsupported.
pub fn recoverable_raw_tape_prefix(bytes: &[u8]) -> Result<usize, RawTapeError> {
    let mut offset = 0;
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        if !line.ends_with(b"\n") {
            break;
        }
        decode_raw_record(line)?;
        offset += line.len();
    }
    Ok(offset)
}

/// A sink for preserving raw text frames before venue adaptation.
pub trait RawTapeSink {
    /// Appends one received frame.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying writer rejects the frame.
    fn append_raw(
        &mut self,
        receipt_time_ms: i64,
        connection_id: &str,
        raw: &str,
    ) -> io::Result<()>;

    /// Flushes buffered data to the underlying writer.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying writer cannot be flushed.
    fn flush(&mut self) -> io::Result<()>;
}

/// A lossless JSON-lines recorder for UTF-8 WebSocket text frames.
#[derive(Debug)]
pub struct RawJsonLinesTape<W: Write> {
    writer: W,
}

impl<W: Write> RawJsonLinesTape<W> {
    /// Creates a raw tape over `writer`.
    pub const fn new(writer: W) -> Self {
        Self { writer }
    }

    /// Consumes the tape and returns the underlying writer.
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W: Write> RawTapeSink for RawJsonLinesTape<W> {
    fn append_raw(
        &mut self,
        receipt_time_ms: i64,
        connection_id: &str,
        raw: &str,
    ) -> io::Result<()> {
        serde_json::to_writer(
            &mut self.writer,
            &json!({
                "schema_version": RAW_TAPE_SCHEMA_VERSION,
                "receipt_time_ms": receipt_time_ms,
                "connection_id": connection_id,
                "raw": raw,
            }),
        )
        .map_err(io::Error::other)?;
        self.writer.write_all(b"\n")
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};

    use super::{
        RawJsonLinesTape, RawTapeError, RawTapeSink, decode_raw_record, recoverable_raw_tape_prefix,
    };

    #[derive(Debug, Default)]
    struct FlushWriter {
        bytes: Vec<u8>,
        flushed: bool,
    }

    impl Write for FlushWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flushed = true;
            Ok(())
        }
    }

    #[test]
    fn records_versioned_raw_frame_as_one_complete_line() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut tape = RawJsonLinesTape::new(Vec::new());
        tape.append_raw(42, "connection-1", r#"{"event_type":"book"}"#)?;
        tape.flush()?;

        let bytes = tape.into_inner();
        assert!(bytes.ends_with(b"\n"));
        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["receipt_time_ms"], 42);
        assert_eq!(value["connection_id"], "connection-1");
        assert_eq!(value["raw"], r#"{"event_type":"book"}"#);
        assert!(value.get("received_ms").is_none());
        Ok(())
    }

    #[test]
    fn rejects_legacy_unversioned_record() {
        let legacy = br#"{"received_ms":42,"connection_id":"old","raw":"{}"}
"#;

        assert!(matches!(
            decode_raw_record(legacy),
            Err(RawTapeError::UnsupportedSchemaVersion { found: 0 })
        ));
    }

    #[test]
    fn crash_recovery_discards_only_incomplete_tail() -> Result<(), RawTapeError> {
        let complete =
            br#"{"schema_version":1,"receipt_time_ms":42,"connection_id":"one","raw":"{}"}
"#;
        let mut crashed = complete.to_vec();
        crashed.extend_from_slice(br#"{"schema_version":1"#);

        assert_eq!(recoverable_raw_tape_prefix(&crashed)?, complete.len());
        Ok(())
    }

    #[test]
    fn crash_recovery_rejects_malformed_complete_record() {
        assert!(matches!(
            recoverable_raw_tape_prefix(b"not-json\n"),
            Err(RawTapeError::Malformed { .. })
        ));
    }

    #[test]
    fn flush_reaches_underlying_writer() -> io::Result<()> {
        let mut tape = RawJsonLinesTape::new(FlushWriter::default());
        tape.flush()?;

        assert!(tape.into_inner().flushed);
        Ok(())
    }
}
