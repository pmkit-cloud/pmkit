use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use crate::spool_record::decode_record;
use crate::{RecoveryUncertainty, SpoolError, TailRecovery};

/// Truncates only an unterminated final line after validating every complete record.
///
/// # Errors
///
/// Returns [`SpoolError`] when the file cannot be read or truncated, or a complete line is invalid.
pub fn recover_open_chunk(path: &Path) -> Result<TailRecovery, SpoolError> {
    let bytes = fs::read(path)?;
    let valid_bytes = validated_prefix(&bytes)?;
    let valid_bytes_u64 = u64::try_from(valid_bytes).map_err(|_| SpoolError::MalformedRecord {
        message: "spool prefix exceeds u64".to_owned(),
    })?;
    if valid_bytes == bytes.len() {
        return Ok(TailRecovery {
            valid_bytes: valid_bytes_u64,
            uncertainty: None,
        });
    }
    let mut file = OpenOptions::new().write(true).open(path)?;
    file.set_len(valid_bytes_u64)?;
    file.flush()?;
    file.sync_all()?;
    Ok(TailRecovery {
        valid_bytes: valid_bytes_u64,
        uncertainty: Some(RecoveryUncertainty::InterruptedFinalLine {
            discarded_bytes: u64::try_from(bytes.len() - valid_bytes).map_err(|_| {
                SpoolError::MalformedRecord {
                    message: "spool tail exceeds u64".to_owned(),
                }
            })?,
        }),
    })
}

fn validated_prefix(bytes: &[u8]) -> Result<usize, SpoolError> {
    let mut offset = 0;
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        if !line.ends_with(b"\n") {
            break;
        }
        decode_record(line)?;
        offset += line.len();
    }
    Ok(offset)
}
