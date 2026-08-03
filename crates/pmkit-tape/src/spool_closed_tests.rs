use std::fs;

use crate::{RawSpoolWriter, SpoolChunk, SpoolError, SpoolFrame, enumerate_closed_chunks};

const MINUTE_MS: i64 = 1_700_000_040_000;
const DISCOVERY_DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn closed_chunk() -> Result<(tempfile::TempDir, SpoolChunk), SpoolError> {
    let root = tempfile::tempdir()?;
    let chunk = SpoolChunk::new("lane-a", "shard-0", MINUTE_MS)?;
    let mut writer = RawSpoolWriter::open(root.path(), chunk.clone())?;
    writer.append(&SpoolFrame::new(
        0,
        0,
        MINUTE_MS + 42,
        DISCOVERY_DIGEST,
        b"frame".to_vec(),
    ))?;
    writer.close()?;
    Ok((root, chunk))
}

#[test]
fn verified_chunk_rejects_changed_length_before_decoding() -> Result<(), Box<dyn std::error::Error>>
{
    let (root, chunk) = closed_chunk()?;
    let verified = enumerate_closed_chunks(root.path())?.remove(0);
    fs::write(chunk.closed_path(root.path()), b"different length\n")?;

    assert!(matches!(
        verified.records(),
        Err(SpoolError::ChecksumMismatch { .. })
    ));
    Ok(())
}

#[test]
fn verified_chunk_rejects_changed_bytes_at_the_original_length()
-> Result<(), Box<dyn std::error::Error>> {
    let (root, chunk) = closed_chunk()?;
    let verified = enumerate_closed_chunks(root.path())?.remove(0);
    let mut bytes = fs::read(chunk.closed_path(root.path()))?;
    let digest_offset = bytes
        .windows(b"aaaaa".len())
        .position(|window| window == b"aaaaa")
        .ok_or("digest marker missing")?;
    let digest = bytes
        .get_mut(digest_offset..digest_offset + b"aaaaa".len())
        .ok_or("digest range missing")?;
    digest.copy_from_slice(b"bbbbb");
    fs::write(chunk.closed_path(root.path()), bytes)?;

    assert!(matches!(
        verified.records(),
        Err(SpoolError::ChecksumMismatch { .. })
    ));
    Ok(())
}
