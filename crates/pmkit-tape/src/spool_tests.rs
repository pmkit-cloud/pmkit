use std::fs::{self, OpenOptions};
use std::io::Write;

use sha2::{Digest, Sha256};

use crate::{
    RawSpoolWriter, RecoveryUncertainty, SpoolChunk, SpoolError, SpoolFrame,
    enumerate_closed_chunks, recover_open_chunk,
};

const MINUTE_MS: i64 = 1_700_000_040_000;
const DISCOVERY_DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn chunk(replica: &str, shard: &str) -> Result<SpoolChunk, SpoolError> {
    SpoolChunk::new(replica, shard, MINUTE_MS)
}

fn frame(epoch: u64, sequence: u64, raw_bytes: &[u8]) -> SpoolFrame {
    SpoolFrame::new(
        epoch,
        sequence,
        MINUTE_MS + 42,
        DISCOVERY_DIGEST,
        raw_bytes.to_vec(),
    )
}

#[test]
fn preserves_raw_bytes_and_lane_identity_without_key_collision()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let lane_a = chunk("lane-a", "shard-0")?;
    let lane_b = chunk("lane-b", "shard-0")?;
    let second_shard = chunk("lane-a", "shard-1")?;

    for chunk in [&lane_a, &lane_b, &second_shard] {
        let mut writer = RawSpoolWriter::open(root.path(), chunk.clone())?;
        writer.append(&frame(0, 7, b"\x00same\xff"))?;
        writer.append(&frame(0, 8, b"\x00same\xff"))?;
        writer.close()?;
    }

    let chunks = enumerate_closed_chunks(root.path())?;
    assert_eq!(chunks.len(), 3);
    assert_ne!(
        lane_a.closed_path(root.path()),
        lane_b.closed_path(root.path())
    );
    assert_ne!(
        lane_a.closed_path(root.path()),
        second_shard.closed_path(root.path())
    );
    for chunk in chunks {
        let records = chunk.records()?;
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].raw_bytes, b"\x00same\xff");
        assert_eq!(records[1].raw_bytes, b"\x00same\xff");
    }
    Ok(())
}

#[test]
fn reconnect_records_a_new_epoch() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let chunk = chunk("lane-a", "shard-0")?;
    let mut writer = RawSpoolWriter::open(root.path(), chunk)?;
    writer.append(&frame(0, 0, b"first"))?;
    writer.append(&frame(1, 0, b"after-reconnect"))?;
    writer.close()?;

    let records = enumerate_closed_chunks(root.path())?.remove(0).records()?;
    assert_eq!(records[0].connection_epoch, 0);
    assert_eq!(records[1].connection_epoch, 1);
    assert_eq!(records[1].frame_sequence, 0);
    Ok(())
}

#[test]
fn recovers_interrupted_final_line_with_typed_uncertainty() -> Result<(), Box<dyn std::error::Error>>
{
    let root = tempfile::tempdir()?;
    let chunk = chunk("lane-a", "shard-0")?;
    let mut writer = RawSpoolWriter::open(root.path(), chunk.clone())?;
    writer.append(&frame(0, 0, b"complete"))?;
    drop(writer);
    let complete = fs::read(chunk.open_path(root.path()))?;
    let mut open = OpenOptions::new()
        .append(true)
        .open(chunk.open_path(root.path()))?;
    open.write_all(b"{\"schema_version\":1")?;
    drop(open);

    let recovery = recover_open_chunk(&chunk.open_path(root.path()))?;
    assert_eq!(recovery.valid_bytes, u64::try_from(complete.len())?);
    assert!(matches!(
        recovery.uncertainty,
        Some(RecoveryUncertainty::InterruptedFinalLine { .. })
    ));
    assert_eq!(fs::read(chunk.open_path(root.path()))?, complete);
    Ok(())
}

#[test]
fn close_is_atomic_and_enumerates_only_verified_chunks() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let chunk = chunk("lane-a", "shard-0")?;
    let mut writer = RawSpoolWriter::open(root.path(), chunk.clone())?;
    writer.append(&frame(0, 0, b"frame"))?;

    assert!(!chunk.closed_path(root.path()).exists());
    assert!(enumerate_closed_chunks(root.path())?.is_empty());
    writer.close()?;

    assert!(!chunk.open_path(root.path()).exists());
    assert!(chunk.closed_path(root.path()).exists());
    assert!(chunk.checksum_path(root.path()).exists());
    assert_eq!(enumerate_closed_chunks(root.path())?.len(), 1);
    Ok(())
}

#[test]
fn checksum_sidecar_rejects_mutated_closed_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let chunk = chunk("lane-a", "shard-0")?;
    let mut writer = RawSpoolWriter::open(root.path(), chunk.clone())?;
    writer.append(&frame(0, 0, b"frame"))?;
    writer.close()?;
    fs::write(chunk.closed_path(root.path()), b"tampered\n")?;

    assert!(matches!(
        enumerate_closed_chunks(root.path()),
        Err(SpoolError::ChecksumMismatch { .. })
    ));
    Ok(())
}

#[test]
fn checksum_sidecar_is_the_digest_of_exact_closed_bytes() -> Result<(), Box<dyn std::error::Error>>
{
    let root = tempfile::tempdir()?;
    let chunk = chunk("lane-a", "shard-0")?;
    let mut writer = RawSpoolWriter::open(root.path(), chunk.clone())?;
    writer.append(&frame(0, 0, b"frame"))?;
    writer.close()?;

    let bytes = fs::read(chunk.closed_path(root.path()))?;
    let sidecar: serde_json::Value =
        serde_json::from_slice(&fs::read(chunk.checksum_path(root.path()))?)?;

    assert_eq!(
        sidecar["sha256_hex"],
        format!("{:x}", Sha256::digest(bytes))
    );
    Ok(())
}

#[test]
fn closing_never_replaces_a_conflicting_closed_chunk_or_sidecar()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let chunk = chunk("lane-a", "shard-0")?;
    let mut writer = RawSpoolWriter::open(root.path(), chunk.clone())?;
    writer.append(&frame(0, 0, b"frame"))?;
    let closed = chunk.closed_path(root.path());
    let sidecar = chunk.checksum_path(root.path());
    let original_closed = b"prior immutable bytes\n";
    let original_sidecar = b"prior immutable sidecar\n";
    fs::write(&closed, original_closed)?;
    fs::write(&sidecar, original_sidecar)?;

    assert!(matches!(
        writer.close(),
        Err(SpoolError::ClosedChunkExists { .. })
    ));
    assert_eq!(fs::read(&closed)?, original_closed);
    assert_eq!(fs::read(&sidecar)?, original_sidecar);
    Ok(())
}

#[test]
fn checkpoint_identity_comes_only_from_a_verified_closed_chunk()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let chunk = chunk("lane-a", "shard-0")?;
    let mut writer = RawSpoolWriter::open(root.path(), chunk.clone())?;
    writer.append(&frame(0, 0, b"frame"))?;
    writer.close()?;

    let checkpoint = enumerate_closed_chunks(root.path())?.remove(0).checkpoint();
    assert_eq!(checkpoint.chunk(), &chunk);
    assert_eq!(
        checkpoint.byte_length(),
        fs::metadata(checkpoint.path())?.len()
    );
    assert_eq!(checkpoint.sha256_hex().len(), 64);
    Ok(())
}

#[test]
fn reopening_a_closed_chunk_never_rewrites_prior_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let chunk = chunk("lane-a", "shard-0")?;
    let mut writer = RawSpoolWriter::open(root.path(), chunk.clone())?;
    writer.append(&frame(0, 0, b"original"))?;
    writer.close()?;
    let before = fs::read(chunk.closed_path(root.path()))?;

    assert!(matches!(
        RawSpoolWriter::open(root.path(), chunk.clone()),
        Err(SpoolError::ClosedChunkExists { .. })
    ));
    assert_eq!(fs::read(chunk.closed_path(root.path()))?, before);
    Ok(())
}

#[test]
fn reopening_an_open_chunk_appends_without_rewriting_valid_prefix()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let chunk = chunk("lane-a", "shard-0")?;
    let mut writer = RawSpoolWriter::open(root.path(), chunk.clone())?;
    writer.append(&frame(0, 0, b"first"))?;
    drop(writer);
    let prefix = fs::read(chunk.open_path(root.path()))?;
    fs::write(chunk.checksum_path(root.path()), b"stale")?;

    let mut reopened = RawSpoolWriter::open(root.path(), chunk.clone())?;
    reopened.append(&frame(0, 1, b"second"))?;
    reopened.close()?;

    let closed = fs::read(chunk.closed_path(root.path()))?;
    assert!(closed.starts_with(&prefix));
    assert_eq!(
        enumerate_closed_chunks(root.path())?
            .remove(0)
            .records()?
            .len(),
        2
    );
    Ok(())
}

#[test]
fn recovery_rejects_malformed_complete_lines() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let chunk = chunk("lane-a", "shard-0")?;
    fs::create_dir_all(
        chunk
            .open_path(root.path())
            .parent()
            .ok_or("missing parent")?,
    )?;
    fs::write(chunk.open_path(root.path()), b"not-json\n")?;

    assert!(matches!(
        recover_open_chunk(&chunk.open_path(root.path())),
        Err(SpoolError::MalformedRecord { .. })
    ));
    Ok(())
}
