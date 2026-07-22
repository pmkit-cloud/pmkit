use std::{
    io::{BufRead as _, Read as _},
    path::{Path, PathBuf},
};

use pmkit_event::CexReferenceEvent;
use pmkit_market::Asset;
use sha2::{Digest, Sha256};
use tokio::{io::AsyncWriteExt, task::spawn_blocking};

use super::{ArchiveKey, BinanceArchiveLimits};
use crate::{DataSourceError, parse_binance_vision_agg_trade_row};

const CHECKSUM_LIMIT: u64 = 4 * 1024;

#[derive(Debug)]
pub(super) struct ArchivePaths {
    pub(super) zip: PathBuf,
    pub(super) checksum: PathBuf,
}

impl ArchivePaths {
    pub(super) fn new(root: &Path, key: ArchiveKey) -> Self {
        let stem = key.filename().trim_end_matches(".zip").to_owned();
        Self {
            zip: root.join(format!("{stem}.zip")),
            checksum: root.join(format!("{stem}.zip.sha256")),
        }
    }
}

pub(super) async fn fetch_checksum(
    client: &reqwest::Client,
    base_url: &str,
    key: ArchiveKey,
) -> Result<String, DataSourceError> {
    let bytes = fetch_bytes(
        client,
        &format!("{base_url}/{}/{}.CHECKSUM", key.symbol(), key.filename()),
        CHECKSUM_LIMIT,
    )
    .await?;
    parse_checksum(&bytes)
}

pub(super) async fn download_verified(
    client: &reqwest::Client,
    root: &Path,
    base_url: &str,
    key: ArchiveKey,
    temp: &Path,
    expected: &str,
    limit: u64,
) -> Result<(), DataSourceError> {
    tokio::fs::create_dir_all(root).await.map_err(gap)?;
    let mut response = client
        .get(format!("{base_url}/{}/{}", key.symbol(), key.filename()))
        .send()
        .await
        .map_err(gap)?
        .error_for_status()
        .map_err(gap)?;
    let mut file = tokio::fs::File::create(temp).await.map_err(gap)?;
    let mut bytes = 0_u64;
    let mut hasher = Sha256::new();
    while let Some(chunk) = response.chunk().await.map_err(gap)? {
        bytes = bytes
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| gap("archive transfer size overflow"))?;
        if bytes > limit {
            let _ = tokio::fs::remove_file(temp).await;
            return Err(gap("archive transfer exceeds configured limit"));
        }
        hasher.update(&chunk);
        file.write_all(&chunk).await.map_err(gap)?;
    }
    file.flush().await.map_err(gap)?;
    if hex(&hasher.finalize()) != expected {
        let _ = tokio::fs::remove_file(temp).await;
        return Err(gap("archive SHA-256 mismatch"));
    }
    Ok(())
}

pub(super) async fn read_verified(
    paths: &ArchivePaths,
    asset: Asset,
    limits: BinanceArchiveLimits,
) -> Result<Option<Vec<CexReferenceEvent>>, DataSourceError> {
    let Some(checksum) = read_checksum(&paths.checksum).await? else {
        return Ok(None);
    };
    if !verify_file(paths.zip.clone(), checksum, limits.transfer_bytes).await? {
        return Ok(None);
    }
    parse_archive(paths.zip.clone(), asset, limits)
        .await
        .map(Some)
}

pub(super) async fn parse_archive(
    path: PathBuf,
    asset: Asset,
    limits: BinanceArchiveLimits,
) -> Result<Vec<CexReferenceEvent>, DataSourceError> {
    spawn_blocking(move || parse_archive_sync(&path, asset, limits))
        .await
        .map_err(gap)?
}

async fn fetch_bytes(
    client: &reqwest::Client,
    url: &str,
    limit: u64,
) -> Result<Vec<u8>, DataSourceError> {
    let mut response = client
        .get(url)
        .send()
        .await
        .map_err(gap)?
        .error_for_status()
        .map_err(gap)?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(gap)? {
        if (bytes.len() as u64)
            .checked_add(chunk.len() as u64)
            .is_none_or(|size| size > limit)
        {
            return Err(gap("response exceeds configured limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn read_checksum(path: &Path) -> Result<Option<String>, DataSourceError> {
    match tokio::fs::read(path).await {
        Ok(bytes) => parse_checksum(&bytes).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(gap(error)),
    }
}

async fn verify_file(path: PathBuf, expected: String, limit: u64) -> Result<bool, DataSourceError> {
    spawn_blocking(move || {
        let file = match std::fs::File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(gap(error)),
        };
        let mut reader = std::io::BufReader::new(file);
        let mut hasher = Sha256::new();
        let mut bytes = 0_u64;
        let mut buffer = [0_u8; 8192];
        loop {
            let read = reader.read(&mut buffer).map_err(gap)?;
            if read == 0 {
                break;
            }
            bytes = bytes
                .checked_add(read as u64)
                .ok_or_else(|| gap("archive size overflow"))?;
            if bytes > limit {
                return Ok(false);
            }
            hasher.update(&buffer[..read]);
        }
        Ok(hex(&hasher.finalize()) == expected)
    })
    .await
    .map_err(gap)?
}

fn parse_archive_sync(
    path: &Path,
    asset: Asset,
    limits: BinanceArchiveLimits,
) -> Result<Vec<CexReferenceEvent>, DataSourceError> {
    let file = std::fs::File::open(path).map_err(gap)?;
    let mut archive = zip::ZipArchive::new(file).map_err(gap)?;
    if archive.len() != 1 {
        return Err(gap("archive must contain exactly one CSV entry"));
    }
    let entry = archive.by_index(0).map_err(gap)?;
    if entry.size() > limits.zip_bytes {
        return Err(gap("archive entry exceeds ZIP limit"));
    }
    let read_limit = limits.zip_bytes.min(limits.csv_bytes).saturating_add(1);
    let mut reader = std::io::BufReader::new(entry.take(read_limit));
    let mut records = Vec::new();
    let mut line = String::new();
    let mut decoded_bytes = 0_u64;
    let mut csv_bytes = 0_u64;
    loop {
        line.clear();
        let read = reader.read_line(&mut line).map_err(gap)?;
        if read == 0 {
            break;
        }
        decoded_bytes = decoded_bytes
            .checked_add(read as u64)
            .ok_or_else(|| gap("ZIP size overflow"))?;
        if decoded_bytes > limits.zip_bytes {
            return Err(gap("archive entry exceeds ZIP limit"));
        }
        csv_bytes = csv_bytes
            .checked_add(read as u64)
            .ok_or_else(|| gap("CSV size overflow"))?;
        if csv_bytes > limits.csv_bytes {
            return Err(gap("CSV exceeds configured limit"));
        }
        records.push(parse_binance_vision_agg_trade_row(line.trim_end(), asset).map_err(gap)?);
    }
    Ok(records)
}

fn parse_checksum(bytes: &[u8]) -> Result<String, DataSourceError> {
    let value = std::str::from_utf8(bytes)
        .map_err(gap)?
        .split_whitespace()
        .next()
        .ok_or_else(|| gap("missing archive checksum"))?;
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(gap("invalid archive checksum"));
    }
    Ok(value.to_ascii_lowercase())
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .flat_map(|byte| {
            [
                b"0123456789abcdef"[(byte >> 4) as usize],
                b"0123456789abcdef"[(byte & 15) as usize],
            ]
        })
        .map(char::from)
        .collect()
}
pub(super) fn gap(error: impl std::fmt::Display) -> DataSourceError {
    DataSourceError::ReplayGap {
        message: error.to_string(),
    }
}
