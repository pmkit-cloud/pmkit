use reqwest::Url;

pub(super) const MAX_CACHE_BYTES: usize = 64 * 1024 * 1024;
pub(super) const MAX_SEGMENT_BYTES: usize = MAX_CACHE_BYTES;

use super::{
    PmKitCloudDataSource,
    cloud_http::{Segment, request},
    cloud_types::CloudReplayError,
};

// The cache guard intentionally spans the request to prevent concurrent misses from
// double-counting metered transfer.
#[allow(clippy::significant_drop_tightening)]
pub(super) async fn encoded_segment(
    source: &PmKitCloudDataSource,
    segment: &Segment,
) -> Result<std::sync::Arc<[u8]>, CloudReplayError> {
    if segment.encoded_bytes > MAX_SEGMENT_BYTES as u64 {
        return Err(CloudReplayError::IntegrityMismatch);
    }
    let cache_key = format!(
        "{}:{}:{}",
        segment.release_id, segment.id, segment.encoded_sha256
    );
    // ponytail: one global lock prevents concurrent misses from double-counting transfer;
    // use per-key singleflight only if concurrent replay contention matters.
    let mut cache = source.cache.lock().await;
    if let Some(bytes) = cache.get(&cache_key).cloned() {
        return Ok(bytes);
    }
    let mut url =
        Url::parse(&source.base_url).map_err(|_| CloudReplayError::InvalidConfiguration)?;
    url.path_segments_mut()
        .map_err(|()| CloudReplayError::InvalidConfiguration)?
        .extend(["replay", "segments", &segment.id]);
    let response = request(source, url).await?;
    let header_encoded = response
        .headers()
        .get("x-pmkit-encoded-sha256")
        .and_then(|value| value.to_str().ok());
    let header_logical = response
        .headers()
        .get("x-pmkit-segment-sha256")
        .and_then(|value| value.to_str().ok());
    if header_encoded != Some(segment.encoded_sha256.as_str())
        || header_logical != Some(segment.sha256.as_str())
    {
        return Err(CloudReplayError::IntegrityMismatch);
    }
    let bytes = read_response_bounded(response, MAX_SEGMENT_BYTES).await?;
    if u64::try_from(bytes.len()).ok() != Some(segment.encoded_bytes)
        || digest(&bytes) != segment.encoded_sha256
    {
        return Err(CloudReplayError::IntegrityMismatch);
    }
    let bytes: std::sync::Arc<[u8]> = std::sync::Arc::from(bytes);
    if bytes.len() <= MAX_CACHE_BYTES {
        let cached_bytes = cache.values().map(|value| value.len()).sum::<usize>();
        if cached_bytes.saturating_add(bytes.len()) > MAX_CACHE_BYTES {
            // ponytail: purge-all eviction keeps this cache bounded without an LRU dependency.
            cache.clear();
        }
        cache.insert(cache_key, bytes.clone());
    }
    Ok(bytes)
}

pub(super) async fn read_response_bounded(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>, CloudReplayError> {
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err(CloudReplayError::IntegrityMismatch);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| CloudReplayError::Transport)?
    {
        append_bounded(&mut bytes, &chunk, max_bytes)?;
    }
    Ok(bytes)
}

pub(super) fn read_bounded<R: std::io::Read>(
    mut reader: R,
    max_bytes: usize,
) -> Result<Vec<u8>, CloudReplayError> {
    let mut bytes = Vec::new();
    let mut buffer = [0; 8192];
    loop {
        let remaining = max_bytes - bytes.len();
        let read_limit = buffer.len().min(remaining.saturating_add(1));
        let read = reader
            .read(&mut buffer[..read_limit])
            .map_err(|_| CloudReplayError::IntegrityMismatch)?;
        if read == 0 {
            return Ok(bytes);
        }
        if read > remaining {
            return Err(CloudReplayError::IntegrityMismatch);
        }
        append_bounded(&mut bytes, &buffer[..read], max_bytes)?;
    }
}

fn append_bounded(
    bytes: &mut Vec<u8>,
    chunk: &[u8],
    max_bytes: usize,
) -> Result<(), CloudReplayError> {
    let length = bytes
        .len()
        .checked_add(chunk.len())
        .ok_or(CloudReplayError::IntegrityMismatch)?;
    if length > max_bytes {
        return Err(CloudReplayError::IntegrityMismatch);
    }
    bytes
        .try_reserve_exact(chunk.len())
        .map_err(|_| CloudReplayError::MalformedResponse)?;
    bytes.extend_from_slice(chunk);
    Ok(())
}

pub(super) fn digest(bytes: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};

    format!("{:x}", Sha256::digest(bytes))
}
