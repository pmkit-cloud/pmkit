use reqwest::Url;

const MAX_CACHE_BYTES: usize = 64 * 1024 * 1024;

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
    let bytes = response
        .bytes()
        .await
        .map_err(|_| CloudReplayError::Transport)?;
    if u64::try_from(bytes.len()).ok() != Some(segment.encoded_bytes)
        || digest(&bytes) != segment.encoded_sha256
    {
        return Err(CloudReplayError::IntegrityMismatch);
    }
    let bytes: std::sync::Arc<[u8]> = std::sync::Arc::from(bytes.as_ref());
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

pub(super) fn digest(bytes: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};

    format!("{:x}", Sha256::digest(bytes))
}
