use std::{collections::HashMap, sync::Arc};

use reqwest::Url;

pub(super) const MAX_CACHE_BYTES: usize = 64 * 1024 * 1024;
pub(super) const MAX_SEGMENT_BYTES: usize = MAX_CACHE_BYTES;
// Retain at most one replay's segment cardinality, even when payloads are tiny.
const MAX_CACHE_ENTRIES: usize = super::MAX_REPLAY_SEGMENTS;
// Covers map storage and allocation overhead in addition to key capacity and payload bytes.
const CACHE_ENTRY_OVERHEAD_BYTES: usize = 128;

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
    let bytes: Arc<[u8]> = Arc::from(bytes);
    insert_cached(&mut cache, cache_key, bytes.clone());
    Ok(bytes)
}

fn insert_cached(cache: &mut HashMap<String, Arc<[u8]>>, key: String, bytes: Arc<[u8]>) {
    insert_cached_with_limit(cache, key, bytes, MAX_CACHE_BYTES);
}

fn insert_cached_with_limit(
    cache: &mut HashMap<String, Arc<[u8]>>,
    key: String,
    bytes: Arc<[u8]>,
    max_cache_bytes: usize,
) {
    let entry_bytes = cache_entry_bytes(key.capacity(), bytes.len());
    if entry_bytes > max_cache_bytes {
        return;
    }
    let cached_bytes = cache.iter().fold(0usize, |total, (key, bytes)| {
        total.saturating_add(cache_entry_bytes(key.capacity(), bytes.len()))
    });
    if cache.len() >= MAX_CACHE_ENTRIES
        || cached_bytes.saturating_add(entry_bytes) > max_cache_bytes
    {
        // ponytail: purge-all eviction keeps this cache bounded without an LRU dependency.
        cache.clear();
    }
    cache.insert(key, bytes);
}

const fn cache_entry_bytes(key_capacity: usize, payload_bytes: usize) -> usize {
    key_capacity
        .saturating_add(payload_bytes)
        .saturating_add(CACHE_ENTRY_OVERHEAD_BYTES)
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

#[cfg(test)]
mod tests {
    use super::{
        CACHE_ENTRY_OVERHEAD_BYTES, MAX_CACHE_ENTRIES, cache_entry_bytes, insert_cached,
        insert_cached_with_limit,
    };
    use std::{collections::HashMap, sync::Arc};

    #[test]
    fn byte_budget_evicts_based_on_key_capacity() {
        let mut cache: HashMap<String, Arc<[u8]>> = HashMap::new();
        let first_key = "a".repeat(512);
        let second_key = "b".repeat(512);
        let max_key_capacity = first_key.capacity().max(second_key.capacity());
        // Each key fits, but the sum of their capacities does not. Payload-only accounting
        // would incorrectly retain both tiny entries under this budget.
        let byte_limit = max_key_capacity + 256;
        let payload: Arc<[u8]> = Arc::from([0_u8].as_slice());

        insert_cached_with_limit(&mut cache, first_key.clone(), payload.clone(), byte_limit);
        insert_cached_with_limit(&mut cache, second_key.clone(), payload, byte_limit);

        assert_eq!(cache.len(), 1);
        assert!(!cache.contains_key(&first_key));
        assert!(cache.contains_key(&second_key));
    }

    #[test]
    fn byte_budget_evicts_based_on_per_entry_overhead() {
        let mut cache: HashMap<String, Arc<[u8]>> = HashMap::new();
        let first_key = String::from("a");
        let second_key = String::from("b");
        let max_key_capacity = first_key.capacity().max(second_key.capacity());
        // Fixed per-entry overhead pushes both otherwise tiny entries over the budget.
        let byte_limit = max_key_capacity + 256;
        let payload: Arc<[u8]> = Arc::from([0_u8].as_slice());

        insert_cached_with_limit(&mut cache, first_key.clone(), payload.clone(), byte_limit);
        insert_cached_with_limit(&mut cache, second_key.clone(), payload, byte_limit);

        assert_eq!(cache.len(), 1);
        assert!(!cache.contains_key(&first_key));
        assert!(cache.contains_key(&second_key));
    }

    #[test]
    fn over_budget_entry_is_not_cached_or_evicting_existing_entries() {
        let mut cache: HashMap<String, Arc<[u8]>> = HashMap::new();
        let byte_limit = 256;
        let kept_key = String::from("keep");
        insert_cached_with_limit(
            &mut cache,
            kept_key.clone(),
            Arc::from([0_u8].as_slice()),
            byte_limit,
        );
        assert!(cache.contains_key(&kept_key));

        let oversized_key = "x".repeat(byte_limit);
        insert_cached_with_limit(
            &mut cache,
            oversized_key.clone(),
            Arc::from([1_u8].as_slice()),
            byte_limit,
        );

        assert_eq!(cache.len(), 1);
        assert!(cache.contains_key(&kept_key));
        assert!(!cache.contains_key(&oversized_key));
    }

    #[test]
    fn over_budget_payload_is_not_cached_or_evicting_existing_entries() {
        let mut cache: HashMap<String, Arc<[u8]>> = HashMap::new();
        let byte_limit = 256;
        let kept_key = String::from("keep");
        insert_cached_with_limit(
            &mut cache,
            kept_key.clone(),
            Arc::from([0_u8].as_slice()),
            byte_limit,
        );
        assert!(cache.contains_key(&kept_key));

        let oversized_key = String::from("large-payload");
        let oversized_payload = Arc::<[u8]>::from(vec![1_u8; byte_limit]);
        insert_cached_with_limit(
            &mut cache,
            oversized_key.clone(),
            oversized_payload,
            byte_limit,
        );

        assert_eq!(cache.len(), 1);
        assert!(cache.contains_key(&kept_key));
        assert!(!cache.contains_key(&oversized_key));
    }

    #[test]
    fn many_tiny_unique_keys_hit_the_cache_entry_cap() {
        let mut cache: HashMap<String, Arc<[u8]>> = HashMap::with_capacity(MAX_CACHE_ENTRIES);
        for index in 0..MAX_CACHE_ENTRIES {
            cache.insert(
                format!("release:segment-{index}:digest"),
                Arc::from([0_u8].as_slice()),
            );
        }

        let payload_bytes = cache.values().map(|bytes| bytes.len()).sum::<usize>();
        let accounted_bytes = cache.iter().fold(0usize, |total, (key, bytes)| {
            total.saturating_add(cache_entry_bytes(key.capacity(), bytes.len()))
        });
        assert_eq!(payload_bytes, MAX_CACHE_ENTRIES);
        assert!(accounted_bytes >= payload_bytes + MAX_CACHE_ENTRIES * CACHE_ENTRY_OVERHEAD_BYTES);

        insert_cached(
            &mut cache,
            "release:new:digest".to_owned(),
            Arc::from([1_u8].as_slice()),
        );
        assert_eq!(cache.len(), 1);
        assert_eq!(
            cache.get("release:new:digest").map(AsRef::as_ref),
            Some(&[1][..])
        );
    }
}
