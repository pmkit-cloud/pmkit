use std::{collections::HashMap, path::PathBuf, sync::Arc};

use chrono::NaiveDate;
use pmkit_event::CexReferenceEvent;
use pmkit_market::Asset;
use tokio::sync::Mutex;

use crate::DataSourceError;

#[path = "binance_cache_io.rs"]
mod binance_cache_io;
#[path = "binance_cache_replay.rs"]
mod binance_cache_replay;
#[path = "binance_cache_state.rs"]
mod binance_cache_state;

const VISION_BASE: &str = "https://data.binance.vision/data/spot/daily/aggTrades";

/// Retention policy for local official history archives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachePolicy {
    /// Retain verified archives up to a hard byte quota.
    Bounded {
        /// Maximum combined ZIP bytes retained by this cache.
        max_bytes: u64,
    },
}

impl CachePolicy {
    pub(super) const fn max_bytes(self) -> u64 {
        match self {
            Self::Bounded { max_bytes } => max_bytes,
        }
    }
}

/// Independent byte limits for archive retrieval and replay parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinanceArchiveLimits {
    /// Maximum compressed bytes accepted from the ZIP response.
    pub transfer_bytes: u64,
    /// Maximum decompressed ZIP-entry bytes read during validation.
    pub zip_bytes: u64,
    /// Maximum CSV bytes parsed into records.
    pub csv_bytes: u64,
}

impl Default for BinanceArchiveLimits {
    fn default() -> Self {
        Self {
            transfer_bytes: 256 * 1024 * 1024,
            zip_bytes: 512 * 1024 * 1024,
            csv_bytes: 512 * 1024 * 1024,
        }
    }
}

/// A local, integrity-checked cache of official Binance Vision `aggTrades` archives.
#[derive(Clone)]
pub struct VerifiedBinanceArchiveCache {
    pub(super) root: Arc<PathBuf>,
    pub(super) policy: CachePolicy,
    pub(super) limits: BinanceArchiveLimits,
    pub(super) client: reqwest::Client,
    pub(super) base_url: Arc<str>,
    key_locks: Arc<Mutex<HashMap<ArchiveKey, Arc<Mutex<()>>>>>,
    quota: Arc<Mutex<binance_cache_state::QuotaState>>,
}

impl std::fmt::Debug for VerifiedBinanceArchiveCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerifiedBinanceArchiveCache")
            .field("root", &self.root)
            .field("policy", &self.policy)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl VerifiedBinanceArchiveCache {
    /// Creates a bounded cache backed by the official Binance Vision endpoint.
    #[must_use]
    pub fn new(root: PathBuf, policy: CachePolicy) -> Self {
        Self::with_base_url(root, policy, BinanceArchiveLimits::default(), VISION_BASE)
    }

    #[cfg(test)]
    pub(super) fn new_for_test(
        root: PathBuf,
        policy: CachePolicy,
        limits: BinanceArchiveLimits,
        base_url: &str,
    ) -> Self {
        Self::with_base_url(root, policy, limits, base_url)
    }

    fn with_base_url(
        root: PathBuf,
        policy: CachePolicy,
        limits: BinanceArchiveLimits,
        base_url: &str,
    ) -> Self {
        Self {
            root: Arc::new(root),
            policy,
            limits,
            client: reqwest::Client::new(),
            base_url: Arc::from(base_url.trim_end_matches('/')),
            key_locks: Arc::new(Mutex::new(HashMap::new())),
            quota: Arc::new(Mutex::new(binance_cache_state::QuotaState::default())),
        }
    }

    /// Replays one verified official archive, downloading it when absent.
    ///
    /// # Errors
    ///
    /// Returns [`DataSourceError::ReplayGap`] when history is unavailable,
    /// corrupt, or exceeds a configured bound.
    pub async fn replay(
        &self,
        asset: Asset,
        date: NaiveDate,
    ) -> Result<Vec<CexReferenceEvent>, DataSourceError> {
        self.replay_key(ArchiveKey { asset, date }).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ArchiveKey {
    asset: Asset,
    date: NaiveDate,
}

impl ArchiveKey {
    pub(super) fn symbol(self) -> String {
        self.asset.binance_symbol().to_ascii_uppercase()
    }

    pub(super) fn filename(self) -> String {
        format!(
            "{}-aggTrades-{}.zip",
            self.symbol(),
            self.date.format("%Y-%m-%d")
        )
    }
}
