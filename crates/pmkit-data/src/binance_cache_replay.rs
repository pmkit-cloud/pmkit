use tokio::task::spawn_blocking;

use super::{
    ArchiveKey, VerifiedBinanceArchiveCache,
    binance_cache_io::{ArchivePaths, download_verified, parse_archive, read_verified},
};
use crate::DataSourceError;

impl VerifiedBinanceArchiveCache {
    pub(super) async fn replay_key(
        &self,
        key: ArchiveKey,
    ) -> Result<Vec<pmkit_event::CexReferenceEvent>, DataSourceError> {
        let lock = self.lock_key(key).await?;
        let _guard = lock;
        let paths = self.paths(key);

        self.remove_stale_part(&paths).await;

        if let Some(records) = read_verified(&paths, key.asset, self.limits).await? {
            self.quota.lock().await.touch(&paths.zip);
            return Ok(records);
        }

        let checksum =
            super::binance_cache_io::fetch_checksum(&self.client, &self.base_url, key).await?;
        let temp = paths.zip.with_extension("zip.part");
        download_verified(
            &self.client,
            &self.root,
            &self.base_url,
            key,
            &temp,
            &checksum,
            self.limits.transfer_bytes,
        )
        .await?;
        let records = parse_archive(temp.clone(), key.asset, self.limits).await?;
        self.install(&paths, &temp, checksum).await?;
        Ok(records)
    }

    async fn lock_key(&self, key: ArchiveKey) -> Result<std::fs::File, DataSourceError> {
        let locks_dir = self.root.join(".locks");
        tokio::fs::create_dir_all(&locks_dir)
            .await
            .map_err(super::binance_cache_io::gap)?;
        let lock_path = locks_dir.join(key.lock_filename());
        let lock_path_clone = lock_path.clone();
        spawn_blocking(move || {
            let lock = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(&lock_path_clone)
                .map_err(|error| super::binance_cache_io::gap(error.to_string()))?;
            lock.lock()
                .map_err(|error| super::binance_cache_io::gap(error.to_string()))?;
            Ok::<_, DataSourceError>(lock)
        })
        .await
        .map_err(|error| super::binance_cache_io::gap(error.to_string()))?
    }

    async fn remove_stale_part(&self, paths: &ArchivePaths) {
        let _ = tokio::fs::remove_file(&paths.zip.with_extension("zip.part")).await;
        let _ = tokio::fs::remove_file(&paths.checksum.with_extension("sha256.part")).await;
    }

    fn paths(&self, key: ArchiveKey) -> ArchivePaths {
        ArchivePaths::new(&self.root, key)
    }

    async fn install(
        &self,
        paths: &ArchivePaths,
        temp: &std::path::Path,
        checksum: String,
    ) -> Result<(), DataSourceError> {
        let bytes = tokio::fs::metadata(temp)
            .await
            .map_err(super::binance_cache_io::gap)?
            .len();
        if bytes > self.policy.max_bytes() {
            let _ = tokio::fs::remove_file(temp).await;
            return Err(super::binance_cache_io::gap("archive exceeds cache quota"));
        }
        let checksum_temp = paths.checksum.with_extension("sha256.part");
        tokio::fs::write(&checksum_temp, checksum)
            .await
            .map_err(super::binance_cache_io::gap)?;
        tokio::fs::rename(checksum_temp, &paths.checksum)
            .await
            .map_err(super::binance_cache_io::gap)?;
        tokio::fs::rename(temp, &paths.zip)
            .await
            .map_err(super::binance_cache_io::gap)?;
        self.load_quota().await?;
        {
            let mut quota = self.quota.lock().await;
            quota.clear();
            quota.insert(paths.zip.clone(), bytes);
            while quota.total_bytes() > self.policy.max_bytes() {
                let victim = quota.oldest_except(&paths.zip).ok_or_else(|| {
                    super::binance_cache_io::gap("cache quota cannot retain archive")
                })?;
                tokio::fs::remove_file(&victim)
                    .await
                    .map_err(super::binance_cache_io::gap)?;
                let _ = tokio::fs::remove_file(victim.with_extension("zip.sha256")).await;
                let _ = tokio::fs::remove_file(
                    self.root
                        .join(".locks")
                        .join(victim.file_stem().unwrap_or_default())
                        .with_extension("lock"),
                )
                .await;
                quota.remove(&victim);
            }
            drop(quota);
        }
        Ok(())
    }

    async fn load_quota(&self) -> Result<(), DataSourceError> {
        {
            let mut quota = self.quota.lock().await;
            quota.clear();
        }

        let mut entries = Vec::new();
        let mut directory = tokio::fs::read_dir(&*self.root)
            .await
            .map_err(super::binance_cache_io::gap)?;
        while let Some(entry) = directory
            .next_entry()
            .await
            .map_err(super::binance_cache_io::gap)?
        {
            let path = entry.path();
            if path.extension().is_some_and(|extension| extension == "zip") {
                let size = entry
                    .metadata()
                    .await
                    .map_err(super::binance_cache_io::gap)?
                    .len();
                entries.push((path, size));
            }
        }

        let mut quota = self.quota.lock().await;
        for (path, size) in entries {
            quota.insert(path, size);
        }
        drop(quota);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]
    use super::super::{BinanceArchiveLimits, CachePolicy, VerifiedBinanceArchiveCache};

    fn temp_root(name: &str) -> std::path::PathBuf {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("pmkit-cache-{name}-{suffix}"))
    }

    #[tokio::test]
    async fn stale_part_files_are_removed_before_download() {
        let root = temp_root("stale");
        let cache = VerifiedBinanceArchiveCache::new_for_test(
            root.clone(),
            CachePolicy::Bounded { max_bytes: 1 << 30 },
            BinanceArchiveLimits::default(),
            "http://127.0.0.1:1",
        );
        let key = super::ArchiveKey {
            asset: pmkit_market::Asset::Btc,
            date: chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        };
        let paths = cache.paths(key);
        tokio::fs::create_dir_all(&root).await.unwrap();
        tokio::fs::write(paths.zip.with_extension("zip.part"), b"stale")
            .await
            .unwrap();
        tokio::fs::write(paths.checksum.with_extension("sha256.part"), b"stale")
            .await
            .unwrap();

        // The replay will fail to download because the server is bogus, but the stale part
        // files should already be removed before the download starts.
        let _ = cache.replay_key(key).await;
        assert!(!paths.zip.with_extension("zip.part").exists());
        assert!(!paths.checksum.with_extension("sha256.part").exists());
    }
}
