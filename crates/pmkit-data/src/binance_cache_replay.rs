use std::sync::Arc;

use tokio::sync::Mutex;

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
        let key_lock = self.key_lock(key).await;
        let _guard = key_lock.lock().await;
        let paths = self.paths(key);

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

    async fn key_lock(&self, key: ArchiveKey) -> Arc<Mutex<()>> {
        let mut locks = self.key_locks.lock().await;
        Arc::clone(locks.entry(key).or_insert_with(|| Arc::new(Mutex::new(()))))
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
            quota.insert(paths.zip.clone(), bytes);
            while quota.total_bytes() > self.policy.max_bytes() {
                let victim = quota.oldest_except(&paths.zip).ok_or_else(|| {
                    super::binance_cache_io::gap("cache quota cannot retain archive")
                })?;
                tokio::fs::remove_file(&victim)
                    .await
                    .map_err(super::binance_cache_io::gap)?;
                let _ = tokio::fs::remove_file(victim.with_extension("zip.sha256")).await;
                quota.remove(&victim);
            }
            drop(quota);
        }
        Ok(())
    }

    async fn load_quota(&self) -> Result<(), DataSourceError> {
        {
            let mut quota = self.quota.lock().await;
            if quota.is_loaded() {
                return Ok(());
            }
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
                    quota.insert(
                        path,
                        entry
                            .metadata()
                            .await
                            .map_err(super::binance_cache_io::gap)?
                            .len(),
                    );
                }
            }
            quota.set_loaded();
        }
        Ok(())
    }
}
