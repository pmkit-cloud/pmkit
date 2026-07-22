use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

#[derive(Debug, Default)]
pub(super) struct QuotaState {
    loaded: bool,
    total_bytes: u64,
    next_use: u64,
    entries: HashMap<PathBuf, CacheEntry>,
}

#[derive(Debug)]
struct CacheEntry {
    bytes: u64,
    used: u64,
}

impl QuotaState {
    pub(super) const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub(super) const fn is_loaded(&self) -> bool {
        self.loaded
    }

    pub(super) const fn set_loaded(&mut self) {
        self.loaded = true;
    }

    pub(super) fn insert(&mut self, path: PathBuf, bytes: u64) {
        self.remove(&path);
        self.next_use += 1;
        self.total_bytes += bytes;
        self.entries.insert(
            path,
            CacheEntry {
                bytes,
                used: self.next_use,
            },
        );
    }

    pub(super) fn remove(&mut self, path: &Path) {
        if let Some(entry) = self.entries.remove(path) {
            self.total_bytes -= entry.bytes;
        }
    }

    pub(super) fn touch(&mut self, path: &PathBuf) {
        self.next_use += 1;
        if let Some(entry) = self.entries.get_mut(path) {
            entry.used = self.next_use;
        }
    }

    pub(super) fn oldest_except(&self, kept: &Path) -> Option<PathBuf> {
        self.entries
            .iter()
            .filter(|(path, _)| path.as_path() != kept)
            .min_by_key(|(_, entry)| entry.used)
            .map(|(path, _)| path.clone())
    }
}
