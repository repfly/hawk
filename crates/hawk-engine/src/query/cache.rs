use std::sync::Mutex;

use lru::LruCache;

use crate::query::result_types::CompareResult;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CompareCacheKey {
    pub dist_id_a: u64,
    pub version_a: u64,
    pub dist_id_b: u64,
    pub version_b: u64,
}

pub struct QueryCache {
    compare_cache: Mutex<LruCache<CompareCacheKey, CompareResult>>,
}

impl QueryCache {
    pub fn new(capacity: usize) -> Self {
        let cap = std::num::NonZeroUsize::new(capacity.max(1)).expect("nonzero capacity");
        Self {
            compare_cache: Mutex::new(LruCache::new(cap)),
        }
    }

    pub fn get_compare(&self, key: &CompareCacheKey) -> Option<CompareResult> {
        self.compare_cache.lock().ok()?.get(key).cloned()
    }

    pub fn put_compare(&self, key: CompareCacheKey, value: CompareResult) {
        if let Ok(mut cache) = self.compare_cache.lock() {
            cache.put(key, value);
        }
    }
}
