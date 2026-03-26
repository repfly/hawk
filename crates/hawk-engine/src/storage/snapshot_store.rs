use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::core::{canonical_dimension_key, DistributionObject, DimensionKey};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub version: u64,
    pub timestamp: u64,
    pub distribution: DistributionObject,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SnapshotStore {
    pub entries: HashMap<String, Vec<SnapshotEntry>>,
}

impl SnapshotStore {
    fn key(variable: &str, dimension_key: &DimensionKey) -> String {
        format!("{variable}|{}", canonical_dimension_key(dimension_key))
    }

    pub fn push_snapshot(&mut self, dist: &DistributionObject) {
        let key = Self::key(&dist.variable, &dist.dimension_key);
        self.entries.entry(key).or_default().push(SnapshotEntry {
            version: dist.version,
            timestamp: dist.last_updated,
            distribution: dist.clone(),
        });
    }

    pub fn get_snapshots(&self, variable: &str, dimension_key: &DimensionKey) -> Vec<SnapshotEntry> {
        self.entries
            .get(&Self::key(variable, dimension_key))
            .cloned()
            .unwrap_or_default()
    }
}
