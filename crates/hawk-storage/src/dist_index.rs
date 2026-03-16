use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use hawk_core::{canonical_dimension_key, DimensionKey};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DistIndex {
    pub by_key: HashMap<String, u64>,
}

impl DistIndex {
    pub fn key(variable: &str, dimension_key: &DimensionKey) -> String {
        format!("{variable}|{}", canonical_dimension_key(dimension_key))
    }

    pub fn insert(&mut self, variable: &str, dimension_key: &DimensionKey, distribution_id: u64) {
        self.by_key
            .insert(Self::key(variable, dimension_key), distribution_id);
    }

    pub fn get(&self, variable: &str, dimension_key: &DimensionKey) -> Option<u64> {
        self.by_key
            .get(&Self::key(variable, dimension_key))
            .copied()
    }

    pub fn remove(&mut self, variable: &str, dimension_key: &DimensionKey) {
        self.by_key.remove(&Self::key(variable, dimension_key));
    }
}
