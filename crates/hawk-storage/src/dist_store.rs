use std::{
    collections::{BTreeSet, HashMap},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use chrono::Utc;

use hawk_core::{
    canonical_dimension_key, DistributionObject, DistributionRepr, DimensionDefinition, DimensionKey, Schema,
    VariableDefinition,
};
use hawk_math::entropy;

use crate::{
    dist_index::DistIndex,
    file_format::{
        ensure_file, ensure_snapshot_file, read_file, read_file_mmap, rebuild_index, write_file, DistributionFile,
        MetaFile,
    },
    lock::DatabaseLock,
    raw_log::RawLog,
    snapshot_store::{SnapshotEntry, SnapshotStore},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    ReadOnly,
    ReadWrite,
}

#[derive(Debug, Clone)]
pub struct DatabaseStats {
    pub distributions: usize,
    pub total_samples: u64,
    pub variables: usize,
    pub dimensions: usize,
}

pub struct Database {
    root: PathBuf,
    mode: OpenMode,
    meta: MetaFile,
    data: DistributionFile,
    index: DistIndex,
    snapshots: SnapshotStore,
    raw_log: Option<RawLog>,
    _lock: Option<DatabaseLock>,
}

impl Database {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Self::create_with_options(path, true)
    }

    pub fn create_with_options(path: impl AsRef<Path>, enable_raw_log: bool) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(&root).with_context(|| format!("create db dir {}", root.display()))?;
        if enable_raw_log {
            fs::create_dir_all(root.join("raw")).with_context(|| format!("create raw dir {}", root.display()))?;
        }

        let meta_path = root.join("meta.edb");
        let dist_path = root.join("distributions.edb");
        let index_path = root.join("dist_index.edb");
        let snapshots_path = root.join("snapshots.edb");

        let mut meta = MetaFile::default();
        meta.raw_log_enabled = enable_raw_log;
        ensure_file(&meta_path, &meta)?;
        ensure_file(&dist_path, &DistributionFile::default())?;
        ensure_file(&index_path, &DistIndex::default())?;
        ensure_snapshot_file(&snapshots_path)?;

        Self::open(path, OpenMode::ReadWrite)
    }

    pub fn open(path: impl AsRef<Path>, mode: OpenMode) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        if !root.exists() {
            return Err(anyhow!("database path does not exist: {}", root.display()));
        }

        let lock_path = root.join("lock.edb");
        let lock = if mode == OpenMode::ReadWrite {
            Some(DatabaseLock::acquire(&lock_path)?)
        } else {
            None
        };

        let meta_path = root.join("meta.edb");
        let dist_path = root.join("distributions.edb");
        let index_path = root.join("dist_index.edb");
        let snapshots_path = root.join("snapshots.edb");

        let meta = read_file::<MetaFile>(&meta_path)?;
        // Use mmap for the largest file in read-only mode
        let data = if mode == OpenMode::ReadOnly {
            read_file_mmap::<DistributionFile>(&dist_path)?
        } else {
            read_file::<DistributionFile>(&dist_path)?
        };

        let index = if index_path.exists() {
            read_file::<DistIndex>(&index_path).unwrap_or_else(|_| rebuild_index(&data.distributions))
        } else {
            rebuild_index(&data.distributions)
        };

        let snapshots = if snapshots_path.exists() {
            read_file::<SnapshotStore>(&snapshots_path).unwrap_or_default()
        } else {
            SnapshotStore::default()
        };

        let raw_log = if meta.raw_log_enabled {
            Some(RawLog::new(&root.join("raw"))?)
        } else {
            None
        };

        Ok(Self {
            root,
            mode,
            meta,
            data,
            index,
            snapshots,
            raw_log,
            _lock: lock,
        })
    }

    pub fn close(&mut self) -> Result<()> {
        self.flush()
    }

    pub fn flush(&self) -> Result<()> {
        if self.mode == OpenMode::ReadOnly {
            return Ok(());
        }

        write_file(&self.root.join("meta.edb"), &self.meta)?;
        write_file(&self.root.join("distributions.edb"), &self.data)?;
        write_file(&self.root.join("dist_index.edb"), &self.index)?;
        write_file(&self.root.join("snapshots.edb"), &self.snapshots)?;
        Ok(())
    }

    pub fn schema(&self) -> &Schema {
        &self.meta.schema
    }

    pub fn define_variable(&mut self, variable: VariableDefinition) -> Result<()> {
        self.ensure_write_mode()?;
        self.meta.schema.define_variable(variable).map_err(|e| anyhow!(e.to_string()))
    }

    pub fn define_dimension(&mut self, dimension: DimensionDefinition) -> Result<()> {
        self.ensure_write_mode()?;
        self.meta.schema.define_dimension(dimension).map_err(|e| anyhow!(e.to_string()))
    }

    pub fn define_joint(&mut self, var_a: &str, var_b: &str) -> Result<()> {
        self.ensure_write_mode()?;
        self.meta
            .schema
            .define_joint(var_a, var_b)
            .map_err(|e| anyhow!(e.to_string()))
    }

    pub fn ensure_distribution(&mut self, variable: &str, dimension_key: &DimensionKey) -> Result<u64> {
        if let Some(id) = self.index.get(variable, dimension_key) {
            return Ok(id);
        }

        let variable_def = self
            .meta
            .schema
            .variables
            .iter()
            .find(|v| v.name == variable)
            .ok_or_else(|| anyhow!("unknown variable '{}'", variable))?;

        let id = self.meta.next_distribution_id;
        self.meta.next_distribution_id += 1;

        let repr = DistributionRepr::from_variable(&variable_def.var_type);
        let dist = DistributionObject::new(id, variable, dimension_key.clone(), repr);

        self.data.distributions.push(dist);
        self.index.insert(variable, dimension_key, id);
        Ok(id)
    }

    pub fn get_distribution(&self, variable: &str, dimension_key: &DimensionKey) -> Option<&DistributionObject> {
        let id = self.index.get(variable, dimension_key)?;
        self.data.distributions.iter().find(|d| d.id == id)
    }

    pub fn get_distribution_mut(
        &mut self,
        variable: &str,
        dimension_key: &DimensionKey,
    ) -> Option<&mut DistributionObject> {
        let id = self.index.get(variable, dimension_key)?;
        self.data.distributions.iter_mut().find(|d| d.id == id)
    }

    pub fn update_distribution<F>(
        &mut self,
        variable: &str,
        dimension_key: &DimensionKey,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(&mut DistributionObject),
    {
        self.ensure_write_mode()?;
        self.ensure_distribution(variable, dimension_key)?;

        let id = self
            .index
            .get(variable, dimension_key)
            .ok_or_else(|| anyhow!("distribution index missing after ensure"))?;

        let dist = self
            .data
            .distributions
            .iter_mut()
            .find(|d| d.id == id)
            .ok_or_else(|| anyhow!("distribution id '{}' not found", id))?;

        self.snapshots.push_snapshot(dist);

        f(dist);

        dist.sample_count = dist.repr.total_count();
        dist.entropy = entropy(&dist.repr.value_count_vector(), dist.repr.total_count());
        dist.version += 1;
        dist.last_updated = Utc::now().timestamp() as u64;

        Ok(())
    }

    /// Low-level increment: applies the closure without snapshotting or
    /// recalculating entropy.  Call `finalize_distributions` once after
    /// a batch of increments.
    pub fn increment_distribution<F>(
        &mut self,
        variable: &str,
        dimension_key: &DimensionKey,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(&mut DistributionObject),
    {
        self.ensure_write_mode()?;
        self.ensure_distribution(variable, dimension_key)?;

        let id = self
            .index
            .get(variable, dimension_key)
            .ok_or_else(|| anyhow!("distribution index missing after ensure"))?;

        let dist = self
            .data
            .distributions
            .iter_mut()
            .find(|d| d.id == id)
            .ok_or_else(|| anyhow!("distribution id '{}' not found", id))?;

        f(dist);
        Ok(())
    }

    /// Finalize a set of distributions after batch increments: recalculate
    /// entropy, update sample_count, and bump version once per distribution.
    pub fn finalize_distributions(
        &mut self,
        touched: &std::collections::HashSet<(String, DimensionKey)>,
    ) -> Result<()> {
        let now = Utc::now().timestamp() as u64;
        for (variable, dim_key) in touched {
            let Some(id) = self.index.get(variable, dim_key) else {
                continue;
            };
            let Some(dist) = self.data.distributions.iter_mut().find(|d| d.id == id) else {
                continue;
            };
            dist.sample_count = dist.repr.total_count();
            dist.entropy = entropy(&dist.repr.value_count_vector(), dist.sample_count);
            dist.version += 1;
            dist.last_updated = now;
        }
        Ok(())
    }

    pub fn raw_log_enabled(&self) -> bool {
        self.meta.raw_log_enabled
    }

    pub fn append_raw_record(&mut self, payload: &serde_json::Value) -> Result<u64> {
        self.ensure_write_mode()?;
        let Some(ref raw_log) = self.raw_log else {
            return Ok(0);
        };
        let id = self.meta.next_raw_record_id;
        self.meta.next_raw_record_id += 1;
        raw_log.append(id, payload)?;
        Ok(id)
    }

    pub fn snapshots_for(&self, variable: &str, dimension_key: &DimensionKey) -> Vec<SnapshotEntry> {
        self.snapshots.get_snapshots(variable, dimension_key)
    }

    pub fn stats(&self) -> DatabaseStats {
        DatabaseStats {
            distributions: self.data.distributions.len(),
            total_samples: self.data.distributions.iter().map(|d| d.sample_count).sum(),
            variables: self.meta.schema.variables.len(),
            dimensions: self.meta.schema.dimensions.len(),
        }
    }

    pub fn dimension_values(&self, dimension: &str) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for dist in &self.data.distributions {
            if let Some(v) = dist.dimension_key.get(dimension) {
                out.insert(v.clone());
            }
        }
        out
    }

    pub fn distributions_for_variable<'a>(&'a self, variable: &str) -> Vec<&'a DistributionObject> {
        self.data
            .distributions
            .iter()
            .filter(|d| d.variable == variable)
            .collect()
    }

    pub fn find_distribution_by_reference(
        &self,
        variable: &str,
        key_parts: &HashMap<String, String>,
    ) -> Option<&DistributionObject> {
        self.data.distributions.iter().find(|dist| {
            if dist.variable != variable {
                return false;
            }
            key_parts
                .iter()
                .all(|(k, v)| dist.dimension_key.get(k) == Some(v))
        })
    }

    pub fn joints_for_pair<'a>(&'a self, var_a: &str, var_b: &str) -> Vec<&'a hawk_core::JointDistributionObject> {
        let pair = if var_a <= var_b {
            (var_a, var_b)
        } else {
            (var_b, var_a)
        };
        self.data
            .joints
            .iter()
            .filter(|j| j.variables.0 == pair.0 && j.variables.1 == pair.1)
            .collect()
    }

    pub fn get_joint_distribution(
        &self,
        var_a: &str,
        var_b: &str,
        dimension_key: &DimensionKey,
    ) -> Option<&hawk_core::JointDistributionObject> {
        let pair = if var_a <= var_b {
            (var_a, var_b)
        } else {
            (var_b, var_a)
        };
        self.data.joints.iter().find(|j| {
            j.variables.0 == pair.0
                && j.variables.1 == pair.1
                && &j.dimension_key == dimension_key
        })
    }

    pub fn get_joint_distribution_mut(
        &mut self,
        var_a: &str,
        var_b: &str,
        dimension_key: &DimensionKey,
    ) -> Option<&mut hawk_core::JointDistributionObject> {
        let pair = if var_a <= var_b {
            (var_a.to_owned(), var_b.to_owned())
        } else {
            (var_b.to_owned(), var_a.to_owned())
        };
        self.data.joints.iter_mut().find(|j| {
            j.variables.0 == pair.0
                && j.variables.1 == pair.1
                && &j.dimension_key == dimension_key
        })
    }

    pub fn ensure_joint_distribution(
        &mut self,
        var_a: &str,
        var_b: &str,
        dimension_key: &DimensionKey,
    ) -> Result<()> {
        if self.get_joint_distribution(var_a, var_b, dimension_key).is_some() {
            return Ok(());
        }
        let pair = if var_a <= var_b {
            (var_a.to_owned(), var_b.to_owned())
        } else {
            (var_b.to_owned(), var_a.to_owned())
        };

        let schema = &self.meta.schema;
        let def_a = schema.variables.iter().find(|v| v.name == pair.0);
        let def_b = schema.variables.iter().find(|v| v.name == pair.1);
        let (Some(def_a), Some(def_b)) = (def_a, def_b) else {
            return Err(anyhow!("joint variables not found in schema"));
        };

        use hawk_core::{JointDistributionObject, JointRepr, VariableType};
        let repr = match (&def_a.var_type, &def_b.var_type) {
            (
                VariableType::Continuous { bins: xb, range: xr },
                VariableType::Continuous { bins: yb, range: yr },
            ) => {
                let (x_min, x_max) = xr.unwrap_or((0.0, 1.0));
                let (y_min, y_max) = yr.unwrap_or((0.0, 1.0));
                JointRepr::HistogramGrid {
                    x_min, x_max, x_bins: *xb,
                    y_min, y_max, y_bins: *yb,
                    counts: vec![vec![0u64; *yb as usize]; *xb as usize],
                    total_count: 0,
                }
            }
            (
                VariableType::Categorical { categories: xc, .. },
                VariableType::Categorical { categories: yc, .. },
            ) => JointRepr::ContingencyTable {
                x_categories: xc.clone(),
                y_categories: yc.clone(),
                counts: vec![vec![0u64; yc.len()]; xc.len()],
                total_count: 0,
            },
            (
                VariableType::Categorical { categories, .. },
                VariableType::Continuous { bins, range },
            )
            | (
                VariableType::Continuous { bins, range },
                VariableType::Categorical { categories, .. },
            ) => {
                let (min, max) = range.unwrap_or((0.0, 1.0));
                JointRepr::ConditionalHistograms {
                    condition_categories: categories.clone(),
                    histograms: categories
                        .iter()
                        .map(|_| DistributionRepr::Histogram {
                            min,
                            max,
                            bin_counts: vec![0u64; *bins as usize],
                            total_count: 0,
                        })
                        .collect(),
                    total_count: 0,
                }
            }
        };

        let id = self.meta.next_joint_id;
        self.meta.next_joint_id += 1;
        let now = Utc::now().timestamp() as u64;
        self.data.joints.push(JointDistributionObject {
            id,
            variables: pair,
            dimension_key: dimension_key.clone(),
            repr,
            sample_count: 0,
            last_updated: now,
            version: 1,
        });
        Ok(())
    }

    pub fn canonical_reference(variable: &str, dimension_key: &DimensionKey) -> String {
        format!("{variable}|{}", canonical_dimension_key(dimension_key))
    }

    /// Return the current high-water mark (number of rows already ingested
    /// for delta ingestion).
    pub fn get_high_water_mark(&self) -> u64 {
        self.meta.high_water_mark
    }

    /// Advance the high-water mark after a successful delta ingestion.
    pub fn set_high_water_mark(&mut self, n: u64) -> Result<()> {
        self.ensure_write_mode()?;
        self.meta.high_water_mark = n;
        Ok(())
    }

    fn ensure_write_mode(&self) -> Result<()> {
        if self.mode == OpenMode::ReadOnly {
            return Err(anyhow!("database opened in read-only mode"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use hawk_core::{
        dimension_key_from_pairs, DimensionDefinition, DistributionRepr, VariableDefinition, VariableType,
    };

    use super::Database;

    fn test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("hawk-storage-test-{}-{}", name, std::process::id()))
    }

    #[test]
    fn create_update_reopen_roundtrip() {
        let root = test_dir("roundtrip");
        if root.exists() {
            std::fs::remove_dir_all(&root).expect("cleanup old dir");
        }

        let mut db = Database::create(&root).expect("create db");
        db.define_variable(VariableDefinition {
            name: "sentiment".into(),
            var_type: VariableType::Continuous {
                bins: 10,
                range: Some((-1.0, 1.0)),
            },
        })
        .expect("define variable");
        db.define_dimension(DimensionDefinition {
            name: "topic".into(),
            source_column: "topic_label".into(),
            granularity: None,
        })
        .expect("define dimension");

        let key = dimension_key_from_pairs([("topic", "climate-change")]);
        db.update_distribution("sentiment", &key, |dist| {
            if let DistributionRepr::Histogram { .. } = &dist.repr {
                dist.repr.increment_histogram(4, 2);
                dist.repr.increment_histogram(5, 1);
            }
        })
        .expect("update dist");

        db.flush().expect("flush");

        let reopened = Database::open(&root, super::OpenMode::ReadOnly).expect("reopen db");
        let loaded = reopened
            .get_distribution("sentiment", &key)
            .expect("load distribution");

        assert_eq!(loaded.sample_count, 3);
    }
}
