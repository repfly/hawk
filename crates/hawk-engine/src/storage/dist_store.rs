use std::{
    collections::{BTreeSet, HashMap},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use chrono::Utc;

use crate::core::{
    canonical_dimension_key, DimensionDefinition, DimensionKey, DistributionObject,
    DistributionRepr, Schema, VariableDefinition,
};
use crate::math::entropy;

use crate::storage::dist_index::DistIndex;
use crate::storage::file_format::{
    ensure_file, ensure_snapshot_file, read_file, read_file_mmap, rebuild_index, write_file,
    DistributionFile, MetaFile,
};
use crate::storage::lock::DatabaseLock;
use crate::storage::raw_log::RawLog;
use crate::storage::snapshot_store::{SnapshotEntry, SnapshotStore};

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
    distribution_positions: HashMap<u64, usize>,
    dimension_value_catalog: HashMap<String, BTreeSet<String>>,
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
            fs::create_dir_all(root.join("raw"))
                .with_context(|| format!("create raw dir {}", root.display()))?;
        }

        let meta_path = root.join("meta.edb");
        let dist_path = root.join("distributions.edb");
        let index_path = root.join("dist_index.edb");
        let snapshots_path = root.join("snapshots.edb");

        let meta = MetaFile {
            raw_log_enabled: enable_raw_log,
            ..MetaFile::default()
        };
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

        let distribution_positions = build_distribution_positions(&data.distributions)?;
        let dimension_value_catalog = build_dimension_value_catalog(&data.distributions);

        let index = if index_path.exists() {
            read_file::<DistIndex>(&index_path)
                .ok()
                .filter(|index| index_matches_distributions(index, &data.distributions))
                .unwrap_or_else(|| rebuild_index(&data.distributions))
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
            distribution_positions,
            dimension_value_catalog,
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
        self.meta
            .schema
            .define_variable(variable)
            .map_err(|e| anyhow!(e.to_string()))
    }

    pub fn define_dimension(&mut self, dimension: DimensionDefinition) -> Result<()> {
        self.ensure_write_mode()?;
        self.meta
            .schema
            .define_dimension(dimension)
            .map_err(|e| anyhow!(e.to_string()))
    }

    pub fn define_joint(&mut self, var_a: &str, var_b: &str) -> Result<()> {
        self.ensure_write_mode()?;
        self.meta
            .schema
            .define_joint(var_a, var_b)
            .map_err(|e| anyhow!(e.to_string()))
    }

    pub fn ensure_distribution(
        &mut self,
        variable: &str,
        dimension_key: &DimensionKey,
    ) -> Result<u64> {
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

        let position = self.data.distributions.len();
        self.data.distributions.push(dist);
        self.distribution_positions.insert(id, position);
        self.add_dimension_values_to_catalog(dimension_key);
        self.index.insert(variable, dimension_key, id);
        Ok(id)
    }

    pub fn get_distribution(
        &self,
        variable: &str,
        dimension_key: &DimensionKey,
    ) -> Option<&DistributionObject> {
        let id = self.index.get(variable, dimension_key)?;
        self.distribution_by_id(id)
    }

    pub fn get_distribution_mut(
        &mut self,
        variable: &str,
        dimension_key: &DimensionKey,
    ) -> Option<&mut DistributionObject> {
        let id = self.index.get(variable, dimension_key)?;
        self.distribution_by_id_mut(id)
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

        let position = self
            .distribution_positions
            .get(&id)
            .copied()
            .ok_or_else(|| anyhow!("distribution id '{}' not found", id))?;
        let snapshot = self
            .data
            .distributions
            .get(position)
            .filter(|d| d.id == id)
            .cloned()
            .ok_or_else(|| anyhow!("distribution id '{}' not found", id))?;

        self.snapshots.push_snapshot(&snapshot);

        let dist = self
            .data
            .distributions
            .get_mut(position)
            .filter(|d| d.id == id)
            .ok_or_else(|| anyhow!("distribution id '{}' not found", id))?;
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
            .distribution_by_id_mut(id)
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
            let Some(dist) = self.distribution_by_id_mut(id) else {
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

    pub fn snapshots_for(
        &self,
        variable: &str,
        dimension_key: &DimensionKey,
    ) -> Vec<SnapshotEntry> {
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
        self.dimension_value_catalog
            .get(dimension)
            .cloned()
            .unwrap_or_default()
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

    pub fn joints_for_pair<'a>(
        &'a self,
        var_a: &str,
        var_b: &str,
    ) -> Vec<&'a crate::core::JointDistributionObject> {
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
    ) -> Option<&crate::core::JointDistributionObject> {
        let pair = if var_a <= var_b {
            (var_a, var_b)
        } else {
            (var_b, var_a)
        };
        self.data.joints.iter().find(|j| {
            j.variables.0 == pair.0 && j.variables.1 == pair.1 && &j.dimension_key == dimension_key
        })
    }

    pub fn get_joint_distribution_mut(
        &mut self,
        var_a: &str,
        var_b: &str,
        dimension_key: &DimensionKey,
    ) -> Option<&mut crate::core::JointDistributionObject> {
        let pair = if var_a <= var_b {
            (var_a.to_owned(), var_b.to_owned())
        } else {
            (var_b.to_owned(), var_a.to_owned())
        };
        self.data.joints.iter_mut().find(|j| {
            j.variables.0 == pair.0 && j.variables.1 == pair.1 && &j.dimension_key == dimension_key
        })
    }

    pub fn ensure_joint_distribution(
        &mut self,
        var_a: &str,
        var_b: &str,
        dimension_key: &DimensionKey,
    ) -> Result<()> {
        if self
            .get_joint_distribution(var_a, var_b, dimension_key)
            .is_some()
        {
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

        use crate::core::{JointDistributionObject, JointRepr, VariableType};
        let repr = match (&def_a.var_type, &def_b.var_type) {
            (
                VariableType::Continuous {
                    bins: xb,
                    range: xr,
                },
                VariableType::Continuous {
                    bins: yb,
                    range: yr,
                },
            ) => {
                let (x_min, x_max) = xr.unwrap_or((0.0, 1.0));
                let (y_min, y_max) = yr.unwrap_or((0.0, 1.0));
                JointRepr::HistogramGrid {
                    x_min,
                    x_max,
                    x_bins: *xb,
                    y_min,
                    y_max,
                    y_bins: *yb,
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

    fn distribution_by_id(&self, id: u64) -> Option<&DistributionObject> {
        let position = *self.distribution_positions.get(&id)?;
        self.data.distributions.get(position).filter(|d| d.id == id)
    }

    fn distribution_by_id_mut(&mut self, id: u64) -> Option<&mut DistributionObject> {
        let position = *self.distribution_positions.get(&id)?;
        self.data
            .distributions
            .get_mut(position)
            .filter(|d| d.id == id)
    }

    fn add_dimension_values_to_catalog(&mut self, dimension_key: &DimensionKey) {
        for (dimension, value) in dimension_key {
            self.dimension_value_catalog
                .entry(dimension.clone())
                .or_default()
                .insert(value.clone());
        }
    }
}

fn build_distribution_positions(
    distributions: &[DistributionObject],
) -> Result<HashMap<u64, usize>> {
    let mut positions = HashMap::with_capacity(distributions.len());
    for (position, distribution) in distributions.iter().enumerate() {
        if positions.insert(distribution.id, position).is_some() {
            return Err(anyhow!("duplicate distribution id '{}'", distribution.id));
        }
    }
    Ok(positions)
}

fn index_matches_distributions(index: &DistIndex, distributions: &[DistributionObject]) -> bool {
    index.by_key.len() == distributions.len()
        && distributions.iter().all(|distribution| {
            index.get(&distribution.variable, &distribution.dimension_key) == Some(distribution.id)
        })
}

fn build_dimension_value_catalog(
    distributions: &[DistributionObject],
) -> HashMap<String, BTreeSet<String>> {
    let mut catalog: HashMap<String, BTreeSet<String>> = HashMap::new();
    for distribution in distributions {
        for (dimension, value) in &distribution.dimension_key {
            catalog
                .entry(dimension.clone())
                .or_default()
                .insert(value.clone());
        }
    }
    catalog
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::core::{
        dimension_key_from_pairs, DimensionDefinition, DistributionRepr, VariableDefinition,
        VariableType,
    };
    use crate::storage::dist_index::DistIndex;
    use crate::storage::file_format::write_file;

    use super::{Database, OpenMode};

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

        let reopened = Database::open(&root, OpenMode::ReadOnly).expect("reopen db");
        let loaded = reopened
            .get_distribution("sentiment", &key)
            .expect("load distribution");

        assert_eq!(loaded.sample_count, 3);
    }

    #[test]
    fn direct_lookup_and_dimension_catalog_survive_reopen() {
        let root = test_dir("lookup-catalog");
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
            source_column: "topic".into(),
            granularity: None,
        })
        .expect("define topic");
        db.define_dimension(DimensionDefinition {
            name: "region".into(),
            source_column: "region".into(),
            granularity: None,
        })
        .expect("define region");

        for i in 0..1_000 {
            let topic = format!("topic-{i}");
            let region = if i % 2 == 0 { "us" } else { "eu" };
            let key = dimension_key_from_pairs([("topic", topic.as_str()), ("region", region)]);
            db.ensure_distribution("sentiment", &key)
                .expect("ensure distribution");
        }

        assert_eq!(db.distribution_positions.len(), db.data.distributions.len());
        assert_eq!(db.dimension_values("region").len(), 2);
        assert_eq!(db.dimension_values("topic").len(), 1_000);
        db.flush().expect("flush");

        let reopened = Database::open(&root, OpenMode::ReadOnly).expect("reopen db");
        let key = dimension_key_from_pairs([("topic", "topic-777"), ("region", "eu")]);
        let loaded = reopened
            .get_distribution("sentiment", &key)
            .expect("direct lookup after reopen");

        assert_eq!(loaded.dimension_key, key);
        assert_eq!(reopened.distribution_positions.len(), 1_000);
        assert_eq!(reopened.dimension_values("region").len(), 2);
        assert_eq!(reopened.dimension_values("topic").len(), 1_000);
    }

    #[test]
    fn stale_persisted_distribution_index_is_rebuilt_on_open() {
        let root = test_dir("stale-index");
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

        let key = dimension_key_from_pairs([("topic", "climate")]);
        db.ensure_distribution("sentiment", &key)
            .expect("ensure distribution");
        db.flush().expect("flush");

        let mut stale_index = DistIndex::default();
        stale_index
            .by_key
            .insert("sentiment|topic:climate".into(), 999);
        write_file(&root.join("dist_index.edb"), &stale_index).expect("write stale index");

        let reopened = Database::open(&root, OpenMode::ReadOnly).expect("reopen db");
        let loaded = reopened
            .get_distribution("sentiment", &key)
            .expect("lookup with rebuilt index");

        assert_eq!(loaded.id, 1);
    }
}
