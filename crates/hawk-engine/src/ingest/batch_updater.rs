use std::collections::{HashMap, HashSet};

use anyhow::Result;
use chrono::{Datelike, NaiveDate, NaiveDateTime};
use rayon::prelude::*;
use serde_json::Value;

use crate::core::{dimension_key_from_pairs, DimensionKey, DistributionRepr, Schema, VariableType};
use crate::storage::Database;

use crate::ingest::column_mapper::MappedRow;

#[derive(Debug, Clone, Default)]
pub struct BatchReport {
    pub processed: usize,
    pub skipped: usize,
    pub distributions_updated: usize,
}

/// Key for accumulated counts: (variable_name, dimension_key)
type AccumKey = (String, DimensionKey);

/// Accumulated count deltas for a single (variable, dim_key).
#[derive(Debug, Clone)]
enum CountDelta {
    Categorical {
        /// index -> count (None index = unknown)
        deltas: HashMap<Option<usize>, u64>,
    },
    Histogram {
        /// bin_index -> count
        deltas: HashMap<usize, u64>,
    },
}

/// Joint count delta for a single (var_a, var_b, dim_key).
#[derive(Debug, Clone, Default)]
struct JointDelta {
    /// (row_idx, col_idx) -> count
    deltas: HashMap<(usize, usize), u64>,
}

pub fn apply_batch(db: &mut Database, schema: &Schema, rows: &[MappedRow]) -> Result<BatchReport> {
    let mut report = BatchReport::default();

    // Phase 1: Pre-compute dimension keys and resolve values (parallel)
    let resolved: Vec<Option<ResolvedRow>> = rows
        .par_iter()
        .map(|row| resolve_row(row, schema))
        .collect();

    // Phase 2: Accumulate count deltas in memory
    let mut marginal_accum: HashMap<AccumKey, CountDelta> = HashMap::new();
    let mut joint_accum: HashMap<(String, String, DimensionKey), JointDelta> = HashMap::new();

    for maybe_row in &resolved {
        let Some(row) = maybe_row else {
            report.skipped += 1;
            continue;
        };

        for (var_name, delta) in &row.variable_deltas {
            let key = (var_name.clone(), row.dimension_key.clone());
            let entry = marginal_accum.entry(key).or_insert_with(|| CountDelta::from_single(delta));
            entry.add(delta);
        }

        for ((va, vb), (ri, ci)) in &row.joint_indices {
            let key = (va.clone(), vb.clone(), row.dimension_key.clone());
            *joint_accum.entry(key).or_default().deltas.entry((*ri, *ci)).or_insert(0) += 1;
        }

        report.processed += 1;

        if db.raw_log_enabled() {
            db.append_raw_record(&Value::Object(
                row.raw_values.clone(),
            ))?;
        }
    }

    // Phase 3: Apply accumulated deltas to database (serial, one call per distribution)
    let mut touched: HashSet<(String, DimensionKey)> = HashSet::new();

    for ((var_name, dim_key), delta) in &marginal_accum {
        db.increment_distribution(var_name, dim_key, |dist| {
            match (&mut dist.repr, delta) {
                (DistributionRepr::Categorical { counts, unknown_count, total_count, .. },
                 CountDelta::Categorical { deltas }) => {
                    for (idx, count) in deltas {
                        match idx {
                            Some(i) => {
                                if let Some(slot) = counts.get_mut(*i) {
                                    *slot += count;
                                }
                            }
                            None => *unknown_count += count,
                        }
                        *total_count += count;
                    }
                }
                (DistributionRepr::Histogram { bin_counts, total_count, .. },
                 CountDelta::Histogram { deltas }) => {
                    for (idx, count) in deltas {
                        if let Some(slot) = bin_counts.get_mut(*idx) {
                            *slot += count;
                        }
                        *total_count += count;
                    }
                }
                _ => {}
            }
        })?;
        touched.insert((var_name.clone(), dim_key.clone()));
    }

    // Apply joint deltas
    for ((va, vb, dim_key), jd) in &joint_accum {
        db.ensure_joint_distribution(va, vb, dim_key)?;
        if let Some(joint) = db.get_joint_distribution_mut(va, vb, dim_key) {
            use crate::core::JointRepr;
            match &mut joint.repr {
                JointRepr::ContingencyTable { counts, total_count, .. }
                | JointRepr::HistogramGrid { counts, total_count, .. } => {
                    for ((ri, ci), count) in &jd.deltas {
                        if let Some(row) = counts.get_mut(*ri) {
                            if let Some(cell) = row.get_mut(*ci) {
                                *cell += count;
                                *total_count += count;
                                joint.sample_count += count;
                            }
                        }
                    }
                }
                JointRepr::ConditionalHistograms { histograms, total_count, .. } => {
                    for ((ri, ci), count) in &jd.deltas {
                        if let Some(hist) = histograms.get_mut(*ri) {
                            if let DistributionRepr::Histogram { bin_counts, total_count: ht, .. } = hist {
                                if let Some(slot) = bin_counts.get_mut(*ci) {
                                    *slot += count;
                                    *ht += count;
                                    *total_count += count;
                                    joint.sample_count += count;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    db.finalize_distributions(&touched)?;
    report.distributions_updated = touched.len();
    Ok(report)
}

/// A pre-resolved row with dimension key and per-variable deltas computed.
struct ResolvedRow {
    dimension_key: DimensionKey,
    variable_deltas: Vec<(String, SingleDelta)>,
    joint_indices: Vec<((String, String), (usize, usize))>,
    raw_values: serde_json::Map<String, Value>,
}

/// A single value's contribution.
#[derive(Debug, Clone)]
enum SingleDelta {
    CategoricalIdx(Option<usize>),
    HistogramIdx(usize),
}

impl CountDelta {
    fn from_single(single: &SingleDelta) -> Self {
        match single {
            SingleDelta::CategoricalIdx(_) => CountDelta::Categorical { deltas: HashMap::new() },
            SingleDelta::HistogramIdx(_) => CountDelta::Histogram { deltas: HashMap::new() },
        }
    }

    fn add(&mut self, single: &SingleDelta) {
        match (self, single) {
            (CountDelta::Categorical { deltas }, SingleDelta::CategoricalIdx(idx)) => {
                *deltas.entry(*idx).or_insert(0) += 1;
            }
            (CountDelta::Histogram { deltas }, SingleDelta::HistogramIdx(idx)) => {
                *deltas.entry(*idx).or_insert(0) += 1;
            }
            _ => {}
        }
    }
}

fn resolve_row(row: &MappedRow, schema: &Schema) -> Option<ResolvedRow> {
    let mut key_pairs = Vec::with_capacity(schema.dimensions.len());
    for dim in &schema.dimensions {
        let raw_value = row.dimension_values.get(&dim.name)?;
        let canonical = normalize_dimension_value(raw_value, dim.granularity.as_deref());
        key_pairs.push((dim.name.clone(), canonical));
    }
    let dimension_key = dimension_key_from_pairs(key_pairs);

    let mut variable_deltas = Vec::new();
    for variable in &schema.variables {
        let Some(value) = row.variable_values.get(&variable.name) else {
            continue;
        };
        if let Some(delta) = resolve_value(variable, value) {
            variable_deltas.push((variable.name.clone(), delta));
        }
    }

    let mut joint_indices = Vec::new();
    for (var_a_name, var_b_name) in &schema.joints {
        let val_a = row.variable_values.get(var_a_name);
        let val_b = row.variable_values.get(var_b_name);
        if let (Some(va), Some(vb)) = (val_a, val_b) {
            if let Some(indices) = resolve_joint_indices(schema, var_a_name, var_b_name, va, vb) {
                joint_indices.push(((var_a_name.clone(), var_b_name.clone()), indices));
            }
        }
    }

    let raw_values: serde_json::Map<String, Value> = row
        .variable_values
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    Some(ResolvedRow {
        dimension_key,
        variable_deltas,
        joint_indices,
        raw_values,
    })
}

fn resolve_value(variable: &crate::core::VariableDefinition, value: &Value) -> Option<SingleDelta> {
    match &variable.var_type {
        VariableType::Continuous { bins, range } => {
            let num = value.as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| value.as_f64())?;
            let (min, max) = range.unwrap_or((0.0, 1.0));
            let width = (max - min) / (*bins as f64);
            if width <= 0.0 {
                return None;
            }
            let raw_index = ((num - min) / width).floor() as isize;
            let index = raw_index.clamp(0, *bins as isize - 1) as usize;
            Some(SingleDelta::HistogramIdx(index))
        }
        VariableType::Categorical { categories, allow_unknown } => {
            let val = value
                .as_str()
                .map(ToOwned::to_owned)
                .or_else(|| value.as_i64().map(|v| v.to_string()))
                .unwrap_or_else(|| "unknown".to_owned());
            let idx = categories.iter().position(|c| c == &val);
            if idx.is_none() && !allow_unknown {
                return None;
            }
            Some(SingleDelta::CategoricalIdx(idx))
        }
    }
}

fn resolve_joint_indices(
    schema: &Schema,
    var_a: &str,
    var_b: &str,
    val_a: &Value,
    val_b: &Value,
) -> Option<(usize, usize)> {
    let def_a = schema.variables.iter().find(|v| v.name == var_a)?;
    let def_b = schema.variables.iter().find(|v| v.name == var_b)?;

    match (&def_a.var_type, &def_b.var_type) {
        (VariableType::Categorical { categories: ca, .. }, VariableType::Categorical { categories: cb, .. }) => {
            let sa = val_a.as_str()?;
            let sb = val_b.as_str()?;
            let xi = ca.iter().position(|c| c == sa)?;
            let yi = cb.iter().position(|c| c == sb)?;
            Some((xi, yi))
        }
        (VariableType::Continuous { bins, range }, VariableType::Continuous { bins: bins_b, range: range_b }) => {
            let a = val_a.as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| val_a.as_f64())?;
            let b = val_b.as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| val_b.as_f64())?;
            let (xmin, xmax) = range.unwrap_or((0.0, 1.0));
            let (ymin, ymax) = range_b.unwrap_or((0.0, 1.0));
            let xw = (xmax - xmin) / *bins as f64;
            let yw = (ymax - ymin) / *bins_b as f64;
            if xw <= 0.0 || yw <= 0.0 { return None; }
            let xi = ((a - xmin) / xw).floor() as isize;
            let yi = ((b - ymin) / yw).floor() as isize;
            Some((xi.clamp(0, *bins as isize - 1) as usize, yi.clamp(0, *bins_b as isize - 1) as usize))
        }
        (VariableType::Categorical { categories, .. }, VariableType::Continuous { bins, range }) => {
            let cat = val_a.as_str()?;
            let num = val_b.as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| val_b.as_f64())?;
            let ci = categories.iter().position(|c| c == cat)?;
            let (min, max) = range.unwrap_or((0.0, 1.0));
            let w = (max - min) / *bins as f64;
            if w <= 0.0 { return None; }
            let bi = ((num - min) / w).floor() as isize;
            Some((ci, bi.clamp(0, *bins as isize - 1) as usize))
        }
        (VariableType::Continuous { bins, range }, VariableType::Categorical { categories, .. }) => {
            let num = val_a.as_str().and_then(|s| s.parse::<f64>().ok()).or_else(|| val_a.as_f64())?;
            let cat = val_b.as_str()?;
            let ci = categories.iter().position(|c| c == cat)?;
            let (min, max) = range.unwrap_or((0.0, 1.0));
            let w = (max - min) / *bins as f64;
            if w <= 0.0 { return None; }
            let bi = ((num - min) / w).floor() as isize;
            Some((ci, bi.clamp(0, *bins as isize - 1) as usize))
        }
    }
}

pub fn normalize_dimension_value(value: &str, granularity: Option<&str>) -> String {
    match granularity {
        Some("daily") => parse_datetime(value)
            .map(|dt| dt.date().format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| value.to_owned()),
        Some("weekly") => parse_datetime(value)
            .map(|dt| {
                let iso = dt.iso_week();
                format!("{}-W{:02}", iso.year(), iso.week())
            })
            .unwrap_or_else(|| value.to_owned()),
        Some("monthly") => parse_datetime(value)
            .map(|dt| dt.date().format("%Y-%m").to_string())
            .unwrap_or_else(|| value.to_owned()),
        Some("yearly") => parse_datetime(value)
            .map(|dt| dt.date().format("%Y").to_string())
            .unwrap_or_else(|| value.to_owned()),
        _ => value.to_owned(),
    }
}

fn parse_datetime(value: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S")
        .ok()
        .or_else(|| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok().and_then(|d| d.and_hms_opt(0, 0, 0)))
}
