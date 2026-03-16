pub mod cache;
pub mod compare;
pub mod explain;
pub mod parser;
pub mod planner;
pub mod result_types;
pub mod track;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};

use hawk_storage::Database;

use hawk_core::{canonical_dimension_key, dimension_key_from_pairs};

use crate::{
    cache::QueryCache,
    compare::execute_compare,
    explain::execute_explain,
    parser::parse_reference,
    result_types::{
        CompareResult, CondMutualInfoResult, CorrelationReport, DimensionMI, ExplainResult,
        TrackResult, VariablePairCorrelation,
    },
    track::execute_track,
};

pub struct QueryEngine {
    cache: Arc<QueryCache>,
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self {
            cache: Arc::new(QueryCache::new(10_000)),
        }
    }
}

impl QueryEngine {
    pub fn compare(
        &self,
        db: &Database,
        ref_a: &str,
        ref_b: &str,
        variable_override: Option<&str>,
    ) -> Result<CompareResult> {
        let a = parse_reference(ref_a)?;
        let b = parse_reference(ref_b)?;
        let variable = variable_override
            .map(ToOwned::to_owned)
            .or(a.variable.clone())
            .or(b.variable.clone())
            .or_else(|| db.schema().first_variable_name().map(ToOwned::to_owned))
            .ok_or_else(|| anyhow!("no variable available in schema"))?;

        execute_compare(db, &self.cache, &variable, &a.dimensions, &b.dimensions)
    }

    pub fn explain(&self, db: &Database, ref_a: &str, ref_b: &str) -> Result<ExplainResult> {
        let a = parse_reference(ref_a)?;
        let b = parse_reference(ref_b)?;
        execute_explain(db, &self.cache, &a.dimensions, &b.dimensions)
    }

    pub fn track(
        &self,
        db: &Database,
        reference: &str,
        start: Option<&str>,
        end: Option<&str>,
        granularity: Option<&str>,
    ) -> Result<TrackResult> {
        let reference = parse_reference(reference)?;
        let variable = reference
            .variable
            .or_else(|| db.schema().first_variable_name().map(ToOwned::to_owned))
            .ok_or_else(|| anyhow!("no variable available in schema"))?;

        execute_track(db, &self.cache, &variable, &reference.dimensions, start, end, granularity)
    }

    pub fn mutual_info(
        &self,
        db: &Database,
        var_a: &str,
        var_b: &str,
        dimension_ref: &str,
    ) -> Result<f64> {
        let parsed = parse_reference(dimension_ref)?;
        let dim_key = dimension_key_from_pairs(parsed.dimensions.iter().map(|(k, v)| (k.clone(), v.clone())));

        let has_joint = db.schema().joints.iter().any(|(a, b)| {
            (a == var_a && b == var_b) || (a == var_b && b == var_a)
        });
        if !has_joint {
            return Err(anyhow!(
                "no joint distribution defined for '{}' and '{}'; call define_joint() first",
                var_a, var_b
            ));
        }

        let joint = db
            .get_joint_distribution(var_a, var_b, &dim_key)
            .ok_or_else(|| anyhow!("joint distribution not found for '{}' and '{}' at {:?}", var_a, var_b, dim_key))?;

        use hawk_core::JointRepr;
        let (counts, total) = match &joint.repr {
            JointRepr::HistogramGrid { counts, total_count, .. } => (counts.clone(), *total_count),
            JointRepr::ContingencyTable { counts, total_count, .. } => (counts.clone(), *total_count),
            JointRepr::ConditionalHistograms { histograms, total_count, .. } => {
                let grid: Vec<Vec<u64>> = histograms
                    .iter()
                    .map(|h| h.value_count_vector())
                    .collect();
                (grid, *total_count)
            }
        };

        Ok(hawk_math::mutual_information(&counts, total))
    }

    /// Conditional mutual information: MI(X; Y | Z)
    ///
    /// For each value of `conditioning_dimension`, computes MI(X;Y) on the
    /// joint distribution restricted to that value, then returns the
    /// weighted average (weighted by sample proportion).
    pub fn conditional_mutual_info(
        &self,
        db: &Database,
        var_a: &str,
        var_b: &str,
        conditioning_dimension: &str,
        filter_dims: Option<&HashMap<String, String>>,
    ) -> Result<CondMutualInfoResult> {
        let has_joint = db.schema().joints.iter().any(|(a, b)| {
            (a == var_a && b == var_b) || (a == var_b && b == var_a)
        });
        if !has_joint {
            return Err(anyhow!(
                "no joint distribution defined for '{}' and '{}'; call define_joint() first",
                var_a, var_b
            ));
        }

        let dim_values: Vec<String> = db.dimension_values(conditioning_dimension).into_iter().collect();
        if dim_values.is_empty() {
            return Err(anyhow!("no values found for dimension '{}'", conditioning_dimension));
        }

        let mut slices = Vec::new();
        let mut per_value = Vec::new();

        for value in &dim_values {
            let mut dim_key_map = std::collections::BTreeMap::new();
            dim_key_map.insert(conditioning_dimension.to_owned(), value.clone());
            if let Some(extra) = filter_dims {
                for (k, v) in extra {
                    dim_key_map.insert(k.clone(), v.clone());
                }
            }
            let dim_key = hawk_core::DimensionKey::from(dim_key_map);

            let joint = match db.get_joint_distribution(var_a, var_b, &dim_key) {
                Some(j) => j,
                None => continue,
            };

            let (counts, total) = Self::extract_joint_counts(joint);

            if total > 0 {
                let mi = hawk_math::mutual_information(&counts, total);
                let nmi = hawk_math::normalized_mutual_information(&counts, total);
                let cv = hawk_math::cramers_v(&counts, total);
                per_value.push(DimensionMI {
                    value: value.clone(),
                    mi,
                    nmi,
                    cramers_v: cv,
                    sample_count: total,
                });
                slices.push((counts, total));
            }
        }

        if slices.is_empty() {
            return Err(anyhow!("no joint distributions found across dimension '{}'", conditioning_dimension));
        }

        let cmi = hawk_math::conditional_mutual_information(&slices);
        let total_samples: u64 = per_value.iter().map(|p| p.sample_count).sum();

        Ok(CondMutualInfoResult {
            cmi,
            total_samples,
            conditioning_dimension: conditioning_dimension.to_owned(),
            per_value,
        })
    }

    /// Discover correlations by ranking all joint-defined variable pairs by MI.
    ///
    /// If `dimension` is provided, computes MI per dimension value and returns
    /// the top-k strongest correlations across all values.
    /// If `dimension` is None, computes MI across all available joint distributions.
    pub fn discover_correlations(
        &self,
        db: &Database,
        dimension: Option<&str>,
        top_k: usize,
    ) -> Result<CorrelationReport> {
        let joints = &db.schema().joints;
        if joints.is_empty() {
            return Err(anyhow!("no joint distributions defined; call define_joint() first"));
        }

        let mut all_pairs: Vec<VariablePairCorrelation> = Vec::new();
        let mut total_scanned = 0usize;

        for (var_a, var_b) in joints {
            if let Some(dim_name) = dimension {
                let dim_values: Vec<String> = db.dimension_values(dim_name).into_iter().collect();
                for value in &dim_values {
                    let mut dim_key_map = std::collections::BTreeMap::new();
                    dim_key_map.insert(dim_name.to_owned(), value.clone());
                    let dim_key = hawk_core::DimensionKey::from(dim_key_map);

                    if let Some(joint) = db.get_joint_distribution(var_a, var_b, &dim_key) {
                        let (counts, total) = Self::extract_joint_counts(joint);
                        if total > 0 {
                            let mi = hawk_math::mutual_information(&counts, total);
                            let nmi = hawk_math::normalized_mutual_information(&counts, total);
                            let cv = hawk_math::cramers_v(&counts, total);
                            all_pairs.push(VariablePairCorrelation {
                                var_a: var_a.clone(),
                                var_b: var_b.clone(),
                                mi,
                                nmi,
                                cramers_v: cv,
                                sample_count: total,
                                dimension_value: Some(value.clone()),
                            });
                        }
                        total_scanned += 1;
                    }
                }
            } else {
                // Aggregate across all dimension keys for this pair
                let all_joints: Vec<_> = db.joints_for_pair(var_a, var_b);
                for joint in &all_joints {
                    let (counts, total) = Self::extract_joint_counts(joint);
                    if total > 0 {
                        let mi = hawk_math::mutual_information(&counts, total);
                        let nmi = hawk_math::normalized_mutual_information(&counts, total);
                        let cv = hawk_math::cramers_v(&counts, total);
                        let dim_label = canonical_dimension_key(&joint.dimension_key);
                        all_pairs.push(VariablePairCorrelation {
                            var_a: var_a.clone(),
                            var_b: var_b.clone(),
                            mi,
                            nmi,
                            cramers_v: cv,
                            sample_count: total,
                            dimension_value: if dim_label.is_empty() { None } else { Some(dim_label) },
                        });
                    }
                    total_scanned += 1;
                }
            }
        }

        // Sort by MI descending
        all_pairs.sort_by(|a, b| b.mi.total_cmp(&a.mi));
        all_pairs.truncate(top_k);

        Ok(CorrelationReport {
            pairs: all_pairs,
            dimension: dimension.map(ToOwned::to_owned),
            total_pairs_scanned: total_scanned,
        })
    }

    fn extract_joint_counts(joint: &hawk_core::JointDistributionObject) -> (Vec<Vec<u64>>, u64) {
        use hawk_core::JointRepr;
        match &joint.repr {
            JointRepr::HistogramGrid { counts, total_count, .. } => (counts.clone(), *total_count),
            JointRepr::ContingencyTable { counts, total_count, .. } => (counts.clone(), *total_count),
            JointRepr::ConditionalHistograms { histograms, total_count, .. } => {
                let grid: Vec<Vec<u64>> = histograms.iter().map(|h| h.value_count_vector()).collect();
                (grid, *total_count)
            }
        }
    }

    pub fn pairwise(
        &self,
        db: &Database,
        dimension: &str,
        variable: &str,
        metric: &str,
    ) -> Result<(Vec<String>, Vec<Vec<f64>>)> {
        let values: Vec<String> = db.dimension_values(dimension).into_iter().collect();
        let n = values.len();
        let mut matrix = vec![vec![0.0_f64; n]; n];

        for i in 0..n {
            for j in (i + 1)..n {
                let mut dims_a = std::collections::HashMap::new();
                dims_a.insert(dimension.to_owned(), values[i].clone());
                let mut dims_b = std::collections::HashMap::new();
                dims_b.insert(dimension.to_owned(), values[j].clone());

                let cmp = execute_compare(db, &self.cache, variable, &dims_a, &dims_b)?;
                let distance = if metric.eq_ignore_ascii_case("wasserstein") {
                    cmp.wasserstein.unwrap_or(cmp.jsd)
                } else {
                    cmp.jsd
                };
                matrix[i][j] = distance;
                matrix[j][i] = distance;
            }
        }

        Ok((values, matrix))
    }
}
