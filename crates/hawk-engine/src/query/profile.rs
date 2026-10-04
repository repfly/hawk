use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::core::VariableType;
use crate::math::mi_matrix;
use crate::storage::Database;

use crate::query::cache::QueryCache;
use crate::query::planner::resolve_distribution;
use crate::query::result_types::{
    ProfileAssociation, ProfileDimension, ProfileDrift, ProfileResult, ProfileVariable,
};
use crate::query::structure::resolve_joint_counts;
use crate::query::track::execute_track;

pub const PROFILE_TOP_ASSOCIATIONS: usize = 10;

/// One-call dataset card: variables (type, pooled entropy, samples),
/// dimensions with value counts, the strongest stored associations by MI
/// (pairs without a stored joint listed as unknown), the biggest recent drift
/// (latest vs previous time slice per variable, by JSD — skipped gracefully
/// when no time dimension exists), and a stored-objects/size summary.
pub fn execute_profile(db: &Database, cache: &QueryCache) -> Result<ProfileResult> {
    let schema = db.schema();

    let variables: Vec<ProfileVariable> = schema
        .variables
        .iter()
        .map(|v| {
            let (entropy, sample_count) =
                match resolve_distribution(db, &v.name, &HashMap::new(), None) {
                    Ok(d) => (d.entropy, d.sample_count),
                    Err(_) => (0.0, 0),
                };
            ProfileVariable {
                name: v.name.clone(),
                var_type: match &v.var_type {
                    VariableType::Continuous { .. } => "continuous".to_owned(),
                    VariableType::Categorical { .. } => "categorical".to_owned(),
                },
                entropy,
                sample_count,
            }
        })
        .collect();

    let dimensions: Vec<ProfileDimension> = schema
        .dimensions
        .iter()
        .map(|d| ProfileDimension {
            name: d.name.clone(),
            value_count: db.dimension_values(&d.name).len(),
        })
        .collect();

    // Associations from stored joints, pooled over all slices.
    let names: Vec<String> = schema.variables.iter().map(|v| v.name.clone()).collect();
    let matrix = mi_matrix(&names, |a, b| {
        resolve_joint_counts(db, a, b, &HashMap::new())
    });
    let top_associations: Vec<ProfileAssociation> = matrix
        .edges
        .iter()
        .take(PROFILE_TOP_ASSOCIATIONS)
        .map(|e| ProfileAssociation {
            var_a: e.var_a.clone(),
            var_b: e.var_b.clone(),
            mi: e.mi,
            sample_count: e.sample_count,
        })
        .collect();

    // Biggest latest-vs-previous shift across variables (needs a time
    // dimension with at least 2 populated slices; otherwise None).
    let mut biggest_drift: Option<ProfileDrift> = None;
    for v in &schema.variables {
        let Ok(track) = execute_track(db, cache, &v.name, &HashMap::new(), None, None, None, None)
        else {
            continue;
        };
        let n = track.time_points.len();
        if n < 2 || track.drift_series.is_empty() {
            continue;
        }
        let jsd = *track.drift_series.last().expect("non-empty drift series");
        if biggest_drift.as_ref().is_none_or(|d| jsd > d.jsd) {
            biggest_drift = Some(ProfileDrift {
                variable: v.name.clone(),
                time_from: track.time_points[n - 2].clone(),
                time_to: track.time_points[n - 1].clone(),
                jsd,
            });
        }
    }

    // Storage summary: object counts + approximate serialized size.
    let stats = db.stats();
    let mut approx_bytes: u64 = 0;
    for v in &schema.variables {
        for dist in db.distributions_for_variable(&v.name) {
            approx_bytes += bincode::serialized_size(dist).unwrap_or(0);
        }
    }
    let mut stored_joints = 0usize;
    let mut seen_joints = HashSet::new();
    for (a, b) in &schema.joints {
        for joint in db.joints_for_pair(a, b) {
            if seen_joints.insert(joint.id) {
                stored_joints += 1;
                approx_bytes += bincode::serialized_size(joint).unwrap_or(0);
            }
        }
    }

    Ok(ProfileResult {
        variables,
        dimensions,
        top_associations,
        unknown_pairs: matrix.unknown_pairs,
        biggest_drift,
        stored_distributions: stats.distributions,
        stored_joints,
        total_samples: stats.total_samples,
        approx_bytes,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use serde_json::Value;

    use crate::core::{DimensionDefinition, VariableDefinition, VariableType};
    use crate::ingest::batch_updater::apply_batch;
    use crate::ingest::column_mapper::MappedRow;
    use crate::query::QueryEngine;
    use crate::storage::Database;

    fn temp_db(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("hawk-profile-test-{}-{}", name, std::process::id()))
    }

    fn categorical(name: &str, categories: &[&str]) -> VariableDefinition {
        VariableDefinition {
            name: name.to_owned(),
            var_type: VariableType::Categorical {
                categories: categories.iter().map(|c| (*c).to_owned()).collect(),
                allow_unknown: false,
            },
        }
    }

    fn row(dim: (&str, &str), values: &[(&str, &str)]) -> MappedRow {
        let mut variable_values = HashMap::new();
        for (var, val) in values {
            variable_values.insert((*var).to_owned(), Value::from(*val));
        }
        let mut dimension_values = HashMap::new();
        dimension_values.insert(dim.0.to_owned(), dim.1.to_owned());
        MappedRow {
            variable_values,
            dimension_values,
        }
    }

    /// Two correlated variables (stored joint) plus one that drifts hard in
    /// the latest month; the pairs with `moving` have no stored joint.
    fn build_db(root: &std::path::Path, time_dim: bool) -> Database {
        let _ = std::fs::remove_dir_all(root);
        let mut db = Database::create_with_options(root, false).expect("create db");

        db.define_variable(categorical("color", &["red", "blue"]))
            .unwrap();
        db.define_variable(categorical("shape", &["round", "square"]))
            .unwrap();
        db.define_variable(categorical("moving", &["up", "down"]))
            .unwrap();
        let dim = if time_dim { "time" } else { "region" };
        db.define_dimension(DimensionDefinition {
            name: dim.to_owned(),
            source_column: dim.to_owned(),
            granularity: None,
        })
        .unwrap();
        db.define_joint("color", "shape").unwrap();

        let slices = if time_dim {
            ["2025-01", "2025-02"]
        } else {
            ["north", "south"]
        };
        let mut rows = Vec::new();
        for (idx, slice) in slices.iter().enumerate() {
            for i in 0..20 {
                // color perfectly predicts shape → strong stored MI.
                let (color, shape) = if i % 2 == 0 {
                    ("red", "round")
                } else {
                    ("blue", "square")
                };
                let moving = if idx == 0 { "up" } else { "down" };
                rows.push(row(
                    (dim, slice),
                    &[("color", color), ("shape", shape), ("moving", moving)],
                ));
            }
        }
        let schema = db.schema().clone();
        apply_batch(&mut db, &schema, &rows).expect("apply batch");
        db
    }

    #[test]
    fn profile_reports_variables_dimensions_associations_and_size() {
        let root = temp_db("card");
        let db = build_db(&root, true);
        let engine = QueryEngine::default();

        let profile = engine.profile(&db).unwrap();

        assert_eq!(profile.variables.len(), 3);
        let color = profile
            .variables
            .iter()
            .find(|v| v.name == "color")
            .unwrap();
        assert_eq!(color.var_type, "categorical");
        assert!((color.entropy - 1.0).abs() < 1e-9);
        assert_eq!(color.sample_count, 40);

        assert_eq!(profile.dimensions.len(), 1);
        assert_eq!(profile.dimensions[0].name, "time");
        assert_eq!(profile.dimensions[0].value_count, 2);

        // The stored joint carries 1 bit of MI; unstored pairs are unknown.
        assert_eq!(profile.top_associations.len(), 1);
        let assoc = &profile.top_associations[0];
        assert_eq!(
            (assoc.var_a.as_str(), assoc.var_b.as_str()),
            ("color", "shape")
        );
        assert!((assoc.mi - 1.0).abs() < 1e-9);
        assert_eq!(
            profile.unknown_pairs,
            vec![
                ("color".to_owned(), "moving".to_owned()),
                ("moving".to_owned(), "shape".to_owned()),
            ]
        );

        assert!(profile.stored_distributions > 0);
        assert_eq!(profile.stored_joints, 2); // one per time slice
        assert_eq!(profile.total_samples, 120);
        assert!(profile.approx_bytes > 0);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn profile_finds_biggest_recent_drift_over_time() {
        let root = temp_db("drift");
        let db = build_db(&root, true);
        let engine = QueryEngine::default();

        let drift = engine.profile(&db).unwrap().biggest_drift.unwrap();
        assert_eq!(drift.variable, "moving");
        assert_eq!(drift.time_from, "2025-01");
        assert_eq!(drift.time_to, "2025-02");
        assert!(drift.jsd > 0.9);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn profile_skips_drift_gracefully_without_a_time_dimension() {
        let root = temp_db("no-time");
        let db = build_db(&root, false);
        let engine = QueryEngine::default();

        let profile = engine.profile(&db).unwrap();
        assert!(profile.biggest_drift.is_none());
        assert_eq!(profile.variables.len(), 3);
        let _ = std::fs::remove_dir_all(root);
    }
}
