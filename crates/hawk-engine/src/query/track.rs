use std::collections::{BTreeMap, HashMap};

use anyhow::Result;

use crate::storage::Database;

use crate::query::cache::QueryCache;
use crate::query::compare::execute_compare;
use crate::query::result_types::{DistributionSummary, DriftEvent, TrackResult};

pub fn execute_track(
    db: &Database,
    cache: &QueryCache,
    variable: &str,
    dims: &HashMap<String, String>,
    start: Option<&str>,
    end: Option<&str>,
    _granularity: Option<&str>,
) -> Result<TrackResult> {
    let mut grouped = BTreeMap::<String, (String, HashMap<String, String>)>::new();

    for dist in db.distributions_for_variable(variable) {
        let mut matches = true;
        for (k, v) in dims {
            if k == "time" {
                continue;
            }
            if dist.dimension_key.get(k) != Some(v) {
                matches = false;
                break;
            }
        }
        if !matches {
            continue;
        }

        let Some(time) = dist.dimension_key.get("time") else {
            continue;
        };

        if let Some(s) = start {
            if time.as_str() < s {
                continue;
            }
        }
        if let Some(e) = end {
            if time.as_str() > e {
                continue;
            }
        }

        let mut parts = dist.dimension_key.clone();
        parts.remove("time");
        grouped.insert(time.clone(), (time.clone(), parts.into_iter().collect()));
    }

    let mut time_points = Vec::new();
    let mut entropy_series = Vec::new();
    let mut snapshots = Vec::new();

    for (time_label, (_key, mut dims_without_time)) in grouped {
        dims_without_time.insert("time".to_owned(), time_label.clone());
        if let Some(dist) = db.find_distribution_by_reference(variable, &dims_without_time) {
            time_points.push(time_label.clone());
            entropy_series.push(dist.entropy);
            snapshots.push(DistributionSummary {
                reference: format!("{variable}|{time_label}"),
                sample_count: dist.sample_count,
                entropy: dist.entropy,
                version: dist.version,
            });
        }
    }

    let mut drift_series = Vec::new();
    let mut drift_events = Vec::new();

    for idx in 1..time_points.len() {
        let prev = &time_points[idx - 1];
        let curr = &time_points[idx];

        let mut d_prev = dims.clone();
        let mut d_curr = dims.clone();
        d_prev.insert("time".to_owned(), prev.clone());
        d_curr.insert("time".to_owned(), curr.clone());

        let compare = execute_compare(db, cache, variable, &d_prev, &d_curr)?;
        drift_series.push(compare.jsd);
    }

    if !drift_series.is_empty() {
        let mut sorted: Vec<f64> = drift_series.clone();
        sorted.sort_by(|a: &f64, b: &f64| a.total_cmp(b));
        let n = sorted.len();

        // IQR-based outlier detection (1.5 * IQR above Q3)
        let q1 = sorted[n / 4];
        let q3 = sorted[(3 * n) / 4];
        let iqr = q3 - q1;
        let threshold = q3 + 1.5 * iqr;

        // Fall back to median * 3 if IQR is zero (all values similar)
        let threshold = if iqr < 1e-12 {
            let median = sorted[n / 2];
            median * 3.0
        } else {
            threshold
        };

        for idx in 0..drift_series.len() {
            let value = drift_series[idx];
            if value > threshold && threshold > 0.0 {
                drift_events.push(DriftEvent {
                    time_from: time_points[idx].clone(),
                    time_to: time_points[idx + 1].clone(),
                    jsd: value,
                    description: format!("drift exceeded threshold {:.4} (IQR-based)", threshold),
                });
            }
        }
    }

    Ok(TrackResult {
        time_points,
        entropy_series,
        drift_series,
        drift_events,
        snapshots,
    })
}
