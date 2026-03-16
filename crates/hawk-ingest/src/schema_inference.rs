use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use hawk_core::{DimensionDefinition, Schema, VariableDefinition, VariableType};

/// Configuration for automatic schema inference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferConfig {
    /// Number of rows to sample when inferring types.
    pub sample_size: usize,
    /// Maximum number of unique string values before a column is treated
    /// as a dimension rather than a categorical variable.
    pub max_categories: usize,
    /// Column names that should be treated as date dimensions regardless
    /// of content.
    pub date_columns: Vec<String>,
    /// Granularity applied to date dimensions (e.g. "daily", "monthly").
    pub date_granularity: String,
}

impl Default for InferConfig {
    fn default() -> Self {
        Self {
            sample_size: 1000,
            max_categories: 50,
            date_columns: Vec::new(),
            date_granularity: "daily".to_owned(),
        }
    }
}

/// Infer a [`Schema`] from a sample of JSON rows.
///
/// For each column present in the sample:
///   - If all non-null values parse as `f64` -> `Continuous` with min/max
///     range and 20 bins.
///   - If it is a string column with <= `config.max_categories` unique values
///     -> `Categorical`.
///   - If it is a string column with > `config.max_categories` unique values,
///     or it is listed in `config.date_columns` -> `Dimension`.
pub fn infer_schema(rows: &[Map<String, Value>], config: &InferConfig) -> Schema {
    let sample = if rows.len() > config.sample_size {
        &rows[..config.sample_size]
    } else {
        rows
    };

    // Collect column names preserving first-seen order.
    let mut column_order: Vec<String> = Vec::new();
    let mut column_set: HashSet<String> = HashSet::new();
    for row in sample {
        for key in row.keys() {
            if column_set.insert(key.clone()) {
                column_order.push(key.clone());
            }
        }
    }

    let mut schema = Schema::default();

    for col in &column_order {
        // Forced date columns become dimensions immediately.
        if config.date_columns.contains(col) {
            schema.dimensions.push(DimensionDefinition {
                name: col.clone(),
                source_column: col.clone(),
                granularity: Some(config.date_granularity.clone()),
            });
            continue;
        }

        let values: Vec<&Value> = sample
            .iter()
            .filter_map(|row| row.get(col))
            .filter(|v| !v.is_null())
            .collect();

        if values.is_empty() {
            continue;
        }

        // Check if all values are numeric (or numeric strings).
        let mut all_numeric = true;
        let mut min_val = f64::INFINITY;
        let mut max_val = f64::NEG_INFINITY;

        for v in &values {
            if let Some(n) = as_f64(v) {
                if n < min_val {
                    min_val = n;
                }
                if n > max_val {
                    max_val = n;
                }
            } else {
                all_numeric = false;
                break;
            }
        }

        if all_numeric && min_val.is_finite() && max_val.is_finite() {
            // Heuristic: integer columns that look like years → date dimension.
            let all_integer = values
                .iter()
                .all(|v| as_f64(v).map_or(false, |n| n.fract() == 0.0));
            let looks_like_year = all_integer
                && min_val >= 1900.0
                && max_val <= 2100.0;
            let unique_count = {
                let mut s = HashSet::new();
                for v in &values {
                    if let Some(n) = as_f64(v) {
                        s.insert(n.to_bits());
                    }
                }
                s.len()
            };
            // Treat as a dimension if values look like years.  Year columns
            // often have more unique values than max_categories but should
            // still be dimensions, so we use a generous upper bound (200).
            if looks_like_year && unique_count <= 200 {
                schema.dimensions.push(DimensionDefinition {
                    name: col.clone(),
                    source_column: col.clone(),
                    granularity: Some(config.date_granularity.clone()),
                });
                continue;
            }

            // Add a small margin so the max value falls inside the last bin.
            let margin = if (max_val - min_val).abs() < f64::EPSILON {
                1.0
            } else {
                (max_val - min_val) * 0.001
            };
            schema.variables.push(VariableDefinition {
                name: col.clone(),
                var_type: VariableType::Continuous {
                    bins: 20,
                    range: Some((min_val, max_val + margin)),
                },
            });
            continue;
        }

        // String-like column: collect unique values.
        let mut unique: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut looks_like_date = true;

        for v in &values {
            let s = value_to_string(v);
            if looks_like_date && !could_be_date(&s) {
                looks_like_date = false;
            }
            if seen.insert(s.clone()) {
                unique.push(s);
            }
        }

        if looks_like_date || unique.len() > config.max_categories {
            let granularity = if looks_like_date {
                Some(config.date_granularity.clone())
            } else {
                None
            };
            schema.dimensions.push(DimensionDefinition {
                name: col.clone(),
                source_column: col.clone(),
                granularity,
            });
        } else {
            schema.variables.push(VariableDefinition {
                name: col.clone(),
                var_type: VariableType::Categorical {
                    categories: unique,
                    allow_unknown: true,
                },
            });
        }
    }

    schema
}

/// Build an [`IngestMapping`] that maps every column to itself (identity).
pub fn identity_mapping(schema: &Schema) -> crate::column_mapper::IngestMapping {
    let mut mapping = crate::column_mapper::IngestMapping::default();
    for var in &schema.variables {
        mapping.variables.insert(var.name.clone(), var.name.clone());
    }
    for dim in &schema.dimensions {
        mapping
            .dimensions
            .insert(dim.source_column.clone(), dim.name.clone());
    }
    mapping
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn as_f64(v: &Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => v.to_string(),
    }
}

/// Very lightweight heuristic: a value "looks like a date" if it matches
/// common date-ish patterns (YYYY-MM-DD, YYYY/MM/DD, ISO-8601 prefix).
fn could_be_date(s: &str) -> bool {
    let s = s.trim();
    if s.len() < 8 {
        return false;
    }
    // YYYY-MM-DD or YYYY/MM/DD
    let bytes = s.as_bytes();
    if bytes.len() >= 10
        && bytes[0..4].iter().all(|b| b.is_ascii_digit())
        && (bytes[4] == b'-' || bytes[4] == b'/')
        && bytes[5..7].iter().all(|b| b.is_ascii_digit())
        && (bytes[7] == b'-' || bytes[7] == b'/')
        && bytes[8..10].iter().all(|b| b.is_ascii_digit())
    {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_numeric_column() {
        let rows: Vec<Map<String, Value>> = (0..10)
            .map(|i| {
                let mut m = Map::new();
                m.insert("score".into(), Value::from(i as f64));
                m
            })
            .collect();

        let schema = infer_schema(&rows, &InferConfig::default());
        assert_eq!(schema.variables.len(), 1);
        assert!(matches!(
            schema.variables[0].var_type,
            VariableType::Continuous { bins: 20, .. }
        ));
    }

    #[test]
    fn test_infer_categorical_column() {
        let cats = vec!["a", "b", "c"];
        let rows: Vec<Map<String, Value>> = cats
            .iter()
            .map(|c| {
                let mut m = Map::new();
                m.insert("color".into(), Value::String(c.to_string()));
                m
            })
            .collect();

        let schema = infer_schema(&rows, &InferConfig::default());
        assert_eq!(schema.variables.len(), 1);
        assert!(matches!(
            schema.variables[0].var_type,
            VariableType::Categorical { .. }
        ));
    }

    #[test]
    fn test_infer_date_dimension() {
        let rows: Vec<Map<String, Value>> = vec!["2024-01-01", "2024-02-15", "2024-03-20"]
            .into_iter()
            .map(|d| {
                let mut m = Map::new();
                m.insert("date".into(), Value::String(d.to_string()));
                m
            })
            .collect();

        let schema = infer_schema(&rows, &InferConfig::default());
        assert_eq!(schema.dimensions.len(), 1);
        assert_eq!(schema.dimensions[0].name, "date");
    }
}
