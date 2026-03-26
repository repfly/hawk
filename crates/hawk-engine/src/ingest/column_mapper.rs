use std::collections::HashMap;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::core::Schema;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IngestMapping {
    pub variables: HashMap<String, String>,
    pub dimensions: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct MappedRow {
    pub variable_values: HashMap<String, Value>,
    pub dimension_values: HashMap<String, String>,
}

pub fn validate_mapping(schema: &Schema, mapping: &IngestMapping) -> Result<()> {
    // At least one mapped variable must exist in the schema
    let mapped_vars: Vec<&String> = mapping
        .variables
        .values()
        .filter(|v| schema.variables.iter().any(|sv| &sv.name == *v))
        .collect();
    if mapped_vars.is_empty() {
        return Err(anyhow!(
            "mapping must include at least one variable defined in the schema"
        ));
    }

    // Validate that mapped variable names actually exist in schema
    for var_name in mapping.variables.values() {
        if !schema.variables.iter().any(|v| &v.name == var_name) {
            return Err(anyhow!(
                "mapped variable '{}' does not exist in schema",
                var_name
            ));
        }
    }

    // All dimensions must be mapped (dimensions are the join key across sources)
    for dimension in &schema.dimensions {
        let exists = mapping.dimensions.values().any(|v| v == &dimension.name);
        if !exists {
            return Err(anyhow!("missing mapping for dimension '{}'", dimension.name));
        }
    }

    Ok(())
}

pub fn map_row(raw_row: &Map<String, Value>, mapping: &IngestMapping) -> Option<MappedRow> {
    let mut variable_values = HashMap::new();
    let mut dimension_values = HashMap::new();

    for (raw_col, var_name) in &mapping.variables {
        if let Some(v) = raw_row.get(raw_col) {
            variable_values.insert(var_name.clone(), v.clone());
        }
    }

    for (raw_col, dim_name) in &mapping.dimensions {
        let raw = raw_row.get(raw_col)?;
        let dim_value = match raw {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => return None,
        };
        dimension_values.insert(dim_name.clone(), dim_value);
    }

    Some(MappedRow {
        variable_values,
        dimension_values,
    })
}
