use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::core::error::HawkError;
use crate::core::Result;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum VariableType {
    Continuous {
        bins: u32,
        range: Option<(f64, f64)>,
    },
    Categorical {
        categories: Vec<String>,
        allow_unknown: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VariableDefinition {
    pub name: String,
    pub var_type: VariableType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DimensionDefinition {
    pub name: String,
    pub source_column: String,
    pub granularity: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Schema {
    pub variables: Vec<VariableDefinition>,
    pub dimensions: Vec<DimensionDefinition>,
    pub joints: Vec<(String, String)>,
}

impl Schema {
    pub fn define_variable(&mut self, variable: VariableDefinition) -> Result<()> {
        validate_name("variable", &variable.name)?;
        if self.variables.iter().any(|v| v.name == variable.name) {
            return Err(HawkError::SchemaValidation(format!(
                "variable '{}' already exists",
                variable.name
            )));
        }
        match &variable.var_type {
            VariableType::Continuous { bins, range } => {
                if *bins == 0 {
                    return Err(HawkError::SchemaValidation(format!(
                        "variable '{}' must have bins > 0",
                        variable.name
                    )));
                }
                if let Some((min, max)) = range {
                    if !min.is_finite() || !max.is_finite() || min >= max {
                        return Err(HawkError::SchemaValidation(format!(
                            "variable '{}' must define a finite range with min < max",
                            variable.name
                        )));
                    }
                }
            }
            VariableType::Categorical { categories, .. } => {
                validate_categories(&variable.name, categories)?;
            }
        }
        self.variables.push(variable);
        Ok(())
    }

    pub fn define_dimension(&mut self, dimension: DimensionDefinition) -> Result<()> {
        validate_name("dimension", &dimension.name)?;
        if dimension.source_column.trim().is_empty() {
            return Err(HawkError::SchemaValidation(format!(
                "dimension '{}' must define a non-empty source column",
                dimension.name
            )));
        }
        if self.dimensions.iter().any(|d| d.name == dimension.name) {
            return Err(HawkError::SchemaValidation(format!(
                "dimension '{}' already exists",
                dimension.name
            )));
        }
        self.dimensions.push(dimension);
        Ok(())
    }

    pub fn define_joint(&mut self, var_a: &str, var_b: &str) -> Result<()> {
        if var_a == var_b {
            return Err(HawkError::SchemaValidation(
                "joint definition requires two distinct variables".to_owned(),
            ));
        }
        let var_exists = |name: &str| self.variables.iter().any(|v| v.name == name);
        if !var_exists(var_a) || !var_exists(var_b) {
            return Err(HawkError::SchemaValidation(format!(
                "both variables must exist for joint definition: '{}' and '{}'",
                var_a, var_b
            )));
        }

        let pair = if var_a <= var_b {
            (var_a.to_owned(), var_b.to_owned())
        } else {
            (var_b.to_owned(), var_a.to_owned())
        };

        if self.joints.iter().any(|p| p == &pair) {
            return Ok(());
        }

        self.joints.push(pair);
        Ok(())
    }

    pub fn first_variable_name(&self) -> Option<&str> {
        self.variables.first().map(|v| v.name.as_str())
    }
}

fn validate_name(kind: &str, name: &str) -> Result<()> {
    if name.trim().is_empty() {
        return Err(HawkError::SchemaValidation(format!(
            "{} name must not be empty",
            kind
        )));
    }
    Ok(())
}

fn validate_categories(variable_name: &str, categories: &[String]) -> Result<()> {
    if categories.is_empty() {
        return Err(HawkError::SchemaValidation(format!(
            "variable '{}' must define at least one category",
            variable_name
        )));
    }

    let mut seen = HashSet::with_capacity(categories.len());
    for category in categories {
        if category.trim().is_empty() {
            return Err(HawkError::SchemaValidation(format!(
                "variable '{}' must not define empty category labels",
                variable_name
            )));
        }
        if !seen.insert(category) {
            return Err(HawkError::SchemaValidation(format!(
                "variable '{}' has duplicate category '{}'",
                variable_name, category
            )));
        }
    }

    Ok(())
}
