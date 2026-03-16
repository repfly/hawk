use serde::{Deserialize, Serialize};

use crate::{error::HawkError, Result};

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
        if self.variables.iter().any(|v| v.name == variable.name) {
            return Err(HawkError::SchemaValidation(format!(
                "variable '{}' already exists",
                variable.name
            )));
        }
        match &variable.var_type {
            VariableType::Continuous { bins, .. } if *bins == 0 => {
                return Err(HawkError::SchemaValidation(format!(
                    "variable '{}' must have bins > 0",
                    variable.name
                )));
            }
            VariableType::Categorical { categories, .. } if categories.is_empty() => {
                return Err(HawkError::SchemaValidation(format!(
                    "variable '{}' must define at least one category",
                    variable.name
                )));
            }
            _ => {}
        }
        self.variables.push(variable);
        Ok(())
    }

    pub fn define_dimension(&mut self, dimension: DimensionDefinition) -> Result<()> {
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
