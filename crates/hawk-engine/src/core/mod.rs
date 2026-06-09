pub mod dimension_key;
pub mod distribution;
pub mod error;
pub mod schema;

pub use dimension_key::{
    canonical_dimension_key, dimension_key_from_pairs, parse_canonical_dimension_key, DimensionKey,
};
pub use distribution::{
    DistributionObject, DistributionRepr, JointDistributionObject, JointRepr, TrackPoint,
    UNKNOWN_CATEGORY_LABEL,
};
pub use error::{HawkError, Result};
pub use schema::{DimensionDefinition, Schema, VariableDefinition, VariableType};

#[cfg(test)]
mod tests;
