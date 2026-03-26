pub mod dimension_key;
pub mod distribution;
pub mod error;
pub mod schema;

pub use dimension_key::{canonical_dimension_key, dimension_key_from_pairs, DimensionKey};
pub use distribution::{
    DistributionObject, DistributionRepr, JointDistributionObject, JointRepr, TrackPoint,
};
pub use error::{HawkError, Result};
pub use schema::{DimensionDefinition, Schema, VariableDefinition, VariableType};

#[cfg(test)]
mod tests;
