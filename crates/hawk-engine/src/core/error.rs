use thiserror::Error;

#[derive(Debug, Error)]
pub enum HawkError {
    #[error("schema validation error: {0}")]
    SchemaValidation(String),
    #[error("distribution not found: {0}")]
    DistributionNotFound(String),
    #[error("type mismatch: {0}")]
    TypeMismatch(String),
    #[error("invalid query reference: {0}")]
    InvalidReference(String),
    #[error("insufficient samples: {0}")]
    InsufficientSamples(String),
    #[error("no snapshots available: {0}")]
    NoSnapshots(String),
    #[error("no joint distribution defined: {0}")]
    NoJointDefined(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}

pub type Result<T> = std::result::Result<T, HawkError>;
