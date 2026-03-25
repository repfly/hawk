//! # Hawk
//!
//! Distribution-native analytics engine. Instead of storing raw rows, Hawk
//! digests data into compact probability distributions and lets you query them
//! with information-theoretic metrics.
//!
//! ```text
//! cargo add hawk-engine
//! ```
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use hawk::storage::{Database, OpenMode};
//! use hawk::query::QueryEngine;
//!
//! let db = Database::open("my.db", OpenMode::ReadOnly).unwrap();
//! let engine = QueryEngine::default();
//! let result = engine.compare(&db, "time:2023", "time:2024", None).unwrap();
//! println!("JSD = {}", result.jsd);
//! ```

/// Core types: distributions, schemas, dimension keys, errors.
pub use hawk_core as core;

/// Information-theoretic math: entropy, JSD, KL, PSI, Hellinger, MI, Cramér's V, Wasserstein.
pub use hawk_math as math;

/// Persistence layer: binary format, mmap, compression, file locking.
pub use hawk_storage as storage;

/// Data ingestion: CSV/JSON/Parquet → distributions.
pub use hawk_ingest as ingest;

/// Query engine: compare, explain, track, correlations.
pub use hawk_query as query;

/// SQL-like DSL parser and executor.
#[cfg(feature = "sql")]
pub use hawk_sql as sql;
