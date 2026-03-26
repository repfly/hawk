pub mod executor;
pub mod formatter;
pub mod parser;
pub mod tokenizer;

use anyhow::Result;

use crate::query::QueryEngine;
use crate::storage::Database;

use crate::sql::formatter::QueryResult;

/// Execute a Hawk query against a database.
pub fn query(db: &Database, engine: &QueryEngine, sql: &str) -> Result<QueryResult> {
    let stmt = parser::parse(sql).map_err(|e| anyhow::anyhow!("parse error: {}", e))?;
    executor::execute(db, engine, &stmt)
}
