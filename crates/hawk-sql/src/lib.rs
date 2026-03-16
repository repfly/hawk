pub mod executor;
pub mod formatter;
pub mod parser;
pub mod tokenizer;

use anyhow::Result;

use hawk_query::QueryEngine;
use hawk_storage::Database;

use crate::formatter::QueryResult;

/// Execute a Hawk query against a database.
pub fn query(db: &Database, engine: &QueryEngine, sql: &str) -> Result<QueryResult> {
    let stmt = parser::parse(sql).map_err(|e| anyhow::anyhow!("parse error: {}", e))?;
    executor::execute(db, engine, &stmt)
}
