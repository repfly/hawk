use std::path::Path;

use anyhow::{anyhow, Result};
use serde_json::{Map, Value};

pub fn read_parquet_rows(_path: &Path) -> Result<Vec<Map<String, Value>>> {
    Err(anyhow!(
        "parquet ingestion feature is scaffolded but Arrow/Parquet reader wiring is pending"
    ))
}
