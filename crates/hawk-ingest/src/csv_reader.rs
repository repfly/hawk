use std::path::Path;

use anyhow::{Context, Result};
use serde_json::{Map, Value};

pub fn read_csv_rows(path: &Path) -> Result<Vec<Map<String, Value>>> {
    let mut reader = csv::Reader::from_path(path)
        .with_context(|| format!("open csv file {}", path.display()))?;

    let headers = reader
        .headers()
        .context("read csv headers")?
        .iter()
        .map(|s| s.to_owned())
        .collect::<Vec<_>>();

    let mut out = Vec::new();
    for row in reader.records() {
        let record = row.context("read csv record")?;
        let mut map = Map::new();
        for (idx, value) in record.iter().enumerate() {
            if let Some(key) = headers.get(idx) {
                map.insert(key.clone(), Value::String(value.to_owned()));
            }
        }
        out.push(map);
    }

    Ok(out)
}
