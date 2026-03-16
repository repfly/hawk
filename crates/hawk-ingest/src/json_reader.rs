use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use anyhow::{Context, Result};
use serde_json::{Map, Value};

pub fn read_json_lines_rows(path: &Path) -> Result<Vec<Map<String, Value>>> {
    let file = File::open(path).with_context(|| format!("open json file {}", path.display()))?;
    let reader = BufReader::new(file);

    let mut out = Vec::new();
    for line in reader.lines() {
        let line = line.context("read json line")?;
        let value: Value = serde_json::from_str(&line).context("parse json line")?;
        if let Value::Object(map) = value {
            out.push(map);
        }
    }

    Ok(out)
}
