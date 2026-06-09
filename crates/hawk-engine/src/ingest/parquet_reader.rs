use std::fs::File;
use std::io::BufRead;
use std::path::Path;

use anyhow::{Context, Result};
use arrow::json::writer::{LineDelimited, Writer};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Map, Value};

pub fn read_parquet_rows(path: &Path) -> Result<Vec<Map<String, Value>>> {
    let file = File::open(path).with_context(|| format!("open parquet file {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("read parquet metadata from {}", path.display()))?;
    let reader = builder
        .build()
        .with_context(|| format!("build parquet reader for {}", path.display()))?;

    let mut buf = Vec::new();
    {
        let mut writer = Writer::<_, LineDelimited>::new(&mut buf);
        for batch_result in reader {
            let batch = batch_result.context("read parquet record batch")?;
            writer.write(&batch).context("write record batch as JSON")?;
        }
        writer.finish().context("finish JSON writer")?;
    }

    let mut rows = Vec::new();
    for line in buf.lines() {
        let line = line.context("read JSON line from buffer")?;
        if line.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line).context("parse JSON line")?;
        if let Value::Object(map) = value {
            rows.push(map);
        }
    }

    Ok(rows)
}
