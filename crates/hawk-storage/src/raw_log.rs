use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct RawLog {
    segment_path: PathBuf,
}

impl RawLog {
    pub fn new(raw_dir: &Path) -> Result<Self> {
        fs::create_dir_all(raw_dir).with_context(|| format!("create raw dir {}", raw_dir.display()))?;
        let segment_path = raw_dir.join("log_000001.raw");
        if !segment_path.exists() {
            fs::write(&segment_path, b"").with_context(|| format!("create {}", segment_path.display()))?;
        }
        Ok(Self { segment_path })
    }

    pub fn append(&self, record_id: u64, payload: &Value) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.segment_path)
            .with_context(|| format!("open raw segment {}", self.segment_path.display()))?;

        let mut line = serde_json::Map::new();
        line.insert("record_id".into(), Value::from(record_id));
        line.insert("payload".into(), payload.clone());

        let bytes = serde_json::to_vec(&line).context("serialize raw line")?;
        file.write_all(&bytes).context("append raw line")?;
        file.write_all(b"\n").context("append newline")?;
        Ok(())
    }
}
