use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde_json::Value;

const DEFAULT_MAX_SEGMENT_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_MAX_TOTAL_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct RawLog {
    raw_dir: PathBuf,
    max_segment_bytes: u64,
    max_total_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct RawLogOptions {
    pub max_segment_bytes: u64,
    pub max_total_bytes: u64,
}

impl Default for RawLogOptions {
    fn default() -> Self {
        Self {
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
            max_total_bytes: DEFAULT_MAX_TOTAL_BYTES,
        }
    }
}

impl RawLog {
    pub fn new(raw_dir: &Path) -> Result<Self> {
        Self::new_with_options(raw_dir, RawLogOptions::default())
    }

    pub fn new_with_options(raw_dir: &Path, options: RawLogOptions) -> Result<Self> {
        eprintln!(
            "warning: raw logs are enabled; original records may be retained under {}",
            raw_dir.display()
        );
        fs::create_dir_all(raw_dir)
            .with_context(|| format!("create raw dir {}", raw_dir.display()))?;
        let segment_path = raw_dir.join(segment_name(1));
        if !segment_path.exists() {
            fs::write(&segment_path, b"")
                .with_context(|| format!("create {}", segment_path.display()))?;
        }
        Ok(Self {
            raw_dir: raw_dir.to_path_buf(),
            max_segment_bytes: options.max_segment_bytes,
            max_total_bytes: options.max_total_bytes,
        })
    }

    pub fn append(&self, record_id: u64, payload: &Value) -> Result<()> {
        let bytes = self.serialize_line(record_id, payload)?;
        let segment_path = self.active_segment_path(bytes.len() as u64)?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&segment_path)
            .with_context(|| format!("open raw segment {}", segment_path.display()))?;

        file.write_all(&bytes).context("append raw line")?;
        self.enforce_retention()?;
        Ok(())
    }

    fn serialize_line(&self, record_id: u64, payload: &Value) -> Result<Vec<u8>> {
        let mut line = serde_json::Map::new();
        line.insert("record_id".into(), Value::from(record_id));
        line.insert("payload".into(), payload.clone());

        let mut bytes = serde_json::to_vec(&line).context("serialize raw line")?;
        bytes.push(b'\n');
        Ok(bytes)
    }

    fn active_segment_path(&self, next_bytes: u64) -> Result<PathBuf> {
        let segments = self.segments()?;
        let Some((last_number, last_path, last_size)) = segments.last() else {
            let path = self.raw_dir.join(segment_name(1));
            fs::write(&path, b"").with_context(|| format!("create {}", path.display()))?;
            return Ok(path);
        };

        if *last_size > 0 && last_size.saturating_add(next_bytes) > self.max_segment_bytes {
            let next = last_number + 1;
            let path = self.raw_dir.join(segment_name(next));
            fs::write(&path, b"").with_context(|| format!("create {}", path.display()))?;
            Ok(path)
        } else {
            Ok(last_path.clone())
        }
    }

    fn enforce_retention(&self) -> Result<()> {
        let mut segments = self.segments()?;
        let mut total: u64 = segments.iter().map(|(_, _, size)| *size).sum();

        while total > self.max_total_bytes && segments.len() > 1 {
            let (_, path, size) = segments.remove(0);
            fs::remove_file(&path)
                .with_context(|| format!("remove raw segment {}", path.display()))?;
            total = total.saturating_sub(size);
        }

        Ok(())
    }

    fn segments(&self) -> Result<Vec<(u64, PathBuf, u64)>> {
        let mut segments = Vec::new();
        for entry in fs::read_dir(&self.raw_dir)
            .with_context(|| format!("read raw dir {}", self.raw_dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(number) = segment_number(name) else {
                continue;
            };
            let size = entry.metadata()?.len();
            segments.push((number, path, size));
        }
        segments.sort_by_key(|(number, _, _)| *number);
        Ok(segments)
    }
}

fn segment_name(number: u64) -> String {
    format!("log_{number:06}.raw")
}

fn segment_number(name: &str) -> Option<u64> {
    name.strip_prefix("log_")?
        .strip_suffix(".raw")?
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("hawk-raw-log-test-{}-{}", name, std::process::id()))
    }

    #[test]
    fn raw_log_rotates_segments() {
        let root = test_dir("rotate");
        let _ = fs::remove_dir_all(&root);
        let raw_log = RawLog::new_with_options(
            &root,
            RawLogOptions {
                max_segment_bytes: 80,
                max_total_bytes: 10_000,
            },
        )
        .expect("raw log");

        raw_log.append(1, &Value::from("first")).expect("append 1");
        raw_log.append(2, &Value::from("second")).expect("append 2");
        raw_log.append(3, &Value::from("third")).expect("append 3");

        let segments = raw_log.segments().expect("segments");
        assert!(segments.len() >= 2);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn raw_log_retention_removes_old_segments() {
        let root = test_dir("retention");
        let _ = fs::remove_dir_all(&root);
        let raw_log = RawLog::new_with_options(
            &root,
            RawLogOptions {
                max_segment_bytes: 70,
                max_total_bytes: 140,
            },
        )
        .expect("raw log");

        for i in 0..8 {
            raw_log
                .append(i, &Value::from(format!("record-{i}-with-padding")))
                .expect("append");
        }

        let segments = raw_log.segments().expect("segments");
        let total: u64 = segments.iter().map(|(_, _, size)| *size).sum();
        assert!(!segments.is_empty());
        assert!(total <= 140 || segments.len() == 1);
        let _ = fs::remove_dir_all(root);
    }
}
