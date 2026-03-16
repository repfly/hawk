use std::{fs, path::Path};

use anyhow::{anyhow, Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use hawk_core::{DistributionObject, JointDistributionObject, Schema};

use crate::{dist_index::DistIndex, snapshot_store::SnapshotStore};

/// Magic bytes: "HAWK" in ASCII
const MAGIC: [u8; 4] = [0x48, 0x41, 0x57, 0x4B];
/// Current binary format version (3 = bincode + zstd)
const FORMAT_VERSION: u32 = 3;
/// zstd compression level (3 = good balance of speed and ratio)
const ZSTD_LEVEL: i32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaFile {
    pub schema: Schema,
    pub next_distribution_id: u64,
    pub next_joint_id: u64,
    pub next_raw_record_id: u64,
    pub format_version: u32,
    #[serde(default = "default_true")]
    pub raw_log_enabled: bool,
    #[serde(default)]
    pub high_water_mark: u64,
}

fn default_true() -> bool {
    true
}

impl Default for MetaFile {
    fn default() -> Self {
        Self {
            schema: Schema::default(),
            next_distribution_id: 1,
            next_joint_id: 1,
            next_raw_record_id: 1,
            format_version: FORMAT_VERSION,
            raw_log_enabled: true,
            high_water_mark: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DistributionFile {
    pub distributions: Vec<DistributionObject>,
    pub joints: Vec<JointDistributionObject>,
}

/// Write a value to a binary file with a HAWK header + zstd compression.
///
/// File layout:
///   [4 bytes] magic "HAWK"
///   [4 bytes] format version (little-endian u32)
///   [rest]    zstd-compressed bincode payload
pub fn write_file<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let payload = bincode::serialize(value).context("serialize bincode")?;
    let compressed = zstd::encode_all(payload.as_slice(), ZSTD_LEVEL).context("zstd compress")?;
    let mut buf = Vec::with_capacity(8 + compressed.len());
    buf.extend_from_slice(&MAGIC);
    buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    buf.extend_from_slice(&compressed);
    fs::write(path, buf).with_context(|| format!("write file {}", path.display()))?;
    Ok(())
}

/// Read a value from a binary file.
/// Supports: v3 (bincode+zstd), v2 (bincode), v1/JSON (fallback).
pub fn read_file<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).with_context(|| format!("read file {}", path.display()))?;
    deserialize_bytes::<T>(&bytes, path)
}

/// Read a value from a memory-mapped file (zero-copy read from disk).
pub fn read_file_mmap<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let file = fs::File::open(path).with_context(|| format!("open file {}", path.display()))?;
    let mmap =
        unsafe { memmap2::Mmap::map(&file) }.with_context(|| format!("mmap {}", path.display()))?;
    deserialize_bytes::<T>(&mmap, path)
}

fn deserialize_bytes<T: DeserializeOwned>(bytes: &[u8], path: &Path) -> Result<T> {
    if bytes.len() >= 8 && bytes[..4] == MAGIC {
        let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if version > FORMAT_VERSION {
            return Err(anyhow!(
                "file {} has format version {} but this build supports up to {}",
                path.display(),
                version,
                FORMAT_VERSION
            ));
        }
        let payload = &bytes[8..];
        return if version >= 3 {
            // v3: zstd-compressed bincode
            let decompressed = zstd::decode_all(payload)
                .with_context(|| format!("zstd decompress {}", path.display()))?;
            bincode::deserialize(&decompressed)
                .with_context(|| format!("deserialize {}", path.display()))
        } else {
            // v2: raw bincode
            bincode::deserialize(payload)
                .with_context(|| format!("deserialize {}", path.display()))
        };
    }

    // Fallback: JSON (v1)
    serde_json::from_slice::<T>(bytes)
        .with_context(|| format!("parse {} (tried binary and JSON)", path.display()))
}

pub fn ensure_file<T>(path: &Path, default: &T) -> Result<()>
where
    T: Serialize,
{
    if !path.exists() {
        write_file(path, default)?;
    }
    Ok(())
}

pub fn rebuild_index(distributions: &[DistributionObject]) -> DistIndex {
    let mut index = DistIndex::default();
    for dist in distributions {
        index.insert(&dist.variable, &dist.dimension_key, dist.id);
    }
    index
}

pub fn ensure_snapshot_file(path: &Path) -> Result<()> {
    if !path.exists() {
        write_file(path, &SnapshotStore::default())?;
    }
    Ok(())
}
