use std::{fs::File, path::Path};

use anyhow::{Context, Result};
use memmap2::Mmap;

pub struct ReadOnlyMmap {
    pub mmap: Mmap,
}

impl ReadOnlyMmap {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open file {}", path.display()))?;
        let mmap = unsafe { Mmap::map(&file).context("mmap file")? };
        Ok(Self { mmap })
    }
}
