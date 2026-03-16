use std::{fs::File, path::Path};

use anyhow::{Context, Result};
use fs4::fs_std::FileExt;

pub struct DatabaseLock {
    file: File,
}

impl DatabaseLock {
    pub fn acquire(path: &Path) -> Result<Self> {
        let file = File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("open lock file {}", path.display()))?;
        file.lock_exclusive().context("acquire exclusive lock")?;
        Ok(Self { file })
    }
}

impl Drop for DatabaseLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}
