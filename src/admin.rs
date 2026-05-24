//! Offline data-directory maintenance: backup, restore, and integrity scan.
//!
//! These functions are meant to be invoked from the CLI with the server
//! **stopped** (redb keeps an exclusive file lock while running). They do
//! not coordinate with a live process.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::storage::manifest::Hash;
use crate::storage::{MetaError, MetaStore};

#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    #[error("io {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("meta: {0}")]
    Meta(#[from] MetaError),
    #[error("snapshot is missing required file: {0}")]
    MissingSnapshotFile(PathBuf),
}

fn io_err(path: impl Into<PathBuf>, source: std::io::Error) -> AdminError {
    AdminError::Io {
        path: path.into(),
        source,
    }
}

#[derive(Debug, Clone, Default)]
pub struct BackupReport {
    pub buckets: u32,
    pub manifests: u32,
    pub parts_copied: u32,
    pub parts_missing: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RestoreReport {
    pub parts_copied: u32,
}

#[derive(Debug, Clone, Default)]
pub struct ScanReport {
    pub parts_checked: u32,
    pub parts_passed: u32,
    pub corrupted: Vec<String>,
}

/// Take an offline backup of `src` into `dst`. Copies `meta.redb` verbatim
/// and only the part files referenced by some manifest — skipping orphans
/// that GC has not yet reclaimed.
pub async fn backup(src: &Path, dst: &Path) -> Result<BackupReport, AdminError> {
    let meta = MetaStore::open(src.join("meta.redb")).await?;
    let buckets = meta.list_buckets().await?;
    let bucket_count = buckets.len() as u32;
    let mut manifests = 0u32;
    let mut referenced: HashSet<Hash> = HashSet::new();
    for (name, _) in &buckets {
        let mfs = meta.list_all_manifests_in_bucket(name).await?;
        manifests += mfs.len() as u32;
        for m in &mfs {
            for p in &m.parts {
                referenced.insert(p.hash);
            }
        }
    }
    // Drop the MetaStore (and its exclusive redb lock) before copying the file.
    drop(meta);

    std::fs::create_dir_all(dst).map_err(|e| io_err(dst, e))?;
    let dst_meta = dst.join("meta.redb");
    std::fs::copy(src.join("meta.redb"), &dst_meta).map_err(|e| io_err(&dst_meta, e))?;

    let src_parts = src.join("parts");
    let dst_parts = dst.join("parts");
    std::fs::create_dir_all(&dst_parts).map_err(|e| io_err(&dst_parts, e))?;

    let mut parts_copied = 0u32;
    let mut parts_missing = Vec::new();
    for hash in &referenced {
        let hex = hex::encode(hash);
        let rel = PathBuf::from(&hex[..2]).join(&hex[2..4]).join(&hex);
        let src_part = src_parts.join(&rel);
        let dst_part = dst_parts.join(&rel);
        if let Some(parent) = dst_part.parent() {
            std::fs::create_dir_all(parent).map_err(|e| io_err(parent, e))?;
        }
        match std::fs::copy(&src_part, &dst_part) {
            Ok(_) => parts_copied += 1,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                parts_missing.push(hex);
            }
            Err(e) => return Err(io_err(&src_part, e)),
        }
    }
    Ok(BackupReport {
        buckets: bucket_count,
        manifests,
        parts_copied,
        parts_missing,
    })
}

/// Restore a snapshot produced by `backup` into a new `target` data dir.
/// The target must not yet contain `meta.redb`.
pub fn restore(snapshot: &Path, target: &Path) -> Result<RestoreReport, AdminError> {
    let src_meta = snapshot.join("meta.redb");
    if !src_meta.exists() {
        return Err(AdminError::MissingSnapshotFile(src_meta));
    }
    std::fs::create_dir_all(target).map_err(|e| io_err(target, e))?;
    let dst_meta = target.join("meta.redb");
    if dst_meta.exists() {
        return Err(io_err(
            &dst_meta,
            std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "target meta.redb already exists; restore refuses to overwrite",
            ),
        ));
    }
    std::fs::copy(&src_meta, &dst_meta).map_err(|e| io_err(&dst_meta, e))?;

    let mut count = 0u32;
    let src_parts = snapshot.join("parts");
    let dst_parts = target.join("parts");
    if src_parts.exists() {
        copy_tree(&src_parts, &dst_parts, &mut count)?;
    } else {
        std::fs::create_dir_all(&dst_parts).map_err(|e| io_err(&dst_parts, e))?;
    }

    Ok(RestoreReport { parts_copied: count })
}

/// Verify every part file in `data_dir/parts/` has a blake3 hash matching
/// its filename. Useful as a last-resort integrity check (the plan calls
/// this `scan-rebuild`); the `rebuild` step is a future extension that
/// reconstructs manifests from surviving parts.
pub fn scan_rebuild(data_dir: &Path) -> Result<ScanReport, AdminError> {
    let parts_dir = data_dir.join("parts");
    let mut report = ScanReport::default();
    if parts_dir.exists() {
        walk_and_verify(&parts_dir, &mut report)?;
    }
    Ok(report)
}

// ---------------- helpers ----------------

fn copy_tree(src: &Path, dst: &Path, count: &mut u32) -> Result<(), AdminError> {
    std::fs::create_dir_all(dst).map_err(|e| io_err(dst, e))?;
    for entry in std::fs::read_dir(src).map_err(|e| io_err(src, e))? {
        let entry = entry.map_err(|e| io_err(src, e))?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        let ty = entry.file_type().map_err(|e| io_err(&src_path, e))?;
        if ty.is_dir() {
            copy_tree(&src_path, &dst_path, count)?;
        } else {
            std::fs::copy(&src_path, &dst_path).map_err(|e| io_err(&dst_path, e))?;
            *count += 1;
        }
    }
    Ok(())
}

fn walk_and_verify(dir: &Path, report: &mut ScanReport) -> Result<(), AdminError> {
    for entry in std::fs::read_dir(dir).map_err(|e| io_err(dir, e))? {
        let entry = entry.map_err(|e| io_err(dir, e))?;
        let path = entry.path();
        let ty = entry.file_type().map_err(|e| io_err(&path, e))?;
        if ty.is_dir() {
            walk_and_verify(&path, report)?;
            continue;
        }
        let bytes = std::fs::read(&path).map_err(|e| io_err(&path, e))?;
        let actual_hash: Hash = blake3::hash(&bytes).into();
        let actual_hex = hex::encode(actual_hash);
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_string();
        report.parts_checked += 1;
        if actual_hex == filename {
            report.parts_passed += 1;
        } else {
            report.corrupted.push(filename);
        }
    }
    Ok(())
}
