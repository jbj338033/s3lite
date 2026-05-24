use std::path::PathBuf;
use std::thread;

use tokio::sync::{mpsc, oneshot};

use crate::storage::manifest::{
    BucketConfig, DeletionEffect, Hash, Manifest, ManifestKey, ManifestState, PartEntry, PartRef,
};

mod actor;
mod errors;
mod tables;

pub use errors::MetaError;

/// Encodes where to resume a `ListObjects` scan. Two distinct semantics —
/// they need different cursor positions in the underlying byte key space.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ListCursor {
    /// S3 `start-after` / `marker` semantics: return keys strictly lex-greater
    /// than this key (skips all version_ids of this exact key).
    AfterKey(String),
    /// Skip every key starting with this prefix; used after emitting a
    /// `CommonPrefix` entry to jump past its members.
    AfterPrefix(String),
}

/// Request body for `MetaStore::list_objects`.
#[derive(Debug, Clone)]
pub struct ListObjectsRequest {
    pub bucket: String,
    pub prefix: String,
    pub delimiter: Option<String>,
    pub cursor: Option<ListCursor>,
    pub limit: usize,
}

/// Single entry in a `ListObjectsPage`. `Manifest` is boxed because it dwarfs
/// the other variant (~310 bytes vs the inline `String`); without the box
/// every list entry would pay full manifest size in memory.
#[derive(Debug, Clone)]
pub enum ListItem {
    Object(Box<Manifest>),
    CommonPrefix(String),
}

/// Single entry in a `ListObjectVersionsPage`. Wraps a committed manifest
/// (object or tombstone) with the latest-version flag.
#[derive(Debug, Clone)]
pub struct ListObjectVersionEntry {
    pub manifest: Box<Manifest>,
    pub is_latest: bool,
}

/// One page of a `ListObjectVersions` scan.
#[derive(Debug, Clone)]
pub struct ListObjectVersionsPage {
    pub entries: Vec<ListObjectVersionEntry>,
    pub truncated: bool,
    pub next_cursor: Option<ListCursor>,
}

/// One page of a `ListObjects` scan.
#[derive(Debug, Clone)]
pub struct ListObjectsPage {
    pub items: Vec<ListItem>,
    pub truncated: bool,
    /// The cursor for fetching the next page; `None` when no more results.
    pub next_cursor: Option<ListCursor>,
}

/// Async facade over the redb actor thread.
///
/// `redb::Database` is owned exclusively by a dedicated OS thread (outside the
/// tokio runtime). All access flows through `mpsc` operation messages and
/// `oneshot` replies. Other modules in the crate never see `redb::Database`
/// directly — the data plane invariant is enforced by visibility.
pub struct MetaStore {
    sender: mpsc::Sender<actor::Op>,
    /// Handle to the redb actor thread. Dropped explicitly on `MetaStore::drop`
    /// so the exclusive redb file lock is released synchronously — admin
    /// tools like backup can immediately re-open the same file.
    actor_thread: Option<std::thread::JoinHandle<()>>,
}

impl MetaStore {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self, MetaError> {
        let path = path.into();
        let (sender, receiver) = mpsc::channel::<actor::Op>(256);
        let (ready_tx, ready_rx) = oneshot::channel();

        let handle = thread::Builder::new()
            .name("s3lite-redb".into())
            .spawn(move || actor::run(path, receiver, ready_tx))
            .map_err(|e| MetaError::ActorSpawn(e.to_string()))?;

        match ready_rx.await {
            Ok(Ok(())) => Ok(Self {
                sender,
                actor_thread: Some(handle),
            }),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(MetaError::ActorDied),
        }
    }

    async fn dispatch<R>(
        &self,
        build: impl FnOnce(oneshot::Sender<R>) -> actor::Op,
    ) -> Result<R, MetaError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(build(tx))
            .await
            .map_err(|_| MetaError::ActorDied)?;
        rx.await.map_err(|_| MetaError::ActorDied)
    }

    pub async fn create_bucket(&self, name: &str, config: BucketConfig) -> Result<(), MetaError> {
        self.dispatch(|reply| actor::Op::CreateBucket {
            name: name.to_string(),
            config,
            reply,
        })
        .await?
    }

    pub async fn get_bucket(&self, name: &str) -> Result<Option<BucketConfig>, MetaError> {
        self.dispatch(|reply| actor::Op::GetBucket {
            name: name.to_string(),
            reply,
        })
        .await?
    }

    pub async fn delete_bucket(&self, name: &str) -> Result<(), MetaError> {
        self.dispatch(|reply| actor::Op::DeleteBucket {
            name: name.to_string(),
            reply,
        })
        .await?
    }

    pub async fn list_buckets(&self) -> Result<Vec<(String, BucketConfig)>, MetaError> {
        self.dispatch(|reply| actor::Op::ListBuckets { reply }).await?
    }

    pub async fn put_manifest(&self, manifest: Manifest) -> Result<DeletionEffect, MetaError> {
        self.dispatch(|reply| actor::Op::PutManifest { manifest, reply })
            .await?
    }

    pub async fn get_manifest(&self, key: ManifestKey) -> Result<Option<Manifest>, MetaError> {
        self.dispatch(|reply| actor::Op::GetManifest { key, reply })
            .await?
    }

    pub async fn delete_manifest(&self, key: ManifestKey) -> Result<DeletionEffect, MetaError> {
        self.dispatch(|reply| actor::Op::DeleteManifest { key, reply })
            .await?
    }

    pub async fn update_manifest_state(
        &self,
        key: ManifestKey,
        new_state: ManifestState,
    ) -> Result<(), MetaError> {
        self.dispatch(|reply| actor::Op::UpdateManifestState {
            key,
            new_state,
            reply,
        })
        .await?
    }

    pub async fn append_part(
        &self,
        key: ManifestKey,
        part: PartRef,
    ) -> Result<DeletionEffect, MetaError> {
        self.dispatch(|reply| actor::Op::AppendPart { key, part, reply })
            .await?
    }

    pub async fn get_part(&self, hash: Hash) -> Result<Option<PartEntry>, MetaError> {
        self.dispatch(|reply| actor::Op::GetPart { hash, reply }).await?
    }

    pub async fn mark_part_gc_pending(&self, hash: Hash) -> Result<bool, MetaError> {
        self.dispatch(|reply| actor::Op::MarkPartGcPending { hash, reply })
            .await?
    }

    pub async fn remove_part(&self, hash: Hash) -> Result<(), MetaError> {
        self.dispatch(|reply| actor::Op::RemovePart { hash, reply }).await?
    }

    pub async fn list_gc_pending_parts(&self) -> Result<Vec<Hash>, MetaError> {
        self.dispatch(|reply| actor::Op::ListGcPendingParts { reply })
            .await?
    }

    pub async fn list_objects(
        &self,
        request: ListObjectsRequest,
    ) -> Result<ListObjectsPage, MetaError> {
        self.dispatch(|reply| actor::Op::ListObjects { request, reply })
            .await?
    }

    /// Atomically transition an in-progress multipart manifest into a
    /// committed object: drops the in-progress row, inserts the committed
    /// manifest, adjusts part refcounts, and reports freed parts.
    pub async fn complete_multipart_upload(
        &self,
        in_progress_key: ManifestKey,
        new_committed: Manifest,
    ) -> Result<DeletionEffect, MetaError> {
        self.dispatch(|reply| actor::Op::CompleteMultipartUpload {
            in_progress_key,
            new_committed,
            reply,
        })
        .await?
    }

    /// Return the latest committed manifest for `(bucket, key)` regardless of
    /// kind (object or tombstone). Caller decides what to do with tombstones.
    pub async fn get_latest_version(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<Manifest>, MetaError> {
        self.dispatch(|reply| actor::Op::GetLatestVersion {
            bucket: bucket.to_string(),
            key: key.to_string(),
            reply,
        })
        .await?
    }

    pub async fn update_bucket_versioning(
        &self,
        name: &str,
        new_state: crate::storage::manifest::VersioningState,
    ) -> Result<(), MetaError> {
        self.dispatch(|reply| actor::Op::UpdateBucketVersioning {
            name: name.to_string(),
            new_state,
            reply,
        })
        .await?
    }

    pub async fn list_object_versions(
        &self,
        request: ListObjectsRequest,
    ) -> Result<ListObjectVersionsPage, MetaError> {
        self.dispatch(|reply| actor::Op::ListObjectVersions { request, reply })
            .await?
    }

    pub async fn update_bucket_cors(
        &self,
        name: &str,
        rules: Vec<crate::storage::manifest::CorsRule>,
    ) -> Result<(), MetaError> {
        self.dispatch(|reply| actor::Op::UpdateBucketCors {
            name: name.to_string(),
            rules,
            reply,
        })
        .await?
    }

    pub async fn update_manifest_tags(
        &self,
        key: ManifestKey,
        tags: std::collections::BTreeMap<String, String>,
    ) -> Result<(), MetaError> {
        self.dispatch(|reply| actor::Op::UpdateManifestTags { key, tags, reply })
            .await?
    }

    pub async fn update_bucket_object_lock(
        &self,
        name: &str,
        cfg: Option<crate::storage::manifest::ObjectLockConfig>,
    ) -> Result<(), MetaError> {
        self.dispatch(|reply| actor::Op::UpdateBucketObjectLock {
            name: name.to_string(),
            cfg,
            reply,
        })
        .await?
    }

    pub async fn update_manifest_lock(
        &self,
        key: ManifestKey,
        lock: Option<crate::storage::manifest::ObjectLock>,
    ) -> Result<(), MetaError> {
        self.dispatch(|reply| actor::Op::UpdateManifestLock { key, lock, reply })
            .await?
    }

    pub async fn update_bucket_lifecycle(
        &self,
        name: &str,
        rules: Vec<crate::storage::manifest::LifecycleRule>,
    ) -> Result<(), MetaError> {
        self.dispatch(|reply| actor::Op::UpdateBucketLifecycle {
            name: name.to_string(),
            rules,
            reply,
        })
        .await?
    }

    pub async fn list_all_manifests_in_bucket(
        &self,
        bucket: &str,
    ) -> Result<Vec<Manifest>, MetaError> {
        self.dispatch(|reply| actor::Op::ListAllManifestsInBucket {
            bucket: bucket.to_string(),
            reply,
        })
        .await?
    }

    pub async fn list_orphan_parts(&self) -> Result<Vec<Hash>, MetaError> {
        self.dispatch(|reply| actor::Op::ListOrphanParts { reply }).await?
    }

    /// Append an entry (opaque bincode-encoded blob) to the dead-letter queue,
    /// returning the assigned sequence number.
    pub async fn insert_dlq_entry(&self, bytes: Vec<u8>) -> Result<u64, MetaError> {
        self.dispatch(|reply| actor::Op::InsertDlqEntry {
            entry: actor::DlqEntryBytes(bytes),
            reply,
        })
        .await?
    }

    /// Read all DLQ entries (for tests and operator inspection). Each entry
    /// is `(sequence_number, opaque_bytes)`.
    pub async fn list_dlq(&self) -> Result<Vec<(u64, Vec<u8>)>, MetaError> {
        let entries = self
            .dispatch(|reply| actor::Op::ListDlq { reply })
            .await??;
        Ok(entries
            .into_iter()
            .map(|(k, e)| (k, e.0))
            .collect())
    }
}

impl Drop for MetaStore {
    fn drop(&mut self) {
        // Signal then join — without joining, the redb exclusive lock can
        // outlive `MetaStore` and block subsequent opens (notably the admin
        // backup path that opens the same data dir).
        let _ = self.sender.try_send(actor::Op::Shutdown);
        if let Some(handle) = self.actor_thread.take() {
            let _ = handle.join();
        }
    }
}
