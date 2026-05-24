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

/// Async facade over the redb actor thread.
///
/// `redb::Database` is owned exclusively by a dedicated OS thread (outside the
/// tokio runtime). All access flows through `mpsc` operation messages and
/// `oneshot` replies. Other modules in the crate never see `redb::Database`
/// directly — the data plane invariant is enforced by visibility.
pub struct MetaStore {
    sender: mpsc::Sender<actor::Op>,
}

impl MetaStore {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self, MetaError> {
        let path = path.into();
        let (sender, receiver) = mpsc::channel::<actor::Op>(256);
        let (ready_tx, ready_rx) = oneshot::channel();

        thread::Builder::new()
            .name("s3lite-redb".into())
            .spawn(move || actor::run(path, receiver, ready_tx))
            .map_err(|e| MetaError::ActorSpawn(e.to_string()))?;

        match ready_rx.await {
            Ok(Ok(())) => Ok(Self { sender }),
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
}

impl Drop for MetaStore {
    fn drop(&mut self) {
        // Best-effort: actor exits when the channel closes anyway.
        let _ = self.sender.try_send(actor::Op::Shutdown);
    }
}
