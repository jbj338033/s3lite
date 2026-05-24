use std::path::PathBuf;

use redb::{Database, ReadableTable};
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};

use crate::storage::manifest::{
    BucketConfig, DeletionEffect, Hash, Manifest, ManifestKey, ManifestKind, ManifestState,
    PartEntry, PartRef, PartState,
};

use super::errors::MetaError;
use super::tables::{BUCKETS, EVENTS_DLQ, OBJECTS, PARTS, SERVER_META};
use super::{
    ListCursor, ListItem, ListObjectVersionEntry, ListObjectVersionsPage, ListObjectsPage,
    ListObjectsRequest,
};

type Reply<T> = oneshot::Sender<T>;

pub(super) enum Op {
    CreateBucket {
        name: String,
        config: BucketConfig,
        reply: Reply<Result<(), MetaError>>,
    },
    GetBucket {
        name: String,
        reply: Reply<Result<Option<BucketConfig>, MetaError>>,
    },
    DeleteBucket {
        name: String,
        reply: Reply<Result<(), MetaError>>,
    },
    ListBuckets {
        reply: Reply<Result<Vec<(String, BucketConfig)>, MetaError>>,
    },

    PutManifest {
        manifest: Manifest,
        reply: Reply<Result<DeletionEffect, MetaError>>,
    },
    GetManifest {
        key: ManifestKey,
        reply: Reply<Result<Option<Manifest>, MetaError>>,
    },
    DeleteManifest {
        key: ManifestKey,
        reply: Reply<Result<DeletionEffect, MetaError>>,
    },
    UpdateManifestState {
        key: ManifestKey,
        new_state: ManifestState,
        reply: Reply<Result<(), MetaError>>,
    },
    AppendPart {
        key: ManifestKey,
        part: PartRef,
        reply: Reply<Result<DeletionEffect, MetaError>>,
    },

    GetPart {
        hash: Hash,
        reply: Reply<Result<Option<PartEntry>, MetaError>>,
    },
    MarkPartGcPending {
        hash: Hash,
        reply: Reply<Result<bool, MetaError>>,
    },
    RemovePart {
        hash: Hash,
        reply: Reply<Result<(), MetaError>>,
    },
    ListGcPendingParts {
        reply: Reply<Result<Vec<Hash>, MetaError>>,
    },
    ListObjects {
        request: ListObjectsRequest,
        reply: Reply<Result<ListObjectsPage, MetaError>>,
    },
    CompleteMultipartUpload {
        in_progress_key: ManifestKey,
        new_committed: Manifest,
        reply: Reply<Result<DeletionEffect, MetaError>>,
    },
    GetLatestVersion {
        bucket: String,
        key: String,
        reply: Reply<Result<Option<Manifest>, MetaError>>,
    },
    UpdateBucketVersioning {
        name: String,
        new_state: crate::storage::manifest::VersioningState,
        reply: Reply<Result<(), MetaError>>,
    },
    ListObjectVersions {
        request: ListObjectsRequest,
        reply: Reply<Result<ListObjectVersionsPage, MetaError>>,
    },
    UpdateBucketCors {
        name: String,
        rules: Vec<crate::storage::manifest::CorsRule>,
        reply: Reply<Result<(), MetaError>>,
    },
    UpdateManifestTags {
        key: ManifestKey,
        tags: std::collections::BTreeMap<String, String>,
        reply: Reply<Result<(), MetaError>>,
    },
    UpdateBucketObjectLock {
        name: String,
        cfg: Option<crate::storage::manifest::ObjectLockConfig>,
        reply: Reply<Result<(), MetaError>>,
    },
    UpdateManifestLock {
        key: ManifestKey,
        lock: Option<crate::storage::manifest::ObjectLock>,
        reply: Reply<Result<(), MetaError>>,
    },
    UpdateBucketLifecycle {
        name: String,
        rules: Vec<crate::storage::manifest::LifecycleRule>,
        reply: Reply<Result<(), MetaError>>,
    },
    ListAllManifestsInBucket {
        bucket: String,
        reply: Reply<Result<Vec<Manifest>, MetaError>>,
    },
    ListOrphanParts {
        reply: Reply<Result<Vec<Hash>, MetaError>>,
    },

    Shutdown,
}

pub(super) fn run(
    path: PathBuf,
    mut receiver: mpsc::Receiver<Op>,
    ready: oneshot::Sender<Result<(), MetaError>>,
) {
    let db = match Database::create(&path) {
        Ok(db) => db,
        Err(e) => {
            let _ = ready.send(Err(MetaError::Redb(e.to_string())));
            return;
        }
    };
    if let Err(e) = bootstrap_tables(&db) {
        let _ = ready.send(Err(e));
        return;
    }
    let _ = ready.send(Ok(()));

    while let Some(op) = receiver.blocking_recv() {
        match op {
            Op::Shutdown => break,
            Op::CreateBucket { name, config, reply } => {
                let _ = reply.send(handle_create_bucket(&db, &name, &config));
            }
            Op::GetBucket { name, reply } => {
                let _ = reply.send(handle_get_bucket(&db, &name));
            }
            Op::DeleteBucket { name, reply } => {
                let _ = reply.send(handle_delete_bucket(&db, &name));
            }
            Op::ListBuckets { reply } => {
                let _ = reply.send(handle_list_buckets(&db));
            }
            Op::PutManifest { manifest, reply } => {
                let _ = reply.send(handle_put_manifest(&db, &manifest));
            }
            Op::GetManifest { key, reply } => {
                let _ = reply.send(handle_get_manifest(&db, &key));
            }
            Op::DeleteManifest { key, reply } => {
                let _ = reply.send(handle_delete_manifest(&db, &key));
            }
            Op::UpdateManifestState { key, new_state, reply } => {
                let _ = reply.send(handle_update_state(&db, &key, new_state));
            }
            Op::AppendPart { key, part, reply } => {
                let _ = reply.send(handle_append_part(&db, &key, &part));
            }
            Op::GetPart { hash, reply } => {
                let _ = reply.send(handle_get_part(&db, &hash));
            }
            Op::MarkPartGcPending { hash, reply } => {
                let _ = reply.send(handle_mark_gc_pending(&db, &hash));
            }
            Op::RemovePart { hash, reply } => {
                let _ = reply.send(handle_remove_part(&db, &hash));
            }
            Op::ListGcPendingParts { reply } => {
                let _ = reply.send(handle_list_gc_pending(&db));
            }
            Op::ListObjects { request, reply } => {
                let _ = reply.send(handle_list_objects(&db, &request));
            }
            Op::CompleteMultipartUpload {
                in_progress_key,
                new_committed,
                reply,
            } => {
                let _ = reply.send(handle_complete_multipart(
                    &db,
                    &in_progress_key,
                    &new_committed,
                ));
            }
            Op::GetLatestVersion { bucket, key, reply } => {
                let _ = reply.send(handle_get_latest_version(&db, &bucket, &key));
            }
            Op::UpdateBucketVersioning {
                name,
                new_state,
                reply,
            } => {
                let _ = reply.send(handle_update_bucket_versioning(&db, &name, new_state));
            }
            Op::ListObjectVersions { request, reply } => {
                let _ = reply.send(handle_list_object_versions(&db, &request));
            }
            Op::UpdateBucketCors { name, rules, reply } => {
                let _ = reply.send(handle_update_bucket_cors(&db, &name, rules));
            }
            Op::UpdateManifestTags { key, tags, reply } => {
                let _ = reply.send(handle_update_manifest_tags(&db, &key, tags));
            }
            Op::UpdateBucketObjectLock { name, cfg, reply } => {
                let _ = reply.send(handle_update_bucket_object_lock(&db, &name, cfg));
            }
            Op::UpdateManifestLock { key, lock, reply } => {
                let _ = reply.send(handle_update_manifest_lock(&db, &key, lock));
            }
            Op::UpdateBucketLifecycle { name, rules, reply } => {
                let _ = reply.send(handle_update_bucket_lifecycle(&db, &name, rules));
            }
            Op::ListAllManifestsInBucket { bucket, reply } => {
                let _ = reply.send(handle_list_all_manifests_in_bucket(&db, &bucket));
            }
            Op::ListOrphanParts { reply } => {
                let _ = reply.send(handle_list_orphan_parts(&db));
            }
        }
    }
}

fn bootstrap_tables(db: &Database) -> Result<(), MetaError> {
    let tx = db.begin_write()?;
    let _ = tx.open_table(BUCKETS)?;
    let _ = tx.open_table(OBJECTS)?;
    let _ = tx.open_table(PARTS)?;
    let _ = tx.open_table(SERVER_META)?;
    let _ = tx.open_table(EVENTS_DLQ)?;
    tx.commit()?;
    Ok(())
}

fn handle_create_bucket(db: &Database, name: &str, config: &BucketConfig) -> Result<(), MetaError> {
    let tx = db.begin_write()?;
    {
        let mut table = tx.open_table(BUCKETS)?;
        if table.get(name)?.is_some() {
            return Err(MetaError::BucketExists(name.to_string()));
        }
        let bytes = bincode_encode(config)?;
        table.insert(name, bytes.as_slice())?;
    }
    tx.commit()?;
    Ok(())
}

fn handle_get_bucket(db: &Database, name: &str) -> Result<Option<BucketConfig>, MetaError> {
    let tx = db.begin_read()?;
    let table = tx.open_table(BUCKETS)?;
    let Some(v) = table.get(name)? else { return Ok(None) };
    let config: BucketConfig = bincode_decode(v.value())?;
    Ok(Some(config))
}

fn handle_delete_bucket(db: &Database, name: &str) -> Result<(), MetaError> {
    let tx = db.begin_write()?;
    {
        let mut buckets = tx.open_table(BUCKETS)?;
        if buckets.get(name)?.is_none() {
            return Err(MetaError::BucketNotFound(name.to_string()));
        }
        // empty-check via prefix scan in OBJECTS table
        {
            let objects = tx.open_table(OBJECTS)?;
            let mut start = name.as_bytes().to_vec();
            start.push(0);
            let mut end = name.as_bytes().to_vec();
            end.push(1);
            let mut iter = objects.range(start.as_slice()..end.as_slice())?;
            if iter.next().is_some() {
                return Err(MetaError::BucketNotEmpty(name.to_string()));
            }
        }
        buckets.remove(name)?;
    }
    tx.commit()?;
    Ok(())
}

fn handle_list_buckets(db: &Database) -> Result<Vec<(String, BucketConfig)>, MetaError> {
    let tx = db.begin_read()?;
    let table = tx.open_table(BUCKETS)?;
    let mut result = Vec::new();
    for entry in table.iter()? {
        let (k, v) = entry?;
        let name = k.value().to_string();
        let config: BucketConfig = bincode_decode(v.value())?;
        result.push((name, config));
    }
    Ok(result)
}

fn handle_put_manifest(db: &Database, manifest: &Manifest) -> Result<DeletionEffect, MetaError> {
    let tx = db.begin_write()?;
    let mut freed = Vec::new();
    {
        let mut objects = tx.open_table(OBJECTS)?;
        let mut parts = tx.open_table(PARTS)?;

        let key_bytes = manifest.key.encode();

        // Load old manifest (if replacing)
        let old: Option<Manifest> = {
            if let Some(v) = objects.get(key_bytes.as_slice())? {
                Some(bincode_decode(v.value())?)
            } else {
                None
            }
        };

        // Increment refcount for every part referenced by the new manifest.
        // GcPending parts may not be referenced (race interlock).
        let now = OffsetDateTime::now_utc();
        for part_ref in &manifest.parts {
            inc_part_refcount(&mut parts, &part_ref.hash, part_ref.size, now)?;
        }

        // Decrement old manifest's part refcounts; collect freed.
        if let Some(old) = old {
            for old_part in &old.parts {
                if let Some(hash) = dec_part_refcount(&mut parts, &old_part.hash)? {
                    freed.push(hash);
                }
            }
        }

        let bytes = bincode_encode(manifest)?;
        objects.insert(key_bytes.as_slice(), bytes.as_slice())?;
    }
    tx.commit()?;
    Ok(DeletionEffect { freed_parts: freed })
}

fn handle_get_manifest(db: &Database, key: &ManifestKey) -> Result<Option<Manifest>, MetaError> {
    let tx = db.begin_read()?;
    let table = tx.open_table(OBJECTS)?;
    let key_bytes = key.encode();
    let Some(v) = table.get(key_bytes.as_slice())? else {
        return Ok(None);
    };
    let m: Manifest = bincode_decode(v.value())?;
    Ok(Some(m))
}

fn handle_delete_manifest(db: &Database, key: &ManifestKey) -> Result<DeletionEffect, MetaError> {
    let tx = db.begin_write()?;
    let mut freed = Vec::new();
    {
        let mut objects = tx.open_table(OBJECTS)?;
        let mut parts = tx.open_table(PARTS)?;

        let key_bytes = key.encode();
        let m: Manifest = {
            let Some(v) = objects.get(key_bytes.as_slice())? else {
                return Err(MetaError::ManifestNotFound(key.clone()));
            };
            bincode_decode(v.value())?
        };

        for p in &m.parts {
            if let Some(hash) = dec_part_refcount(&mut parts, &p.hash)? {
                freed.push(hash);
            }
        }
        objects.remove(key_bytes.as_slice())?;
    }
    tx.commit()?;
    Ok(DeletionEffect { freed_parts: freed })
}

fn handle_update_state(
    db: &Database,
    key: &ManifestKey,
    new_state: ManifestState,
) -> Result<(), MetaError> {
    let tx = db.begin_write()?;
    {
        let mut objects = tx.open_table(OBJECTS)?;
        let key_bytes = key.encode();
        let mut m: Manifest = {
            let Some(v) = objects.get(key_bytes.as_slice())? else {
                return Err(MetaError::ManifestNotFound(key.clone()));
            };
            bincode_decode(v.value())?
        };
        m.state = new_state;
        m.last_modified = OffsetDateTime::now_utc();
        let bytes = bincode_encode(&m)?;
        objects.insert(key_bytes.as_slice(), bytes.as_slice())?;
    }
    tx.commit()?;
    Ok(())
}

fn handle_append_part(
    db: &Database,
    key: &ManifestKey,
    part: &PartRef,
) -> Result<DeletionEffect, MetaError> {
    let tx = db.begin_write()?;
    let mut freed = Vec::new();
    {
        let mut objects = tx.open_table(OBJECTS)?;
        let mut parts = tx.open_table(PARTS)?;

        let key_bytes = key.encode();
        let mut m: Manifest = {
            let Some(v) = objects.get(key_bytes.as_slice())? else {
                return Err(MetaError::ManifestNotFound(key.clone()));
            };
            bincode_decode(v.value())?
        };

        // If replacing existing part_number, decrement old refcount
        if let Some(old_idx) = m.parts.iter().position(|p| p.part_number == part.part_number) {
            let old_hash = m.parts[old_idx].hash;
            if old_hash != part.hash {
                if let Some(hash) = dec_part_refcount(&mut parts, &old_hash)? {
                    freed.push(hash);
                }
            } else {
                // Same hash, same number: net zero — decrement now, will re-increment below
                let _ = dec_part_refcount(&mut parts, &old_hash)?;
            }
            m.parts.remove(old_idx);
        }

        // Increment new part refcount
        let now = OffsetDateTime::now_utc();
        inc_part_refcount(&mut parts, &part.hash, part.size, now)?;

        // Append + sort
        m.parts.push(part.clone());
        m.parts.sort_by_key(|p| p.part_number);
        m.size = m.parts.iter().map(|p| p.size).sum();
        m.last_modified = OffsetDateTime::now_utc();

        let bytes = bincode_encode(&m)?;
        objects.insert(key_bytes.as_slice(), bytes.as_slice())?;
    }
    tx.commit()?;
    Ok(DeletionEffect { freed_parts: freed })
}

fn handle_get_part(db: &Database, hash: &Hash) -> Result<Option<PartEntry>, MetaError> {
    let tx = db.begin_read()?;
    let table = tx.open_table(PARTS)?;
    let Some(v) = table.get(hash)? else { return Ok(None) };
    let e: PartEntry = bincode_decode(v.value())?;
    Ok(Some(e))
}

fn handle_mark_gc_pending(db: &Database, hash: &Hash) -> Result<bool, MetaError> {
    let tx = db.begin_write()?;
    let marked;
    {
        let mut parts = tx.open_table(PARTS)?;
        let mut e: PartEntry = {
            let Some(v) = parts.get(hash)? else {
                return Err(MetaError::PartNotFound(hex::encode(hash)));
            };
            bincode_decode(v.value())?
        };
        if e.refcount != 0 {
            marked = false;
        } else {
            e.state = PartState::GcPending;
            let bytes = bincode_encode(&e)?;
            parts.insert(hash, bytes.as_slice())?;
            marked = true;
        }
    }
    tx.commit()?;
    Ok(marked)
}

fn handle_remove_part(db: &Database, hash: &Hash) -> Result<(), MetaError> {
    let tx = db.begin_write()?;
    {
        let mut parts = tx.open_table(PARTS)?;
        parts.remove(hash)?;
    }
    tx.commit()?;
    Ok(())
}

fn handle_list_objects(
    db: &Database,
    req: &ListObjectsRequest,
) -> Result<ListObjectsPage, MetaError> {
    use std::collections::BTreeMap;

    let tx = db.begin_read()?;
    let table = tx.open_table(OBJECTS)?;

    let scan_start = match &req.cursor {
        None => initial_scan_start(&req.bucket, &req.prefix),
        Some(ListCursor::AfterKey(k)) => after_key(&req.bucket, k),
        Some(ListCursor::AfterPrefix(p)) => after_prefix(&req.bucket, p),
    };
    let scan_end = {
        let mut v = req.bucket.as_bytes().to_vec();
        v.push(1);
        v
    };

    // Group committed manifests by user key, then pick the latest version per
    // key. With versioning Enabled a single key may have many manifests; with
    // Off/Suspended there's usually one or two. BTreeMap iterates sorted by
    // key so the downstream loop visits keys in S3's expected order.
    let mut latest_by_key: BTreeMap<String, Manifest> = BTreeMap::new();
    for entry in table.range(scan_start.as_slice()..scan_end.as_slice())? {
        let (_k, v) = entry?;
        let manifest: Manifest = bincode_decode(v.value())?;
        if !matches!(manifest.state, ManifestState::Committed) {
            continue;
        }
        if !manifest.key.key.starts_with(&req.prefix) {
            break;
        }
        let take = match latest_by_key.get(&manifest.key.key) {
            None => true,
            Some(existing) => manifest.last_modified > existing.last_modified,
        };
        if take {
            latest_by_key.insert(manifest.key.key.clone(), manifest);
        }
    }

    let mut items: Vec<ListItem> = Vec::new();
    let mut truncated = false;
    let mut next_cursor: Option<ListCursor> = None;
    let mut skip_prefix: Option<String> = None;

    for (key, manifest) in latest_by_key.iter() {
        // Tombstones are not "current objects" — they hide the key from
        // ListObjects. They still appear in ListObjectVersions.
        if !matches!(manifest.kind, ManifestKind::Object) {
            continue;
        }
        if let Some(skip) = &skip_prefix {
            if key.starts_with(skip.as_str()) {
                continue;
            }
            skip_prefix = None;
        }
        if items.len() >= req.limit {
            truncated = true;
            next_cursor = items.last().map(|item| match item {
                ListItem::Object(m) => ListCursor::AfterKey(m.key.key.clone()),
                ListItem::CommonPrefix(p) => ListCursor::AfterPrefix(p.clone()),
            });
            break;
        }
        if let Some(delim) = &req.delimiter {
            let after_prefix = &key[req.prefix.len()..];
            if let Some(pos) = after_prefix.find(delim.as_str()) {
                let common_end = req.prefix.len() + pos + delim.len();
                let common_prefix = key[..common_end].to_string();
                items.push(ListItem::CommonPrefix(common_prefix.clone()));
                skip_prefix = Some(common_prefix);
                continue;
            }
        }
        items.push(ListItem::Object(Box::new(manifest.clone())));
    }

    Ok(ListObjectsPage {
        items,
        truncated,
        next_cursor,
    })
}

fn initial_scan_start(bucket: &str, prefix: &str) -> Vec<u8> {
    let mut v = bucket.as_bytes().to_vec();
    v.push(0);
    v.extend_from_slice(prefix.as_bytes());
    v
}

/// Cursor strictly greater than `bucket\0key\0<anything>` — skips all
/// version_ids of `key` while still including `key` extensions like
/// `key0`, `key1` (S3 `start-after` semantics).
fn after_key(bucket: &str, key: &str) -> Vec<u8> {
    let mut v = bucket.as_bytes().to_vec();
    v.push(0);
    v.extend_from_slice(key.as_bytes());
    v.push(1);
    v
}

/// Cursor lex-greater than every key starting with `prefix` in this bucket —
/// used to jump past a `CommonPrefix` group.
fn after_prefix(bucket: &str, prefix: &str) -> Vec<u8> {
    let mut v = bucket.as_bytes().to_vec();
    v.push(0);
    if prefix.is_empty() {
        v.push(1);
        return v;
    }
    let bytes = prefix.as_bytes();
    for i in (0..bytes.len()).rev() {
        if bytes[i] < 0xff {
            v.extend_from_slice(&bytes[..i]);
            v.push(bytes[i] + 1);
            return v;
        }
    }
    v.extend_from_slice(bytes);
    v.push(1);
    v
}

/// Atomic transition: in-progress multipart manifest → committed object.
/// All three steps (in-progress part decrement, old-committed part decrement,
/// new committed part increment, manifest row swap) happen in a single redb
/// write tx so partial states never appear to the rest of the system.
fn handle_complete_multipart(
    db: &Database,
    in_progress_key: &ManifestKey,
    new_committed: &Manifest,
) -> Result<DeletionEffect, MetaError> {
    use std::collections::HashSet;

    let tx = db.begin_write()?;
    let mut freed: Vec<Hash> = Vec::new();
    {
        let mut objects = tx.open_table(OBJECTS)?;
        let mut parts = tx.open_table(PARTS)?;

        let in_progress_key_bytes = in_progress_key.encode();
        let committed_key_bytes = new_committed.key.encode();

        let in_progress: Manifest = {
            let Some(v) = objects.get(in_progress_key_bytes.as_slice())? else {
                return Err(MetaError::ManifestNotFound(in_progress_key.clone()));
            };
            bincode_decode(v.value())?
        };
        if !matches!(in_progress.state, ManifestState::InProgress) {
            return Err(MetaError::UploadNotInProgress(in_progress_key.clone()));
        }

        let old_committed: Option<Manifest> = {
            if let Some(v) = objects.get(committed_key_bytes.as_slice())? {
                Some(bincode_decode(v.value())?)
            } else {
                None
            }
        };

        // Refcount delta — decrement all parts referenced by the soon-to-be-
        // gone manifests, then increment all parts referenced by the new one.
        // Order matters only in that increment can resurrect a part that just
        // hit refcount=0 (state=Live, not GcPending) — which inc_part_refcount
        // accepts.
        for p in &in_progress.parts {
            let _ = dec_part_refcount(&mut parts, &p.hash)?;
        }
        if let Some(old) = &old_committed {
            for p in &old.parts {
                let _ = dec_part_refcount(&mut parts, &p.hash)?;
            }
        }
        let now = OffsetDateTime::now_utc();
        for p in &new_committed.parts {
            inc_part_refcount(&mut parts, &p.hash, p.size, now)?;
        }

        // Collect parts that finished at refcount=0 (eligible for GC). Only
        // parts NOT referenced by the new committed manifest can be free.
        let new_hashes: HashSet<Hash> =
            new_committed.parts.iter().map(|p| p.hash).collect();
        let mut considered: HashSet<Hash> = HashSet::new();
        let mut collect = |parts: &redb::Table<'_, &'static [u8; 32], &'static [u8]>,
                           candidate: &Hash|
         -> Result<(), MetaError> {
            if !considered.insert(*candidate) || new_hashes.contains(candidate) {
                return Ok(());
            }
            if let Some(v) = parts.get(candidate)? {
                let entry: PartEntry = bincode_decode(v.value())?;
                if entry.refcount == 0 {
                    freed.push(*candidate);
                }
            }
            Ok(())
        };
        for p in &in_progress.parts {
            collect(&parts, &p.hash)?;
        }
        if let Some(old) = &old_committed {
            for p in &old.parts {
                collect(&parts, &p.hash)?;
            }
        }

        objects.insert(
            committed_key_bytes.as_slice(),
            bincode_encode(new_committed)?.as_slice(),
        )?;
        objects.remove(in_progress_key_bytes.as_slice())?;
    }
    tx.commit()?;
    Ok(DeletionEffect { freed_parts: freed })
}

/// Return the latest committed manifest for `(bucket, key)`, of either object
/// or tombstone kind. Latest is defined by `last_modified`. Returns `None`
/// when no committed manifest exists for the key.
fn handle_get_latest_version(
    db: &Database,
    bucket: &str,
    key: &str,
) -> Result<Option<Manifest>, MetaError> {
    let tx = db.begin_read()?;
    let table = tx.open_table(OBJECTS)?;

    let scan_start = {
        let mut v = bucket.as_bytes().to_vec();
        v.push(0);
        v.extend_from_slice(key.as_bytes());
        v.push(0);
        v
    };
    let scan_end = {
        let mut v = bucket.as_bytes().to_vec();
        v.push(0);
        v.extend_from_slice(key.as_bytes());
        v.push(1);
        v
    };

    let mut latest: Option<Manifest> = None;
    for entry in table.range(scan_start.as_slice()..scan_end.as_slice())? {
        let (_, v) = entry?;
        let m: Manifest = bincode_decode(v.value())?;
        if !matches!(m.state, ManifestState::Committed) {
            continue;
        }
        if m.key.bucket != bucket || m.key.key != key {
            continue;
        }
        let take = match &latest {
            None => true,
            Some(l) => m.last_modified > l.last_modified,
        };
        if take {
            latest = Some(m);
        }
    }
    Ok(latest)
}

fn handle_update_bucket_versioning(
    db: &Database,
    name: &str,
    new_state: crate::storage::manifest::VersioningState,
) -> Result<(), MetaError> {
    let tx = db.begin_write()?;
    {
        let mut table = tx.open_table(BUCKETS)?;
        let mut cfg: BucketConfig = {
            let Some(v) = table.get(name)? else {
                return Err(MetaError::BucketNotFound(name.to_string()));
            };
            bincode_decode(v.value())?
        };
        cfg.versioning = new_state;
        let bytes = bincode_encode(&cfg)?;
        table.insert(name, bytes.as_slice())?;
    }
    tx.commit()?;
    Ok(())
}

/// Enumerate all committed manifests (object + tombstone) in the bucket,
/// grouped by key with the latest marked `is_latest=true`. Same prefix /
/// delimiter / pagination model as `list_objects` but at the version level.
fn handle_list_object_versions(
    db: &Database,
    req: &ListObjectsRequest,
) -> Result<ListObjectVersionsPage, MetaError> {
    use std::collections::BTreeMap;

    let tx = db.begin_read()?;
    let table = tx.open_table(OBJECTS)?;

    let scan_start = match &req.cursor {
        None => initial_scan_start(&req.bucket, &req.prefix),
        Some(ListCursor::AfterKey(k)) => after_key(&req.bucket, k),
        Some(ListCursor::AfterPrefix(p)) => after_prefix(&req.bucket, p),
    };
    let scan_end = {
        let mut v = req.bucket.as_bytes().to_vec();
        v.push(1);
        v
    };

    // Collect committed manifests grouped by key
    let mut by_key: BTreeMap<String, Vec<Manifest>> = BTreeMap::new();
    for entry in table.range(scan_start.as_slice()..scan_end.as_slice())? {
        let (_, v) = entry?;
        let m: Manifest = bincode_decode(v.value())?;
        if !matches!(m.state, ManifestState::Committed) {
            continue;
        }
        if !m.key.key.starts_with(&req.prefix) {
            break;
        }
        by_key.entry(m.key.key.clone()).or_default().push(m);
    }

    let mut entries: Vec<ListObjectVersionEntry> = Vec::new();
    let mut truncated = false;
    let mut next_cursor: Option<ListCursor> = None;

    for (_key, mut versions) in by_key {
        // Sort by last_modified desc — newest first
        versions.sort_by(|a, b| b.last_modified.cmp(&a.last_modified));
        for (idx, m) in versions.into_iter().enumerate() {
            let is_latest = idx == 0;
            if entries.len() >= req.limit {
                truncated = true;
                next_cursor = entries
                    .last()
                    .map(|e| ListCursor::AfterKey(e.manifest.key.key.clone()));
                break;
            }
            entries.push(ListObjectVersionEntry {
                manifest: Box::new(m),
                is_latest,
            });
        }
        if truncated {
            break;
        }
    }

    Ok(ListObjectVersionsPage {
        entries,
        truncated,
        next_cursor,
    })
}

fn handle_update_bucket_cors(
    db: &Database,
    name: &str,
    rules: Vec<crate::storage::manifest::CorsRule>,
) -> Result<(), MetaError> {
    let tx = db.begin_write()?;
    {
        let mut table = tx.open_table(BUCKETS)?;
        let mut cfg: BucketConfig = {
            let Some(v) = table.get(name)? else {
                return Err(MetaError::BucketNotFound(name.to_string()));
            };
            bincode_decode(v.value())?
        };
        cfg.cors_rules = rules;
        let bytes = bincode_encode(&cfg)?;
        table.insert(name, bytes.as_slice())?;
    }
    tx.commit()?;
    Ok(())
}

/// Replace just the tag set on a manifest. Per S3 semantics, tag updates
/// do NOT touch `last_modified` or `etag`.
fn handle_update_manifest_tags(
    db: &Database,
    key: &ManifestKey,
    tags: std::collections::BTreeMap<String, String>,
) -> Result<(), MetaError> {
    let tx = db.begin_write()?;
    {
        let mut objects = tx.open_table(OBJECTS)?;
        let key_bytes = key.encode();
        let mut m: Manifest = {
            let Some(v) = objects.get(key_bytes.as_slice())? else {
                return Err(MetaError::ManifestNotFound(key.clone()));
            };
            bincode_decode(v.value())?
        };
        m.tags = tags;
        let bytes = bincode_encode(&m)?;
        objects.insert(key_bytes.as_slice(), bytes.as_slice())?;
    }
    tx.commit()?;
    Ok(())
}

fn handle_update_bucket_object_lock(
    db: &Database,
    name: &str,
    cfg: Option<crate::storage::manifest::ObjectLockConfig>,
) -> Result<(), MetaError> {
    let tx = db.begin_write()?;
    {
        let mut table = tx.open_table(BUCKETS)?;
        let mut bucket: BucketConfig = {
            let Some(v) = table.get(name)? else {
                return Err(MetaError::BucketNotFound(name.to_string()));
            };
            bincode_decode(v.value())?
        };
        bucket.object_lock = cfg;
        let bytes = bincode_encode(&bucket)?;
        table.insert(name, bytes.as_slice())?;
    }
    tx.commit()?;
    Ok(())
}

/// Replace the manifest's `object_lock` field only — same metadata-only
/// semantics as `update_manifest_tags`. Last-modified is left intact.
fn handle_update_manifest_lock(
    db: &Database,
    key: &ManifestKey,
    lock: Option<crate::storage::manifest::ObjectLock>,
) -> Result<(), MetaError> {
    let tx = db.begin_write()?;
    {
        let mut objects = tx.open_table(OBJECTS)?;
        let key_bytes = key.encode();
        let mut m: Manifest = {
            let Some(v) = objects.get(key_bytes.as_slice())? else {
                return Err(MetaError::ManifestNotFound(key.clone()));
            };
            bincode_decode(v.value())?
        };
        m.object_lock = lock;
        let bytes = bincode_encode(&m)?;
        objects.insert(key_bytes.as_slice(), bytes.as_slice())?;
    }
    tx.commit()?;
    Ok(())
}

fn handle_update_bucket_lifecycle(
    db: &Database,
    name: &str,
    rules: Vec<crate::storage::manifest::LifecycleRule>,
) -> Result<(), MetaError> {
    let tx = db.begin_write()?;
    {
        let mut table = tx.open_table(BUCKETS)?;
        let mut cfg: BucketConfig = {
            let Some(v) = table.get(name)? else {
                return Err(MetaError::BucketNotFound(name.to_string()));
            };
            bincode_decode(v.value())?
        };
        cfg.lifecycle_rules = rules;
        let bytes = bincode_encode(&cfg)?;
        table.insert(name, bytes.as_slice())?;
    }
    tx.commit()?;
    Ok(())
}

/// Maintenance helper — return every manifest in the bucket regardless of
/// state/kind. Used by the lifecycle sweeper to evaluate Expiration on
/// committed objects, NoncurrentVersionExpiration on older versions, and
/// AbortIncompleteMultipart on in-progress uploads.
fn handle_list_all_manifests_in_bucket(
    db: &Database,
    bucket: &str,
) -> Result<Vec<Manifest>, MetaError> {
    let tx = db.begin_read()?;
    let table = tx.open_table(OBJECTS)?;
    let mut start = bucket.as_bytes().to_vec();
    start.push(0);
    let mut end = bucket.as_bytes().to_vec();
    end.push(1);
    let mut out = Vec::new();
    for entry in table.range(start.as_slice()..end.as_slice())? {
        let (_, v) = entry?;
        let m: Manifest = bincode_decode(v.value())?;
        if m.key.bucket != bucket {
            continue;
        }
        out.push(m);
    }
    Ok(out)
}

/// Maintenance helper — list part hashes that have refcount=0 and are still
/// in the Live state (not yet handed off to the GC pipeline). These are the
/// orphans that the inline GC path failed to reach (e.g., after a crash).
fn handle_list_orphan_parts(db: &Database) -> Result<Vec<Hash>, MetaError> {
    let tx = db.begin_read()?;
    let parts = tx.open_table(PARTS)?;
    let mut out = Vec::new();
    for entry in parts.iter()? {
        let (k, v) = entry?;
        let e: PartEntry = bincode_decode(v.value())?;
        if e.refcount == 0 && e.state == PartState::Live {
            out.push(*k.value());
        }
    }
    Ok(out)
}

fn handle_list_gc_pending(db: &Database) -> Result<Vec<Hash>, MetaError> {
    let tx = db.begin_read()?;
    let parts = tx.open_table(PARTS)?;
    let mut result = Vec::new();
    for entry in parts.iter()? {
        let (k, v) = entry?;
        let e: PartEntry = bincode_decode(v.value())?;
        if e.state == PartState::GcPending {
            result.push(*k.value());
        }
    }
    Ok(result)
}

// ---- helpers ----

fn inc_part_refcount(
    parts: &mut redb::Table<'_, &'static [u8; 32], &'static [u8]>,
    hash: &Hash,
    size: u64,
    now: OffsetDateTime,
) -> Result<(), MetaError> {
    let existing: Option<PartEntry> = {
        if let Some(v) = parts.get(hash)? {
            Some(bincode_decode(v.value())?)
        } else {
            None
        }
    };
    let entry = match existing {
        Some(mut e) => {
            if e.state == PartState::GcPending {
                return Err(MetaError::PartGcPending(hex::encode(hash)));
            }
            e.refcount = e.refcount.saturating_add(1);
            e
        }
        None => PartEntry {
            refcount: 1,
            size,
            state: PartState::Live,
            created_at: now,
        },
    };
    let bytes = bincode_encode(&entry)?;
    parts.insert(hash, bytes.as_slice())?;
    Ok(())
}

/// Returns Some(hash) if refcount dropped to 0 (caller should mark+GC).
fn dec_part_refcount(
    parts: &mut redb::Table<'_, &'static [u8; 32], &'static [u8]>,
    hash: &Hash,
) -> Result<Option<Hash>, MetaError> {
    let mut e: PartEntry = {
        let Some(v) = parts.get(hash)? else {
            return Ok(None);
        };
        bincode_decode(v.value())?
    };
    if e.refcount > 0 {
        e.refcount -= 1;
    }
    let freed = if e.refcount == 0 { Some(*hash) } else { None };
    let bytes = bincode_encode(&e)?;
    parts.insert(hash, bytes.as_slice())?;
    Ok(freed)
}

fn bincode_encode<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, MetaError> {
    bincode::serde::encode_to_vec(v, bincode::config::standard())
        .map_err(|e| MetaError::BincodeEncode(e.to_string()))
}

fn bincode_decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, MetaError> {
    let (v, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map_err(|e| MetaError::BincodeDecode(e.to_string()))?;
    Ok(v)
}

// Silence unused-import warning until later phases wire up SERVER_META / EVENTS_DLQ.
#[allow(dead_code)]
fn _ensure_tables_referenced() {
    let _ = &SERVER_META;
    let _ = &EVENTS_DLQ;
}
