use std::collections::BTreeMap;

use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

pub type Hash = [u8; 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManifestState {
    InProgress,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManifestKind {
    Object,
    Tombstone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UploadMode {
    SinglePut,
    Multipart,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    Crc32,
    Crc32c,
    Sha1,
    Sha256,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdditionalChecksum {
    pub algorithm: ChecksumAlgorithm,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockMode {
    Compliance,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectLock {
    pub mode: LockMode,
    pub retain_until: OffsetDateTime,
    pub legal_hold: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartRef {
    pub part_number: u32,
    pub hash: Hash,
    pub md5: [u8; 16],
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ManifestKey {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
}

impl ManifestKey {
    pub fn new(bucket: impl Into<String>, key: impl Into<String>, version_id: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
            version_id: version_id.into(),
        }
    }

    /// Composite key bytes for redb. Null-separated. S3 forbids NUL in bucket and key names,
    /// and version_id is server-generated, so this is unambiguous.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.bucket.len() + self.key.len() + self.version_id.len() + 2);
        out.extend_from_slice(self.bucket.as_bytes());
        out.push(0);
        out.extend_from_slice(self.key.as_bytes());
        out.push(0);
        out.extend_from_slice(self.version_id.as_bytes());
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub key: ManifestKey,
    pub state: ManifestState,
    pub kind: ManifestKind,
    pub upload_mode: UploadMode,
    pub parts: Vec<PartRef>,
    pub size: u64,
    pub content_type: Option<String>,
    pub user_metadata: BTreeMap<String, String>,
    pub tags: BTreeMap<String, String>,
    pub additional_checksum: Option<AdditionalChecksum>,
    pub storage_class: String,
    pub object_lock: Option<ObjectLock>,
    pub created_at: OffsetDateTime,
    pub last_modified: OffsetDateTime,
    pub upload_id: Option<String>,
}

impl Manifest {
    /// Compute S3 ETag.
    /// - SinglePut: hex(md5) of the single part, in quotes.
    /// - Multipart: hex(md5(concat(part_md5s))) + "-" + N, in quotes.
    pub fn etag(&self) -> String {
        match self.upload_mode {
            UploadMode::SinglePut => {
                debug_assert_eq!(self.parts.len(), 1, "SinglePut must have exactly one part");
                let part = &self.parts[0];
                format!("\"{}\"", hex::encode(part.md5))
            }
            UploadMode::Multipart => {
                let mut hasher = Md5::new();
                for part in &self.parts {
                    hasher.update(part.md5);
                }
                let digest = hasher.finalize();
                format!("\"{}-{}\"", hex::encode(digest), self.parts.len())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersioningState {
    #[default]
    Off,
    Enabled,
    Suspended,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketConfig {
    pub created_at: OffsetDateTime,
    pub versioning: VersioningState,
    pub region: String,
}

impl BucketConfig {
    pub fn new(region: impl Into<String>) -> Self {
        Self {
            created_at: OffsetDateTime::now_utc(),
            versioning: VersioningState::Off,
            region: region.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartState {
    Live,
    GcPending,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartEntry {
    pub refcount: u32,
    pub size: u64,
    pub state: PartState,
    pub created_at: OffsetDateTime,
}

/// Returned by manifest delete/replace operations. Lists parts whose refcount
/// dropped to 0 and are now eligible for GC. Forces the caller to consider
/// the GC path — they cannot accidentally leak references.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeletionEffect {
    pub freed_parts: Vec<Hash>,
}
