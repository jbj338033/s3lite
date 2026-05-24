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

/// Retention block of an Object Lock — both fields move together: present
/// only when the object has an active retain-until clause.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Retention {
    pub mode: LockMode,
    pub retain_until: OffsetDateTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectLock {
    pub retention: Option<Retention>,
    pub legal_hold: bool,
}

impl ObjectLock {
    /// True if the lock currently forbids deletion: either retention has not
    /// yet expired, or a legal hold is in force.
    pub fn forbids_delete(&self, now: OffsetDateTime) -> bool {
        if self.legal_hold {
            return true;
        }
        match &self.retention {
            Some(r) => r.retain_until > now,
            None => false,
        }
    }
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

/// Bucket-level lifecycle rule. Phase 11 supports the three most common
/// actions: object expiration, noncurrent-version expiration, and aborting
/// long-lived in-progress multipart uploads. Filter is a simple key prefix
/// (richer filters land in later phases).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleRule {
    pub id: Option<String>,
    pub status: LifecycleStatus,
    pub filter_prefix: Option<String>,
    pub expiration: Option<LifecycleExpiration>,
    pub noncurrent_version_expiration: Option<NoncurrentVersionExpiration>,
    pub abort_incomplete_multipart_upload: Option<AbortIncompleteMultipart>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum LifecycleStatus {
    #[default]
    Disabled,
    Enabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleExpiration {
    /// Number of days after object creation when it expires.
    pub days: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoncurrentVersionExpiration {
    /// Number of days after a version became noncurrent. We approximate this
    /// with the version's `created_at` (we don't track demotion time).
    pub noncurrent_days: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbortIncompleteMultipart {
    pub days_after_initiation: u32,
}

/// Bucket-level Object Lock configuration. `enabled` is set at bucket
/// creation time (via `x-amz-bucket-object-lock-enabled: true`) and is
/// immutable thereafter — only `default_retention` can change.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectLockConfig {
    pub enabled: bool,
    pub default_retention: Option<DefaultRetention>,
}

/// Default retention applied to new objects when the bucket enables it.
/// `days` and `years` are mutually exclusive in AWS — only one is set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultRetention {
    pub mode: LockMode,
    pub days: Option<u32>,
    pub years: Option<u32>,
}

/// One bucket-level CORS rule. Matches AWS S3's CORSRule shape minus the
/// rarely-used `<ID>` element (kept as Option for round-trip clients).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorsRule {
    pub id: Option<String>,
    pub allowed_origins: Vec<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub expose_headers: Vec<String>,
    pub max_age_seconds: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketConfig {
    pub created_at: OffsetDateTime,
    pub versioning: VersioningState,
    pub region: String,
    #[serde(default)]
    pub cors_rules: Vec<CorsRule>,
    #[serde(default)]
    pub object_lock: Option<ObjectLockConfig>,
    #[serde(default)]
    pub lifecycle_rules: Vec<LifecycleRule>,
}

impl BucketConfig {
    pub fn new(region: impl Into<String>) -> Self {
        Self {
            created_at: OffsetDateTime::now_utc(),
            versioning: VersioningState::Off,
            region: region.into(),
            cors_rules: Vec::new(),
            object_lock: None,
            lifecycle_rules: Vec::new(),
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
