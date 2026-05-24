use redb::TableDefinition;

/// key: bucket name (string)
/// value: bincode-encoded `BucketConfig`
pub const BUCKETS: TableDefinition<&str, &[u8]> = TableDefinition::new("buckets");

/// key: `ManifestKey::encode()` — null-separated `bucket\0key\0version_id`
/// value: bincode-encoded `Manifest`
pub const OBJECTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("objects");

/// key: 32-byte BLAKE3 hash
/// value: bincode-encoded `PartEntry`
pub const PARTS: TableDefinition<&[u8; 32], &[u8]> = TableDefinition::new("parts");

/// key: short name (e.g. "region", "root_access_key")
/// value: bincode-encoded value
pub const SERVER_META: TableDefinition<&str, &[u8]> = TableDefinition::new("server_meta");

/// key: monotonic u64 sequence
/// value: bincode-encoded webhook DLQ entry (filled in later phases)
pub const EVENTS_DLQ: TableDefinition<u64, &[u8]> = TableDefinition::new("events_dlq");
