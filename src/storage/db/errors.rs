use crate::storage::manifest::ManifestKey;

#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error("redb: {0}")]
    Redb(String),
    #[error("bincode encode: {0}")]
    BincodeEncode(String),
    #[error("bincode decode: {0}")]
    BincodeDecode(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("actor thread spawn: {0}")]
    ActorSpawn(String),
    #[error("actor died unexpectedly")]
    ActorDied,
    #[error("bucket already exists: {0}")]
    BucketExists(String),
    #[error("bucket not found: {0}")]
    BucketNotFound(String),
    #[error("bucket not empty: {0}")]
    BucketNotEmpty(String),
    #[error("manifest not found: {0:?}")]
    ManifestNotFound(ManifestKey),
    #[error("part is gc_pending, cannot reference: {0}")]
    PartGcPending(String),
    #[error("part not found: {0}")]
    PartNotFound(String),
}

impl From<redb::DatabaseError> for MetaError {
    fn from(e: redb::DatabaseError) -> Self {
        MetaError::Redb(e.to_string())
    }
}
impl From<redb::TransactionError> for MetaError {
    fn from(e: redb::TransactionError) -> Self {
        MetaError::Redb(e.to_string())
    }
}
impl From<redb::TableError> for MetaError {
    fn from(e: redb::TableError) -> Self {
        MetaError::Redb(e.to_string())
    }
}
impl From<redb::StorageError> for MetaError {
    fn from(e: redb::StorageError) -> Self {
        MetaError::Redb(e.to_string())
    }
}
impl From<redb::CommitError> for MetaError {
    fn from(e: redb::CommitError) -> Self {
        MetaError::Redb(e.to_string())
    }
}
