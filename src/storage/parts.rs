use std::path::{Path, PathBuf};

use blake3::Hasher as Blake3Hasher;
use bytes::Bytes;
use md5::{Digest, Md5};
use tokio::fs;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use crate::storage::manifest::Hash;

#[derive(Debug, thiserror::Error)]
pub enum PartError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Copy)]
pub struct PartWriteResult {
    pub hash: Hash,
    pub md5: [u8; 16],
    pub size: u64,
}

/// Content-addressed immutable blob store.
///
/// Owns `data_dir` privately. All `parts/` and `tmp/` access goes through this
/// type — no other module sees the underlying paths. External consumers receive
/// only the hash + an `AsyncRead` handle.
pub struct PartStore {
    parts_dir: PathBuf,
    tmp_dir: PathBuf,
}

impl PartStore {
    pub async fn open(data_dir: impl AsRef<Path>) -> Result<Self, PartError> {
        let data_dir = data_dir.as_ref();
        let parts_dir = data_dir.join("parts");
        let tmp_dir = data_dir.join("tmp");
        fs::create_dir_all(&parts_dir).await?;
        fs::create_dir_all(&tmp_dir).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(data_dir, std::fs::Permissions::from_mode(0o700)).await?;
            fs::set_permissions(&parts_dir, std::fs::Permissions::from_mode(0o700)).await?;
            fs::set_permissions(&tmp_dir, std::fs::Permissions::from_mode(0o700)).await?;
        }
        Ok(Self { parts_dir, tmp_dir })
    }

    /// Streaming write. Single pass computes BLAKE3 (content address) and MD5
    /// (S3 ETag input) while writing to a tmp file, then atomically renames
    /// into `parts/aa/bb/<hex>`. If a part with the same hash already exists,
    /// the tmp file is dropped (natural dedup).
    pub async fn write_stream<R>(&self, mut reader: R) -> Result<PartWriteResult, PartError>
    where
        R: AsyncRead + Unpin,
    {
        let tmp_path = self.tmp_dir.join(Uuid::new_v4().to_string());
        let mut tmp_file = fs::File::create(&tmp_path).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tmp_file
                .set_permissions(std::fs::Permissions::from_mode(0o600))
                .await?;
        }

        let mut blake3 = Blake3Hasher::new();
        let mut md5 = Md5::new();
        let mut size: u64 = 0;
        let mut buf = vec![0u8; 64 * 1024];

        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let chunk = &buf[..n];
            tmp_file.write_all(chunk).await?;
            blake3.update(chunk);
            md5.update(chunk);
            size += n as u64;
        }
        tmp_file.flush().await?;
        tmp_file.sync_all().await?;
        drop(tmp_file);

        let hash: Hash = blake3.finalize().into();
        let md5_digest: [u8; 16] = md5.finalize().into();

        let dest = self.path_for(&hash);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).await?;
        }
        if fs::metadata(&dest).await.is_ok() {
            let _ = fs::remove_file(&tmp_path).await;
        } else {
            fs::rename(&tmp_path, &dest).await?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o600)).await?;
            }
        }

        Ok(PartWriteResult {
            hash,
            md5: md5_digest,
            size,
        })
    }

    /// Convenience variant for already-buffered payloads (sigv4 middleware
    /// has buffered the request body to `Bytes`). Same tmp→rename semantics
    /// and dedup behavior as `write_stream`.
    pub async fn write_bytes(&self, data: Bytes) -> Result<PartWriteResult, PartError> {
        self.write_stream(&data[..]).await
    }

    pub async fn open_read(&self, hash: &Hash) -> Result<fs::File, PartError> {
        let path = self.path_for(hash);
        Ok(fs::File::open(&path).await?)
    }

    pub async fn exists(&self, hash: &Hash) -> bool {
        let path = self.path_for(hash);
        fs::metadata(&path).await.is_ok()
    }

    /// Unlink the part file. Idempotent: not-found is success.
    pub async fn delete(&self, hash: &Hash) -> Result<(), PartError> {
        let path = self.path_for(hash);
        match fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn path_for(&self, hash: &Hash) -> PathBuf {
        let hex = hex::encode(hash);
        self.parts_dir.join(&hex[..2]).join(&hex[2..4]).join(&hex)
    }
}
