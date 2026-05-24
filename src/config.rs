use std::net::SocketAddr;
use std::sync::Arc;

/// Single root credential pair for Sigv4. There is exactly one root key
/// per server (per s3lite's design). The actual secret never leaves this
/// module's owners.
#[derive(Debug, Clone)]
pub struct RootKey {
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub region: String,
    pub root_key: RootKey,
    pub listen_addr: SocketAddr,
    /// Maximum bytes buffered from a request body for Sigv4 verification.
    /// Larger bodies (multipart UploadPart with signed payload) hit this
    /// limit; streaming-signed and unsigned-payload paths skip buffering
    /// (Phase 6+).
    pub max_signed_body_bytes: usize,
}

impl ServerConfig {
    pub fn new(
        region: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        listen_addr: SocketAddr,
    ) -> Arc<Self> {
        Arc::new(Self {
            region: region.into(),
            root_key: RootKey {
                access_key_id: access_key_id.into(),
                secret_access_key: secret_access_key.into(),
            },
            listen_addr,
            max_signed_body_bytes: 64 * 1024 * 1024,
        })
    }
}
