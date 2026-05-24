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

/// S3 event types emitted by handlers. The string form is what AWS uses in
/// notification payloads (matches the `s3:` event filter language).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    ObjectCreatedPut,
    ObjectCreatedPost,
    ObjectCreatedCopy,
    ObjectCreatedCompleteMultipartUpload,
    ObjectRemovedDelete,
    ObjectRemovedDeleteMarkerCreated,
}

impl EventType {
    pub fn as_str(self) -> &'static str {
        match self {
            EventType::ObjectCreatedPut => "s3:ObjectCreated:Put",
            EventType::ObjectCreatedPost => "s3:ObjectCreated:Post",
            EventType::ObjectCreatedCopy => "s3:ObjectCreated:Copy",
            EventType::ObjectCreatedCompleteMultipartUpload => {
                "s3:ObjectCreated:CompleteMultipartUpload"
            }
            EventType::ObjectRemovedDelete => "s3:ObjectRemoved:Delete",
            EventType::ObjectRemovedDeleteMarkerCreated => "s3:ObjectRemoved:DeleteMarkerCreated",
        }
    }
}

/// A single webhook subscription. Matches when the bucket equals (empty bucket
/// means any-bucket), the event type is in `events` (empty = all events), and
/// the key passes the optional prefix/suffix filter.
#[derive(Debug, Clone)]
pub struct WebhookSubscription {
    pub bucket: String,
    pub events: Vec<EventType>,
    pub prefix: Option<String>,
    pub suffix: Option<String>,
    pub url: String,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub region: String,
    pub root_key: RootKey,
    pub listen_addr: SocketAddr,
    /// Public host suffix used to detect virtual-hosted addressing.
    /// If the incoming `Host` matches `<bucket>.<endpoint_host>`, the bucket
    /// is extracted from the subdomain; otherwise path-style is assumed.
    /// `None` disables virtual-hosted detection (path-style only).
    pub endpoint_host: Option<String>,
    /// Maximum bytes buffered from a request body for Sigv4 verification.
    /// Larger bodies (multipart UploadPart with signed payload) hit this
    /// limit; streaming-signed and unsigned-payload paths skip buffering
    /// (Phase 6+).
    pub max_signed_body_bytes: usize,
    /// Outbound webhook subscriptions configured at server startup. Empty by
    /// default; loaded from `config.toml` once Phase 14 lands.
    pub webhook_subscriptions: Vec<WebhookSubscription>,
    /// Default `false`: SSRF defense rejects webhook URLs that resolve to
    /// loopback / private / link-local IPs. Tests flip this on so they can
    /// run a local HTTP sink on 127.0.0.1.
    pub allow_loopback_webhooks: bool,
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
            endpoint_host: None,
            max_signed_body_bytes: 64 * 1024 * 1024,
            webhook_subscriptions: Vec::new(),
            allow_loopback_webhooks: false,
        })
    }
}
