use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::Deserialize;

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

// ---------------- TOML config file ----------------

/// On-disk shape of `config.toml`. Decoupled from `ServerConfig` so the file
/// schema can evolve without affecting the in-memory representation.
#[derive(Debug, Clone, Deserialize)]
pub struct ConfigFile {
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub listen_addr: SocketAddr,
    pub data_dir: PathBuf,
    #[serde(default)]
    pub endpoint_host: Option<String>,
    #[serde(default)]
    pub max_signed_body_bytes: Option<usize>,
    #[serde(default)]
    pub allow_loopback_webhooks: bool,
    #[serde(default, rename = "webhook")]
    pub webhooks: Vec<WebhookFile>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WebhookFile {
    pub url: String,
    #[serde(default)]
    pub bucket: String,
    #[serde(default)]
    pub events: Vec<String>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub suffix: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("toml: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("unknown event type '{0}'")]
    UnknownEventType(String),
}

/// Parse a TOML config file into the runtime config + data dir path.
pub fn load_config(path: &Path) -> Result<(Arc<ServerConfig>, PathBuf), ConfigError> {
    let text = std::fs::read_to_string(path)?;
    let file: ConfigFile = toml::from_str(&text)?;
    let mut subs = Vec::with_capacity(file.webhooks.len());
    for w in file.webhooks {
        let mut events = Vec::with_capacity(w.events.len());
        for e in w.events {
            events.push(parse_event_type(&e)?);
        }
        subs.push(WebhookSubscription {
            bucket: w.bucket,
            events,
            prefix: w.prefix,
            suffix: w.suffix,
            url: w.url,
        });
    }
    let cfg = ServerConfig {
        region: file.region,
        root_key: RootKey {
            access_key_id: file.access_key_id,
            secret_access_key: file.secret_access_key,
        },
        listen_addr: file.listen_addr,
        endpoint_host: file.endpoint_host,
        max_signed_body_bytes: file
            .max_signed_body_bytes
            .unwrap_or(64 * 1024 * 1024),
        webhook_subscriptions: subs,
        allow_loopback_webhooks: file.allow_loopback_webhooks,
    };
    Ok((Arc::new(cfg), file.data_dir))
}

fn parse_event_type(s: &str) -> Result<EventType, ConfigError> {
    Ok(match s {
        "ObjectCreated:Put" | "s3:ObjectCreated:Put" => EventType::ObjectCreatedPut,
        "ObjectCreated:Post" | "s3:ObjectCreated:Post" => EventType::ObjectCreatedPost,
        "ObjectCreated:Copy" | "s3:ObjectCreated:Copy" => EventType::ObjectCreatedCopy,
        "ObjectCreated:CompleteMultipartUpload" | "s3:ObjectCreated:CompleteMultipartUpload" => {
            EventType::ObjectCreatedCompleteMultipartUpload
        }
        "ObjectRemoved:Delete" | "s3:ObjectRemoved:Delete" => EventType::ObjectRemovedDelete,
        "ObjectRemoved:DeleteMarkerCreated" | "s3:ObjectRemoved:DeleteMarkerCreated" => {
            EventType::ObjectRemovedDeleteMarkerCreated
        }
        other => return Err(ConfigError::UnknownEventType(other.to_string())),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn load_minimal_config() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
region = "us-east-1"
access_key_id = "AK"
secret_access_key = "SK"
listen_addr = "127.0.0.1:9000"
data_dir = "/tmp/s3lite-data"
"#,
        )
        .unwrap();
        let (cfg, data_dir) = load_config(&path).unwrap();
        assert_eq!(cfg.region, "us-east-1");
        assert_eq!(cfg.root_key.access_key_id, "AK");
        assert_eq!(data_dir, PathBuf::from("/tmp/s3lite-data"));
        assert_eq!(cfg.webhook_subscriptions.len(), 0);
    }

    #[test]
    fn load_config_with_webhooks() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
region = "us-east-1"
access_key_id = "AK"
secret_access_key = "SK"
listen_addr = "0.0.0.0:80"
data_dir = "/data"
allow_loopback_webhooks = true

[[webhook]]
url = "https://hook.example.com/s3"
bucket = "photos"
events = ["s3:ObjectCreated:Put"]
prefix = "uploads/"
"#,
        )
        .unwrap();
        let (cfg, _) = load_config(&path).unwrap();
        assert_eq!(cfg.webhook_subscriptions.len(), 1);
        let w = &cfg.webhook_subscriptions[0];
        assert_eq!(w.bucket, "photos");
        assert_eq!(w.events, vec![EventType::ObjectCreatedPut]);
        assert_eq!(w.prefix.as_deref(), Some("uploads/"));
        assert!(cfg.allow_loopback_webhooks);
    }

    #[test]
    fn load_rejects_unknown_event() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
region = "us-east-1"
access_key_id = "AK"
secret_access_key = "SK"
listen_addr = "127.0.0.1:0"
data_dir = "/data"

[[webhook]]
url = "https://h"
events = ["s3:NotARealEvent"]
"#,
        )
        .unwrap();
        let err = load_config(&path).unwrap_err();
        assert!(matches!(err, ConfigError::UnknownEventType(_)));
    }
}
