use std::net::IpAddr;
use std::time::Duration;

use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

use crate::config::{EventType, WebhookSubscription};

use super::state::AppState;

const MAX_RETRIES: u32 = 1; // single retry after initial attempt (= 2 total)
const PER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// Per-event metadata captured from the handler at emission time. Kept small
/// so the `tokio::spawn` capture stays cheap.
#[derive(Debug, Clone)]
pub struct ObjectEvent {
    pub event_type: EventType,
    pub bucket: String,
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub version_id: Option<String>,
}

/// Fire-and-forget dispatch: enumerate matching subscriptions and spawn a
/// delivery task for each. Returns immediately so the request handler can
/// reply without waiting on the webhook RTT.
pub fn emit(state: &AppState, event: ObjectEvent) {
    let config = state.config_snapshot();
    let subs: Vec<WebhookSubscription> = config
        .webhook_subscriptions
        .iter()
        .filter(|s| matches_subscription(s, &event))
        .cloned()
        .collect();
    if subs.is_empty() {
        return;
    }
    let payload = match build_payload(&config, event.clone()) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, "failed to build event payload");
            return;
        }
    };
    let allow_loopback = config.allow_loopback_webhooks;
    let meta = state.meta.clone();
    for sub in subs {
        let payload = payload.clone();
        let meta = meta.clone();
        tokio::spawn(async move {
            deliver_with_retry(sub.url, payload, allow_loopback, meta).await;
        });
    }
}

fn matches_subscription(sub: &WebhookSubscription, event: &ObjectEvent) -> bool {
    if !sub.bucket.is_empty() && sub.bucket != event.bucket {
        return false;
    }
    if !sub.events.is_empty() && !sub.events.contains(&event.event_type) {
        return false;
    }
    if let Some(prefix) = &sub.prefix
        && !event.key.starts_with(prefix.as_str())
    {
        return false;
    }
    if let Some(suffix) = &sub.suffix
        && !event.key.ends_with(suffix.as_str())
    {
        return false;
    }
    true
}

#[derive(Serialize)]
struct EventPayload {
    #[serde(rename = "Records")]
    records: Vec<EventRecord>,
}

#[derive(Serialize)]
struct EventRecord {
    #[serde(rename = "eventVersion")]
    event_version: &'static str,
    #[serde(rename = "eventSource")]
    event_source: &'static str,
    #[serde(rename = "awsRegion")]
    aws_region: String,
    #[serde(rename = "eventTime")]
    event_time: String,
    #[serde(rename = "eventName")]
    event_name: String,
    s3: EventS3,
}

#[derive(Serialize)]
struct EventS3 {
    #[serde(rename = "s3SchemaVersion")]
    schema_version: &'static str,
    bucket: EventBucket,
    object: EventObject,
}

#[derive(Serialize)]
struct EventBucket {
    name: String,
    arn: String,
}

#[derive(Serialize)]
struct EventObject {
    key: String,
    size: u64,
    #[serde(rename = "eTag")]
    etag: String,
    #[serde(rename = "versionId", skip_serializing_if = "Option::is_none")]
    version_id: Option<String>,
    sequencer: String,
}

fn build_payload(
    config: &crate::config::ServerConfig,
    event: ObjectEvent,
) -> Result<String, serde_json::Error> {
    let payload = EventPayload {
        records: vec![EventRecord {
            event_version: "2.1",
            event_source: "s3lite",
            aws_region: config.region.clone(),
            event_time: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_default(),
            event_name: event.event_type.as_str().to_string(),
            s3: EventS3 {
                schema_version: "1.0",
                bucket: EventBucket {
                    arn: format!("arn:s3lite:::{}", event.bucket),
                    name: event.bucket,
                },
                object: EventObject {
                    key: event.key,
                    size: event.size,
                    etag: event.etag,
                    version_id: event.version_id,
                    // Sequencer is meant to be a monotonically-increasing token
                    // letting consumers order events for the same key. A uuid
                    // simple id is unique-enough and stable-shaped for clients
                    // that just compare strings.
                    sequencer: Uuid::new_v4().simple().to_string(),
                },
            },
        }],
    };
    serde_json::to_string(&payload)
}

async fn deliver_with_retry(
    url: String,
    payload: String,
    allow_loopback: bool,
    meta: std::sync::Arc<crate::storage::MetaStore>,
) {
    let client = match reqwest::Client::builder()
        .timeout(PER_ATTEMPT_TIMEOUT)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "reqwest client init failed");
            return;
        }
    };

    let mut last_error: Option<String> = None;
    for attempt in 0..=MAX_RETRIES {
        if !ssrf_safe(&url, allow_loopback).await {
            last_error = Some(format!("blocked by SSRF policy: {url}"));
            break;
        }
        match client
            .post(&url)
            .header("content-type", "application/json")
            .header("x-amz-event-source", "s3lite")
            .body(payload.clone())
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => return,
            Ok(resp) => {
                last_error = Some(format!("HTTP {}", resp.status()));
            }
            Err(e) => {
                last_error = Some(format!("transport error: {e}"));
            }
        }
        if attempt < MAX_RETRIES {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    // All attempts failed — push to DLQ for operator inspection.
    let dlq = DlqRecord {
        url,
        payload,
        last_error: last_error.unwrap_or_else(|| "unknown".to_string()),
        attempted_at: OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_default(),
    };
    let bytes = match bincode::serde::encode_to_vec(&dlq, bincode::config::standard()) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, "failed to encode dlq record");
            return;
        }
    };
    if let Err(e) = meta.insert_dlq_entry(bytes).await {
        tracing::warn!(error = %e, "failed to push to DLQ");
    }
}

/// Resolve the URL host and check none of the addresses are loopback, private,
/// or link-local. Performed at every dispatch (defeats DNS rebinding).
async fn ssrf_safe(url: &str, allow_loopback: bool) -> bool {
    let parsed = match url::Url::parse(url) {
        Ok(u) => u,
        Err(_) => return false,
    };
    if !matches!(parsed.scheme(), "http" | "https") {
        return false;
    }
    let host = match parsed.host_str() {
        Some(h) => h,
        None => return false,
    };
    let port = parsed.port_or_known_default().unwrap_or(80);
    let lookup = match tokio::net::lookup_host((host, port)).await {
        Ok(l) => l,
        Err(_) => return false,
    };
    for addr in lookup {
        let ip = addr.ip();
        if !ip_allowed(&ip, allow_loopback) {
            return false;
        }
    }
    true
}

fn ip_allowed(ip: &IpAddr, allow_loopback: bool) -> bool {
    if ip.is_loopback() {
        return allow_loopback;
    }
    if ip.is_unspecified() || ip.is_multicast() {
        return false;
    }
    match ip {
        IpAddr::V4(v4) => {
            // RFC 1918 private, link-local, CGNAT
            if v4.is_private() || v4.is_link_local() || v4.octets()[0] == 100 {
                return false;
            }
        }
        IpAddr::V6(v6) => {
            // Unique-local fc00::/7 and link-local fe80::/10
            let first = v6.segments()[0];
            if (first & 0xfe00) == 0xfc00 || (first & 0xffc0) == 0xfe80 {
                return false;
            }
        }
    }
    true
}

#[derive(serde::Serialize, serde::Deserialize)]
struct DlqRecord {
    url: String,
    payload: String,
    last_error: String,
    attempted_at: String,
}

/// Decode an opaque DLQ entry into the human-readable record. Used by tests.
pub fn decode_dlq_entry(bytes: &[u8]) -> Result<DlqRecordOwned, String> {
    let (rec, _) = bincode::serde::decode_from_slice::<DlqRecord, _>(
        bytes,
        bincode::config::standard(),
    )
    .map_err(|e| e.to_string())?;
    Ok(DlqRecordOwned {
        url: rec.url,
        payload: rec.payload,
        last_error: rec.last_error,
        attempted_at: rec.attempted_at,
    })
}

#[derive(Debug, Clone)]
pub struct DlqRecordOwned {
    pub url: String,
    pub payload: String,
    pub last_error: String,
    pub attempted_at: String,
}
