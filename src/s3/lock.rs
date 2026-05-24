use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::http::error::{S3Error, S3ErrorCode};
use crate::storage::manifest::{
    DefaultRetention, LockMode, ManifestKey, ObjectLock, ObjectLockConfig, Retention,
};
use crate::storage::MetaError;

use super::bucket::map_meta_err;
use super::state::AppState;
use super::xml::{
    DefaultRetentionXml, LegalHoldXml, ObjectLockConfigurationXml, ObjectLockRuleXml, RetentionXml,
    XmlBody,
};

/// `PUT /bucket?object-lock` — set or update the bucket-level object-lock
/// configuration. Enabling Object Lock on an existing bucket whose lock was
/// never enabled at creation is rejected (S3 requires lock-at-creation).
pub async fn put_bucket_object_lock_config(
    state: AppState,
    bucket: &str,
    body: Bytes,
) -> Result<Response, S3Error> {
    let xml: ObjectLockConfigurationXml = quick_xml::de::from_reader(body.as_ref()).map_err(|e| {
        S3Error::new(
            S3ErrorCode::InvalidRequest,
            format!("malformed ObjectLockConfiguration body: {e}"),
        )
        .with_resource(format!("/{bucket}"))
    })?;

    let existing = state
        .meta
        .get_bucket(bucket)
        .await
        .map_err(map_meta_err)?
        .ok_or_else(|| {
            S3Error::new(S3ErrorCode::NoSuchBucket, "bucket does not exist")
                .with_resource(format!("/{bucket}"))
        })?;

    let was_enabled = existing
        .object_lock
        .as_ref()
        .map(|c| c.enabled)
        .unwrap_or(false);
    let now_enabled = xml.object_lock_enabled.as_deref() == Some("Enabled");

    if now_enabled && !was_enabled {
        return Err(S3Error::new(
            S3ErrorCode::InvalidRequest,
            "Object Lock must be enabled at bucket creation (x-amz-bucket-object-lock-enabled)",
        )
        .with_resource(format!("/{bucket}")));
    }

    let default_retention = xml.rule.and_then(|r| r.default_retention).map(|d| {
        Ok::<_, S3Error>(DefaultRetention {
            mode: parse_mode(d.mode.as_deref())?,
            days: d.days,
            years: d.years,
        })
    }).transpose()?;

    let cfg = ObjectLockConfig {
        enabled: was_enabled || now_enabled,
        default_retention,
    };
    state
        .meta
        .update_bucket_object_lock(bucket, Some(cfg))
        .await
        .map_err(map_meta_err)?;
    Ok(StatusCode::OK.into_response())
}

/// `GET /bucket?object-lock` — return the bucket's object-lock configuration.
pub async fn get_bucket_object_lock_config(
    state: AppState,
    bucket: &str,
) -> Result<Response, S3Error> {
    let cfg = state
        .meta
        .get_bucket(bucket)
        .await
        .map_err(map_meta_err)?
        .ok_or_else(|| {
            S3Error::new(S3ErrorCode::NoSuchBucket, "bucket does not exist")
                .with_resource(format!("/{bucket}"))
        })?;
    let lock_cfg = cfg.object_lock.unwrap_or_default();
    let body = ObjectLockConfigurationXml {
        object_lock_enabled: if lock_cfg.enabled {
            Some("Enabled".to_string())
        } else {
            None
        },
        rule: lock_cfg.default_retention.map(|d| ObjectLockRuleXml {
            default_retention: Some(DefaultRetentionXml {
                mode: Some(mode_to_str(d.mode).to_string()),
                days: d.days,
                years: d.years,
            }),
        }),
    };
    Ok(XmlBody(body).into_response())
}

/// `PUT /bucket/key?retention` — set the retention block on a specific object
/// version.
pub async fn put_object_retention(
    state: AppState,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    body: Bytes,
) -> Result<Response, S3Error> {
    let xml: RetentionXml = quick_xml::de::from_reader(body.as_ref()).map_err(|e| {
        S3Error::new(
            S3ErrorCode::InvalidRequest,
            format!("malformed Retention body: {e}"),
        )
        .with_resource(format!("/{bucket}/{key}"))
    })?;
    let mode = parse_mode(Some(xml.mode.as_str()))?;
    let retain_until = OffsetDateTime::parse(&xml.retain_until_date, &Rfc3339).map_err(|_| {
        S3Error::new(
            S3ErrorCode::InvalidRequest,
            "RetainUntilDate must be ISO 8601",
        )
        .with_resource(format!("/{bucket}/{key}"))
    })?;

    let manifest = require_object_with_version(&state, bucket, key, version_id).await?;
    // Compliance retention cannot be shortened (S3 rule). For Phase 10 baseline
    // we allow extension only.
    if let Some(existing_lock) = &manifest.object_lock
        && let Some(existing) = &existing_lock.retention
        && retain_until < existing.retain_until
    {
        return Err(S3Error::new(
            S3ErrorCode::AccessForbidden,
            "Compliance retention period cannot be shortened",
        )
        .with_resource(format!("/{bucket}/{key}")));
    }

    let new_lock = ObjectLock {
        retention: Some(Retention {
            mode,
            retain_until,
        }),
        legal_hold: manifest
            .object_lock
            .as_ref()
            .map(|l| l.legal_hold)
            .unwrap_or(false),
    };
    state
        .meta
        .update_manifest_lock(manifest.key.clone(), Some(new_lock))
        .await
        .map_err(map_meta_err)?;
    Ok(StatusCode::OK.into_response())
}

/// `GET /bucket/key?retention` — return the current retention block (empty
/// XML when no retention is set).
pub async fn get_object_retention(
    state: AppState,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<Response, S3Error> {
    let manifest = require_object_with_version(&state, bucket, key, version_id).await?;
    let Some(lock) = manifest.object_lock else {
        return Err(S3Error::new(
            S3ErrorCode::InvalidRequest,
            "no retention configured for this object",
        )
        .with_resource(format!("/{bucket}/{key}")));
    };
    let retention = lock.retention.ok_or_else(|| {
        S3Error::new(
            S3ErrorCode::InvalidRequest,
            "no retention configured for this object",
        )
        .with_resource(format!("/{bucket}/{key}"))
    })?;
    let body = RetentionXml {
        mode: mode_to_str(retention.mode).to_string(),
        retain_until_date: retention
            .retain_until
            .format(&Rfc3339)
            .unwrap_or_default(),
    };
    Ok(XmlBody(body).into_response())
}

/// `PUT /bucket/key?legal-hold` — set or clear the legal-hold flag.
pub async fn put_object_legal_hold(
    state: AppState,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    body: Bytes,
) -> Result<Response, S3Error> {
    let xml: LegalHoldXml = quick_xml::de::from_reader(body.as_ref()).map_err(|e| {
        S3Error::new(
            S3ErrorCode::InvalidRequest,
            format!("malformed LegalHold body: {e}"),
        )
        .with_resource(format!("/{bucket}/{key}"))
    })?;
    let on = match xml.status.as_str() {
        "ON" => true,
        "OFF" => false,
        other => {
            return Err(S3Error::new(
                S3ErrorCode::InvalidArgument,
                format!("invalid LegalHold Status '{other}'"),
            )
            .with_resource(format!("/{bucket}/{key}")));
        }
    };

    let manifest = require_object_with_version(&state, bucket, key, version_id).await?;
    let new_lock = ObjectLock {
        retention: manifest.object_lock.as_ref().and_then(|l| l.retention.clone()),
        legal_hold: on,
    };
    state
        .meta
        .update_manifest_lock(manifest.key.clone(), Some(new_lock))
        .await
        .map_err(map_meta_err)?;
    Ok(StatusCode::OK.into_response())
}

/// `GET /bucket/key?legal-hold` — return the current legal-hold flag.
pub async fn get_object_legal_hold(
    state: AppState,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<Response, S3Error> {
    let manifest = require_object_with_version(&state, bucket, key, version_id).await?;
    let status = manifest
        .object_lock
        .map(|l| if l.legal_hold { "ON" } else { "OFF" })
        .unwrap_or("OFF")
        .to_string();
    Ok(XmlBody(LegalHoldXml { status }).into_response())
}

/// Helper: parse a lock from PUT-object headers. Returns None if no lock
/// headers are present.
pub fn parse_lock_headers(headers: &HeaderMap) -> Result<Option<ObjectLock>, S3Error> {
    let mode = headers
        .get("x-amz-object-lock-mode")
        .and_then(|v| v.to_str().ok());
    let retain_until = headers
        .get("x-amz-object-lock-retain-until-date")
        .and_then(|v| v.to_str().ok());
    let legal_hold = headers
        .get("x-amz-object-lock-legal-hold")
        .and_then(|v| v.to_str().ok());

    if mode.is_none() && retain_until.is_none() && legal_hold.is_none() {
        return Ok(None);
    }

    let retention = match (mode, retain_until) {
        (Some(m), Some(d)) => {
            let mode = parse_mode(Some(m))?;
            let retain_until = OffsetDateTime::parse(d, &Rfc3339).map_err(|_| {
                S3Error::new(
                    S3ErrorCode::InvalidRequest,
                    "x-amz-object-lock-retain-until-date must be ISO 8601",
                )
            })?;
            Some(Retention { mode, retain_until })
        }
        (None, None) => None,
        _ => {
            return Err(S3Error::new(
                S3ErrorCode::InvalidRequest,
                "x-amz-object-lock-mode and x-amz-object-lock-retain-until-date must be set together",
            ));
        }
    };
    let legal_hold = match legal_hold {
        Some("ON") => true,
        Some("OFF") | None => false,
        Some(other) => {
            return Err(S3Error::new(
                S3ErrorCode::InvalidArgument,
                format!("invalid x-amz-object-lock-legal-hold '{other}'"),
            ));
        }
    };
    Ok(Some(ObjectLock {
        retention,
        legal_hold,
    }))
}

/// Compute the default lock to apply to a freshly-PUT object based on the
/// bucket configuration. Per-object headers override these defaults.
pub fn default_lock_for_bucket(cfg: &Option<ObjectLockConfig>) -> Option<ObjectLock> {
    let lock_cfg = cfg.as_ref()?;
    if !lock_cfg.enabled {
        return None;
    }
    let default = lock_cfg.default_retention.as_ref()?;
    let now = OffsetDateTime::now_utc();
    let retain_until = if let Some(days) = default.days {
        now + time::Duration::days(days as i64)
    } else if let Some(years) = default.years {
        now + time::Duration::days((years as i64) * 365)
    } else {
        return None;
    };
    Some(ObjectLock {
        retention: Some(Retention {
            mode: default.mode,
            retain_until,
        }),
        legal_hold: false,
    })
}

// ---------------- helpers ----------------

async fn require_object_with_version(
    state: &AppState,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<crate::storage::manifest::Manifest, S3Error> {
    let result = match version_id {
        Some(vid) => state
            .meta
            .get_manifest(ManifestKey::new(bucket, key, vid))
            .await
            .map_err(map_meta_err)?,
        None => state
            .meta
            .get_latest_version(bucket, key)
            .await
            .map_err(map_meta_err)?,
    };
    result
        .filter(|m| {
            matches!(m.state, crate::storage::manifest::ManifestState::Committed)
                && matches!(m.kind, crate::storage::manifest::ManifestKind::Object)
        })
        .ok_or_else(|| {
            S3Error::new(S3ErrorCode::NoSuchKey, "key does not exist")
                .with_resource(format!("/{bucket}/{key}"))
        })
}

fn parse_mode(value: Option<&str>) -> Result<LockMode, S3Error> {
    match value {
        Some("COMPLIANCE") => Ok(LockMode::Compliance),
        Some("GOVERNANCE") => Err(S3Error::new(
            S3ErrorCode::InvalidArgument,
            "GOVERNANCE mode is not supported (Compliance only)",
        )),
        Some(other) => Err(S3Error::new(
            S3ErrorCode::InvalidArgument,
            format!("invalid lock Mode '{other}'"),
        )),
        None => Err(S3Error::new(
            S3ErrorCode::InvalidRequest,
            "lock Mode is required",
        )),
    }
}

fn mode_to_str(mode: LockMode) -> &'static str {
    match mode {
        LockMode::Compliance => "COMPLIANCE",
    }
}

#[allow(dead_code)]
fn _meta_err_used() -> std::marker::PhantomData<MetaError> {
    std::marker::PhantomData
}
