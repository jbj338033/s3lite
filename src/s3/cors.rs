use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;

use crate::http::error::{S3Error, S3ErrorCode};
use crate::storage::manifest::CorsRule;
use crate::storage::MetaError;

use super::bucket::map_meta_err;
use super::state::AppState;
use super::xml::{CorsConfiguration, CorsRuleXml, XmlBody};

/// `PUT /bucket?cors` — replace the bucket CORS configuration.
pub async fn put_bucket_cors(
    state: AppState,
    bucket: &str,
    body: Bytes,
) -> Result<Response, S3Error> {
    let parsed: CorsConfiguration = quick_xml::de::from_reader(body.as_ref()).map_err(|e| {
        S3Error::new(
            S3ErrorCode::InvalidRequest,
            format!("malformed CORSConfiguration body: {e}"),
        )
        .with_resource(format!("/{bucket}"))
    })?;
    let rules: Vec<CorsRule> = parsed
        .rules
        .into_iter()
        .map(|r| CorsRule {
            id: r.id,
            allowed_origins: r.allowed_origin,
            allowed_methods: r.allowed_method,
            allowed_headers: r.allowed_header,
            expose_headers: r.expose_header,
            max_age_seconds: r.max_age_seconds,
        })
        .collect();
    match state.meta.update_bucket_cors(bucket, rules).await {
        Ok(()) => Ok(StatusCode::OK.into_response()),
        Err(MetaError::BucketNotFound(_)) => Err(S3Error::new(
            S3ErrorCode::NoSuchBucket,
            "bucket does not exist",
        )
        .with_resource(format!("/{bucket}"))),
        Err(e) => Err(map_meta_err(e)),
    }
}

/// `GET /bucket?cors` — return the bucket CORS configuration. Empty body
/// (with a `NoSuchCORSConfiguration` error) when no rules are configured.
pub async fn get_bucket_cors(state: AppState, bucket: &str) -> Result<Response, S3Error> {
    let cfg = state
        .meta
        .get_bucket(bucket)
        .await
        .map_err(map_meta_err)?
        .ok_or_else(|| {
            S3Error::new(S3ErrorCode::NoSuchBucket, "bucket does not exist")
                .with_resource(format!("/{bucket}"))
        })?;
    if cfg.cors_rules.is_empty() {
        return Err(S3Error::new(
            S3ErrorCode::NoSuchCorsConfiguration,
            "no CORS configuration found",
        )
        .with_resource(format!("/{bucket}")));
    }
    let body = CorsConfiguration {
        rules: cfg
            .cors_rules
            .into_iter()
            .map(|r| CorsRuleXml {
                id: r.id,
                allowed_origin: r.allowed_origins,
                allowed_method: r.allowed_methods,
                allowed_header: r.allowed_headers,
                expose_header: r.expose_headers,
                max_age_seconds: r.max_age_seconds,
            })
            .collect(),
    };
    Ok(XmlBody(body).into_response())
}

/// `DELETE /bucket?cors` — remove all CORS rules.
pub async fn delete_bucket_cors(state: AppState, bucket: &str) -> Result<Response, S3Error> {
    match state.meta.update_bucket_cors(bucket, Vec::new()).await {
        Ok(()) => Ok(StatusCode::NO_CONTENT.into_response()),
        Err(MetaError::BucketNotFound(_)) => Err(S3Error::new(
            S3ErrorCode::NoSuchBucket,
            "bucket does not exist",
        )
        .with_resource(format!("/{bucket}"))),
        Err(e) => Err(map_meta_err(e)),
    }
}

/// `OPTIONS /bucket(/key)?` with `Origin` and `Access-Control-Request-Method`
/// — preflight check. Find a matching rule and emit Access-Control-* headers.
pub async fn preflight(
    state: AppState,
    bucket: &str,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    let origin = headers
        .get("origin")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            S3Error::new(S3ErrorCode::InvalidRequest, "preflight requires Origin header")
        })?;
    let method = headers
        .get("access-control-request-method")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            S3Error::new(
                S3ErrorCode::InvalidRequest,
                "preflight requires Access-Control-Request-Method",
            )
        })?;
    let requested_headers: Vec<String> = headers
        .get("access-control-request-headers")
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            s.split(',')
                .map(|p| p.trim().to_ascii_lowercase())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();

    let cfg = state
        .meta
        .get_bucket(bucket)
        .await
        .map_err(map_meta_err)?
        .ok_or_else(|| {
            S3Error::new(S3ErrorCode::NoSuchBucket, "bucket does not exist")
                .with_resource(format!("/{bucket}"))
        })?;

    let rule = pick_rule(&cfg.cors_rules, origin, method, &requested_headers).ok_or_else(|| {
        S3Error::new(
            S3ErrorCode::AccessForbidden,
            "no CORS rule matches the preflight request",
        )
    })?;

    let mut resp = StatusCode::OK.into_response();
    let h = resp.headers_mut();
    set_header(h, "access-control-allow-origin", echo_origin(rule, origin));
    set_header(h, "access-control-allow-methods", &rule.allowed_methods.join(", "));
    let allow_headers = if requested_headers.is_empty() {
        rule.allowed_headers.join(", ")
    } else {
        requested_headers.join(", ")
    };
    if !allow_headers.is_empty() {
        set_header(h, "access-control-allow-headers", &allow_headers);
    }
    if !rule.expose_headers.is_empty() {
        set_header(h, "access-control-expose-headers", &rule.expose_headers.join(", "));
    }
    if let Some(age) = rule.max_age_seconds {
        set_header(h, "access-control-max-age", &age.to_string());
    }
    set_header(h, "vary", "Origin, Access-Control-Request-Method, Access-Control-Request-Headers");
    Ok(resp)
}

/// Decorate the response of a real (non-preflight) cross-origin request with
/// `Access-Control-Allow-*` headers when a matching rule exists. Called on
/// every response by the dispatcher.
pub async fn apply_response_cors(
    state: &AppState,
    bucket: Option<&str>,
    request_headers: &HeaderMap,
    response: &mut Response,
    request_method: &str,
) {
    let Some(origin) = request_headers.get("origin").and_then(|v| v.to_str().ok()) else {
        return;
    };
    let Some(bucket) = bucket else { return };
    let Ok(Some(cfg)) = state.meta.get_bucket(bucket).await else {
        return;
    };
    let Some(rule) = pick_rule(&cfg.cors_rules, origin, request_method, &[]) else {
        return;
    };
    let h = response.headers_mut();
    set_header(h, "access-control-allow-origin", echo_origin(rule, origin));
    if !rule.expose_headers.is_empty() {
        set_header(h, "access-control-expose-headers", &rule.expose_headers.join(", "));
    }
    set_header(h, "vary", "Origin");
}

// ---------------- helpers ----------------

fn pick_rule<'a>(
    rules: &'a [CorsRule],
    origin: &str,
    method: &str,
    requested_headers: &[String],
) -> Option<&'a CorsRule> {
    rules.iter().find(|r| rule_matches(r, origin, method, requested_headers))
}

fn rule_matches(
    rule: &CorsRule,
    origin: &str,
    method: &str,
    requested_headers: &[String],
) -> bool {
    let origin_ok = rule
        .allowed_origins
        .iter()
        .any(|allowed| origin_matches(allowed, origin));
    let method_ok = rule
        .allowed_methods
        .iter()
        .any(|m| m.eq_ignore_ascii_case(method));
    let headers_ok = requested_headers.iter().all(|h| {
        rule.allowed_headers
            .iter()
            .any(|a| a == "*" || a.eq_ignore_ascii_case(h))
    });
    origin_ok && method_ok && headers_ok
}

fn origin_matches(allowed: &str, candidate: &str) -> bool {
    if allowed == "*" {
        return true;
    }
    if !allowed.contains('*') {
        return allowed == candidate;
    }
    // Wildcard support: one '*' substituted by any sequence of non-slash chars.
    let parts: Vec<&str> = allowed.splitn(2, '*').collect();
    if parts.len() != 2 {
        return false;
    }
    let (head, tail) = (parts[0], parts[1]);
    candidate.starts_with(head)
        && candidate.ends_with(tail)
        && candidate.len() >= head.len() + tail.len()
}

fn echo_origin<'a>(rule: &CorsRule, origin: &'a str) -> &'a str {
    if rule.allowed_origins.iter().any(|o| o == "*") && !rule.allowed_origins.iter().any(|o| o == origin) {
        // Wildcard rule, echo "*"
        "*"
    } else {
        origin
    }
}

fn set_header(h: &mut HeaderMap, name: &str, value: &str) {
    if let (Ok(n), Ok(v)) = (HeaderName::try_from(name), HeaderValue::from_str(value)) {
        h.insert(n, v);
    }
}
