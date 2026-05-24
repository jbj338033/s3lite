//! End-to-end Phase 3 verification: drive a live s3lite server with the
//! official `aws-sdk-s3` client. Each test spins up the server on an
//! ephemeral TCP port and runs the SDK against `http://127.0.0.1:<port>`.

use std::net::SocketAddr;
use std::sync::Arc;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use s3lite::config::ServerConfig;
use s3lite::http::build_app;
use s3lite::s3::AppState;
use s3lite::storage::{MetaStore, PartStore};
use tempfile::TempDir;
use tokio::net::TcpListener;

const REGION: &str = "us-east-1";
const AK: &str = "AKIAIOSFODNN7EXAMPLE";
const SK: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

struct Harness {
    _dir: TempDir,
    _server: tokio::task::JoinHandle<()>,
    client: Client,
}

async fn start_server() -> Harness {
    let dir = TempDir::new().unwrap();
    let meta = Arc::new(
        MetaStore::open(dir.path().join("meta.redb"))
            .await
            .unwrap(),
    );
    let parts = Arc::new(PartStore::open(dir.path()).await.unwrap());
    let config = ServerConfig::new(
        REGION,
        AK,
        SK,
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
    );
    let state = AppState::new(meta, parts, config);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = build_app(state);
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let endpoint = format!("http://{addr}");
    let creds = Credentials::new(AK, SK, None, None, "test");
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(REGION))
        .credentials_provider(creds)
        .endpoint_url(endpoint)
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .force_path_style(true)
        .build();
    let client = Client::from_conf(s3_config);

    Harness {
        _dir: dir,
        _server: server,
        client,
    }
}

#[tokio::test]
async fn full_object_lifecycle() {
    let h = start_server().await;
    let bucket = "test-bucket";
    let key = "hello.txt";
    let payload = b"hello, s3lite!".to_vec();

    // ListBuckets — should be empty
    let listed = h.client.list_buckets().send().await.unwrap();
    assert!(listed.buckets().is_empty(), "fresh server should have no buckets");

    // CreateBucket
    h.client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("create_bucket");

    // ListBuckets — should contain the new bucket
    let listed = h.client.list_buckets().send().await.unwrap();
    let names: Vec<&str> = listed
        .buckets()
        .iter()
        .filter_map(|b| b.name())
        .collect();
    assert_eq!(names, vec![bucket]);

    // HeadBucket
    h.client.head_bucket().bucket(bucket).send().await.unwrap();

    // PutObject
    let put = h
        .client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(payload.clone()))
        .send()
        .await
        .expect("put_object");
    assert!(put.e_tag().is_some());

    // HeadObject
    let head = h
        .client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("head_object");
    assert_eq!(head.content_length(), Some(payload.len() as i64));

    // GetObject — full body
    let got = h
        .client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("get_object");
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), payload.as_slice());

    // GetObject — range
    let got = h
        .client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range("bytes=0-4")
        .send()
        .await
        .expect("get_object range");
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), &payload[0..=4]);

    // DeleteObject
    h.client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();

    // GetObject after delete — NoSuchKey
    let err = h
        .client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect_err("get_object should fail after delete");
    let msg = format!("{err:?}");
    assert!(msg.contains("NoSuchKey"), "expected NoSuchKey, got {msg}");

    // DeleteBucket
    h.client
        .delete_bucket()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn duplicate_create_bucket_rejected() {
    let h = start_server().await;
    let bucket = "duplicated";
    h.client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    let err = h
        .client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect_err("duplicate create_bucket should fail");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("BucketAlreadyOwnedByYou"),
        "expected BucketAlreadyOwnedByYou, got {msg}"
    );
}

#[tokio::test]
async fn delete_non_empty_bucket_rejected() {
    let h = start_server().await;
    let bucket = "nonempty";
    h.client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"x".to_vec()))
        .send()
        .await
        .unwrap();
    let err = h
        .client
        .delete_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect_err("non-empty delete should fail");
    let msg = format!("{err:?}");
    assert!(msg.contains("BucketNotEmpty"), "expected BucketNotEmpty, got {msg}");
}

#[tokio::test]
async fn etag_matches_md5_for_single_put() {
    let h = start_server().await;
    let bucket = "etagchk";
    let key = "k";
    let payload = b"the quick brown fox".to_vec();

    h.client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    let put = h
        .client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(payload.clone()))
        .send()
        .await
        .unwrap();

    use md5::Digest;
    let mut h_md5 = md5::Md5::new();
    h_md5.update(&payload);
    let digest: [u8; 16] = h_md5.finalize().into();
    let expected_etag = format!("\"{}\"", hex::encode(digest));
    assert_eq!(put.e_tag().unwrap(), expected_etag);
}

#[tokio::test]
async fn put_with_bad_content_md5_returns_bad_digest() {
    let h = start_server().await;
    let bucket = "md5chk";
    h.client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .unwrap();

    // Build a deliberately mismatched Content-MD5
    let payload = b"actual content".to_vec();
    use md5::Digest;
    let mut h_md5 = md5::Md5::new();
    h_md5.update(b"different content");
    let bad_digest: [u8; 16] = h_md5.finalize().into();
    use base64::Engine as _;
    let bad_b64 = base64::engine::general_purpose::STANDARD.encode(bad_digest);

    let err = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .content_md5(bad_b64)
        .body(ByteStream::from(payload))
        .send()
        .await
        .expect_err("PutObject with mismatched Content-MD5 should fail");
    let msg = format!("{err:?}");
    assert!(msg.contains("BadDigest"), "expected BadDigest, got {msg}");
}

#[tokio::test]
async fn if_none_match_star_blocks_overwrite() {
    let h = start_server().await;
    let bucket = "cond";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"v1".to_vec()))
        .send()
        .await
        .unwrap();

    // If-None-Match: * on GET means "give me the object unless it exists"
    // → for existing object, returns 304.
    let res = h
        .client
        .get_object()
        .bucket(bucket)
        .key("k")
        .if_none_match("*")
        .send()
        .await;
    // SDK maps 304 to a NotModified error variant.
    let err = res.expect_err("expected NotModified");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("NotModified") || msg.contains("304"),
        "expected NotModified, got {msg}"
    );
}

// ---------------- Phase 4: additional checksums ----------------

#[tokio::test]
async fn put_with_sha256_checksum_stored_and_echoed() {
    use base64::Engine as _;
    use sha2::Digest;

    let h = start_server().await;
    let bucket = "sha256-bucket";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();

    let payload = b"sha256 checksum body".to_vec();
    let mut hasher = sha2::Sha256::new();
    hasher.update(&payload);
    let digest = hasher.finalize();
    let b64 = base64::engine::general_purpose::STANDARD.encode(digest);

    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .checksum_sha256(b64.clone())
        .body(ByteStream::from(payload.clone()))
        .send()
        .await
        .expect("put_object with sha256 checksum");

    let got = h
        .client
        .get_object()
        .bucket(bucket)
        .key("k")
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await
        .expect("get_object with checksum mode");
    assert_eq!(got.checksum_sha256().unwrap(), b64);
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), payload.as_slice());
}

#[tokio::test]
async fn put_with_mismatched_sha256_returns_bad_digest() {
    use base64::Engine as _;
    let h = start_server().await;
    let bucket = "bad-sha256";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();

    let bad_b64 = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
    let err = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .checksum_sha256(bad_b64)
        .body(ByteStream::from(b"actual body".to_vec()))
        .send()
        .await
        .expect_err("mismatched sha256 should fail");
    let msg = format!("{err:?}");
    assert!(msg.contains("BadDigest"), "expected BadDigest, got {msg}");
}

#[tokio::test]
async fn put_with_crc32_checksum_succeeds() {
    use base64::Engine as _;
    let h = start_server().await;
    let bucket = "crc32-bucket";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();

    let payload = b"crc32 body content".to_vec();
    let digest = crc32fast::hash(&payload).to_be_bytes();
    let b64 = base64::engine::general_purpose::STANDARD.encode(digest);

    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .checksum_crc32(b64.clone())
        .body(ByteStream::from(payload))
        .send()
        .await
        .expect("put_object with crc32 checksum");

    let got = h
        .client
        .get_object()
        .bucket(bucket)
        .key("k")
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(got.checksum_crc32().unwrap(), b64);
}

#[tokio::test]
async fn put_with_sha1_checksum_succeeds() {
    use base64::Engine as _;
    use sha1::Digest;
    let h = start_server().await;
    let bucket = "sha1-bucket";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();

    let payload = b"sha1 content".to_vec();
    let mut hasher = sha1::Sha1::new();
    hasher.update(&payload);
    let b64 = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());

    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .checksum_sha1(b64.clone())
        .body(ByteStream::from(payload))
        .send()
        .await
        .expect("put_object with sha1 checksum");
}

// ---------------- Phase 4: conditional PUT ----------------

#[tokio::test]
async fn put_if_none_match_star_blocks_overwrite() {
    let h = start_server().await;
    let bucket = "cond-put";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"first".to_vec()))
        .send()
        .await
        .unwrap();

    let err = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .if_none_match("*")
        .body(ByteStream::from(b"second".to_vec()))
        .send()
        .await
        .expect_err("If-None-Match: * should block overwrite");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("PreconditionFailed") || msg.contains("412"),
        "expected PreconditionFailed, got {msg}"
    );

    // original content unchanged
    let got = h
        .client
        .get_object()
        .bucket(bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"first");
}

#[tokio::test]
async fn put_if_none_match_star_allows_create() {
    let h = start_server().await;
    let bucket = "cond-create";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    h.client
        .put_object()
        .bucket(bucket)
        .key("new-key")
        .if_none_match("*")
        .body(ByteStream::from(b"created".to_vec()))
        .send()
        .await
        .expect("If-None-Match: * should allow create when absent");
}

#[tokio::test]
async fn put_if_match_with_wrong_etag_returns_412() {
    let h = start_server().await;
    let bucket = "cond-cas";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"v1".to_vec()))
        .send()
        .await
        .unwrap();

    let err = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .if_match("\"deadbeef\"")
        .body(ByteStream::from(b"v2".to_vec()))
        .send()
        .await
        .expect_err("If-Match with wrong ETag should fail");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("PreconditionFailed") || msg.contains("412"),
        "expected PreconditionFailed, got {msg}"
    );
}

#[tokio::test]
async fn put_if_match_with_correct_etag_replaces() {
    let h = start_server().await;
    let bucket = "cond-cas-ok";
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    let put1 = h
        .client
        .put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from(b"v1".to_vec()))
        .send()
        .await
        .unwrap();
    let etag = put1.e_tag().unwrap().to_string();

    h.client
        .put_object()
        .bucket(bucket)
        .key("k")
        .if_match(etag)
        .body(ByteStream::from(b"v2".to_vec()))
        .send()
        .await
        .expect("If-Match with correct ETag should replace");

    let got = h
        .client
        .get_object()
        .bucket(bucket)
        .key("k")
        .send()
        .await
        .unwrap();
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"v2");
}

// ---------------- Phase 5: ListObjects ----------------

async fn seed_objects(h: &Harness, bucket: &str, keys: &[&str]) {
    h.client.create_bucket().bucket(bucket).send().await.unwrap();
    for k in keys {
        h.client
            .put_object()
            .bucket(bucket)
            .key(*k)
            .body(ByteStream::from(format!("body-{k}").into_bytes()))
            .send()
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn list_v2_returns_all_objects_lexicographic_order() {
    let h = start_server().await;
    let bucket = "list-basic";
    seed_objects(&h, bucket, &["b.txt", "a.txt", "c.txt"]).await;

    let resp = h
        .client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .unwrap();

    let keys: Vec<&str> = resp.contents().iter().filter_map(|c| c.key()).collect();
    assert_eq!(keys, vec!["a.txt", "b.txt", "c.txt"]);
    assert_eq!(resp.key_count(), Some(3));
    assert_eq!(resp.is_truncated(), Some(false));
}

#[tokio::test]
async fn list_v2_prefix_filter() {
    let h = start_server().await;
    let bucket = "list-prefix";
    seed_objects(
        &h,
        bucket,
        &["photos/a.jpg", "photos/b.jpg", "docs/r.pdf", "z.bin"],
    )
    .await;

    let resp = h
        .client
        .list_objects_v2()
        .bucket(bucket)
        .prefix("photos/")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = resp.contents().iter().filter_map(|c| c.key()).collect();
    assert_eq!(keys, vec!["photos/a.jpg", "photos/b.jpg"]);
}

#[tokio::test]
async fn list_v2_delimiter_produces_common_prefixes() {
    let h = start_server().await;
    let bucket = "list-delim";
    seed_objects(
        &h,
        bucket,
        &[
            "photos/2024/jan/01.jpg",
            "photos/2024/jan/02.jpg",
            "photos/2024/feb/01.jpg",
            "photos/2023/dec/31.jpg",
            "videos/v.mp4",
            "root.txt",
        ],
    )
    .await;

    let resp = h
        .client
        .list_objects_v2()
        .bucket(bucket)
        .delimiter("/")
        .send()
        .await
        .unwrap();
    let prefixes: Vec<&str> = resp
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    let keys: Vec<&str> = resp.contents().iter().filter_map(|c| c.key()).collect();
    assert_eq!(prefixes, vec!["photos/", "videos/"]);
    assert_eq!(keys, vec!["root.txt"]);
}

#[tokio::test]
async fn list_v2_prefix_with_delimiter_lists_subdirs() {
    let h = start_server().await;
    let bucket = "list-subdir";
    seed_objects(
        &h,
        bucket,
        &[
            "photos/2023/dec/31.jpg",
            "photos/2024/feb/01.jpg",
            "photos/2024/jan/01.jpg",
            "photos/index.html",
        ],
    )
    .await;

    let resp = h
        .client
        .list_objects_v2()
        .bucket(bucket)
        .prefix("photos/")
        .delimiter("/")
        .send()
        .await
        .unwrap();
    let prefixes: Vec<&str> = resp
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    let keys: Vec<&str> = resp.contents().iter().filter_map(|c| c.key()).collect();
    assert_eq!(prefixes, vec!["photos/2023/", "photos/2024/"]);
    assert_eq!(keys, vec!["photos/index.html"]);
}

#[tokio::test]
async fn list_v2_pagination_via_continuation_token() {
    let h = start_server().await;
    let bucket = "list-page";
    let all: Vec<String> = (0..5).map(|i| format!("k{i:02}")).collect();
    let refs: Vec<&str> = all.iter().map(String::as_str).collect();
    seed_objects(&h, bucket, &refs).await;

    let mut collected: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut req = h
            .client
            .list_objects_v2()
            .bucket(bucket)
            .max_keys(2);
        if let Some(t) = &token {
            req = req.continuation_token(t.clone());
        }
        let resp = req.send().await.unwrap();
        for c in resp.contents() {
            collected.push(c.key().unwrap().to_string());
        }
        if resp.is_truncated() != Some(true) {
            break;
        }
        token = resp.next_continuation_token().map(String::from);
        assert!(token.is_some(), "truncated response must include next-token");
    }
    assert_eq!(collected, all);
}

#[tokio::test]
async fn list_v2_start_after_skips_to_resume_point() {
    let h = start_server().await;
    let bucket = "list-start-after";
    seed_objects(&h, bucket, &["a", "b", "c", "d", "e"]).await;

    let resp = h
        .client
        .list_objects_v2()
        .bucket(bucket)
        .start_after("b")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = resp.contents().iter().filter_map(|c| c.key()).collect();
    assert_eq!(keys, vec!["c", "d", "e"]);
}

#[tokio::test]
async fn list_v2_encoding_type_url_encodes_keys_and_prefixes() {
    let h = start_server().await;
    let bucket = "list-enc";
    seed_objects(&h, bucket, &["pictures/cat dog.jpg"]).await;

    let resp = h
        .client
        .list_objects_v2()
        .bucket(bucket)
        .delimiter("/")
        .encoding_type(aws_sdk_s3::types::EncodingType::Url)
        .send()
        .await
        .unwrap();
    // The SDK passes through encoded values; "/" becomes "%2F".
    let prefixes: Vec<&str> = resp
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    assert_eq!(prefixes, vec!["pictures%2F"]);
    assert_eq!(resp.encoding_type(), Some(&aws_sdk_s3::types::EncodingType::Url));
}

#[tokio::test]
async fn list_v1_marker_pagination() {
    let h = start_server().await;
    let bucket = "list-v1";
    seed_objects(&h, bucket, &["a", "b", "c", "d"]).await;

    let first = h
        .client
        .list_objects()
        .bucket(bucket)
        .max_keys(2)
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = first.contents().iter().filter_map(|c| c.key()).collect();
    assert_eq!(keys, vec!["a", "b"]);
    assert_eq!(first.is_truncated(), Some(true));

    let second = h
        .client
        .list_objects()
        .bucket(bucket)
        .marker("b")
        .max_keys(2)
        .send()
        .await
        .unwrap();
    let keys2: Vec<&str> = second.contents().iter().filter_map(|c| c.key()).collect();
    assert_eq!(keys2, vec!["c", "d"]);
}

#[tokio::test]
async fn list_v2_etag_and_size_in_contents() {
    let h = start_server().await;
    let bucket = "list-meta";
    seed_objects(&h, bucket, &["hello"]).await;

    let resp = h
        .client
        .list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    let entry = resp.contents().first().unwrap();
    assert_eq!(entry.key(), Some("hello"));
    assert!(entry.e_tag().is_some());
    // "body-hello" is 10 bytes
    assert_eq!(entry.size(), Some(10));
    assert_eq!(
        entry.storage_class(),
        Some(&aws_sdk_s3::types::ObjectStorageClass::Standard)
    );
}
