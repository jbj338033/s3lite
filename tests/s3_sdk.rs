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
