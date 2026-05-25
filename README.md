# s3lite

A lightweight, single-binary S3-compatible object storage server written in Rust.

`s3lite` lets you drop a tiny self-hosted S3 endpoint into a project so that
application code can use the AWS S3 SDK in dev, CI, and self-hosted production
**without an `if dev { local_fs } else { s3 }` branch**. The only client-side
change is setting `AWS_ENDPOINT_URL_S3` to point at s3lite.

- Single binary, single node, single root credential
- Pure Rust: `axum` + `redb` + `blake3`, no JVM, no Go runtime
- Content-addressed parts (BLAKE3) + immutable blob store with redb metadata
- Crash-safe writes, race-safe GC, atomic multipart completion
- Versioning, Object Lock (Compliance), Lifecycle, CORS, Tagging, Webhook notifications
- TLS termination (rustls), Sigv4 header + presigned, full SDK-grade error codes

## Status / Scope

`s3lite` is for **single-node, self-hosted, single-tenant** workloads where you
want S3 compatibility without operating MinIO or paying for cloud S3. It is
not a replacement for a multi-node object store.

**In scope:** every common S3 operation an SDK or `aws-cli` would invoke against
a single account.

**Out of scope (returns `501 NotImplemented`):** ACLs, bucket policies, SSE-*,
multi-tenancy/IAM, replication, S3 Select, static website hosting,
`BucketNotificationConfiguration` (event notifications are configured via
`config.toml`, not the API).

## Quickstart

### Native

```bash
cargo build --release
./target/release/s3lite init --data-dir ./data
./target/release/s3lite serve --config ./data/config.toml
```

`init` prints the freshly generated `access_key_id` / `secret_access_key`
exactly once — copy them somewhere safe.

### Docker

Multi-arch image published on every push to `main` and on `v*` tags
(linux/amd64 + linux/arm64, distroless base, ~20 MB).

```bash
docker pull ghcr.io/jbj338033/s3lite:latest

# One-shot: random credentials printed to stdout once, then serve.
docker run --rm -p 9000:9000 -v s3data:/data ghcr.io/jbj338033/s3lite:latest

# Or with explicit credentials.
docker run --rm -p 9000:9000 -v s3data:/data \
  -e S3LITE_ACCESS_KEY_ID=AKIA... \
  -e S3LITE_SECRET_ACCESS_KEY='...' \
  ghcr.io/jbj338033/s3lite:latest
```

### Docker Compose

```yaml
services:
  s3:
    image: ghcr.io/jbj338033/s3lite:latest
    volumes: [s3data:/data]
    ports: ["9000:9000"]
    environment:
      S3LITE_ACCESS_KEY_ID: dev-key-dev-key-dev-key-dev-key-dev
      S3LITE_SECRET_ACCESS_KEY: dev-secret-dev-secret-dev-secret-dev-secr
  app:
    build: .
    depends_on: [s3]
    environment:
      AWS_ENDPOINT_URL_S3: http://s3:9000
      AWS_REGION: us-east-1
      AWS_ACCESS_KEY_ID: dev-key-dev-key-dev-key-dev-key-dev
      AWS_SECRET_ACCESS_KEY: dev-secret-dev-secret-dev-secret-dev-secr
volumes:
  s3data: {}
```

Recognized container env vars: `S3LITE_DATA_DIR` (default `/data`),
`S3LITE_LISTEN_ADDR` (default `0.0.0.0:9000`), `S3LITE_REGION` (default
`us-east-1`), `S3LITE_ACCESS_KEY_ID` + `S3LITE_SECRET_ACCESS_KEY` (both or
neither), `S3LITE_ENDPOINT_HOST`, `S3LITE_TLS_CERT_PATH` +
`S3LITE_TLS_KEY_PATH` (both or neither).

The container runs as UID 65532. If you bind-mount a host directory at
`/data`, make sure it's owned by that UID:
`chown 65532:65532 ./data && chmod 700 ./data`. Docker-managed named
volumes (as shown above) handle this automatically.

### Smoke test with aws-cli

```bash
export AWS_ENDPOINT_URL_S3=http://127.0.0.1:9000
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=<from init>
export AWS_SECRET_ACCESS_KEY=<from init>

aws s3 mb s3://demo
echo hello > /tmp/hi.txt
aws s3 cp /tmp/hi.txt s3://demo/hi.txt
aws s3 ls s3://demo/
aws s3 cp s3://demo/hi.txt -
```

## Configuration

`config.toml`:

```toml
region = "us-east-1"
listen_addr = "127.0.0.1:9000"
data_dir = "/var/lib/s3lite"
access_key_id = "AKIA..."
secret_access_key = "..."

# Optional virtual-hosted addressing
# endpoint_host = "s3.example.com"

# Optional TLS (both keys required together)
# tls_cert_path = "/etc/s3lite/cert.pem"
# tls_key_path  = "/etc/s3lite/key.pem"

# Optional cap on signed-body buffering (default 64 MiB)
# max_signed_body_bytes = 67108864

# Webhook subscriptions for object events
# [[webhook]]
# url = "https://example.com/hook"
# bucket = "my-bucket"          # empty = any bucket
# events = ["s3:ObjectCreated:Put"]   # empty = all events
# prefix = "uploads/"
# suffix = ".jpg"
```

The data directory contains `meta.redb` (metadata) and `parts/` (immutable
content-addressed blobs). Permissions are set to `0700` / `0600` at init.

## Operations

- **Hot reload** — send `SIGHUP` to reload `config.toml` (rotates root key,
  webhook subscriptions, etc.) and the TLS cert/key. In-flight requests
  finish on the snapshot they entered with; new requests see the new config.
- **Backup / restore** —
  ```bash
  s3lite backup  --data-dir /var/lib/s3lite --output /backups/2026-05-25
  s3lite restore --snapshot /backups/2026-05-25 --data-dir /var/lib/s3lite-new
  ```
  Stop the server first; both commands need exclusive access to `meta.redb`.
- **Integrity scan** — `s3lite scan-rebuild --data-dir /var/lib/s3lite`
  re-hashes every part and reports corruption. Last-resort recovery only.
- **Endpoints** —
  - `GET /health` → `200 ok` (unauthenticated)
  - `GET /metrics` → Prometheus text exposition (bucket count, DLQ depth,
    build info; unauthenticated)

## SDK / client setup

Any AWS S3 SDK works — set `AWS_ENDPOINT_URL_S3` and use the credentials from
`init`. Path-style and virtual-hosted addressing both work; SDKs default to
virtual-hosted, but path-style is convenient in dev when wildcard DNS isn't
available. AWS CLI:

```bash
aws s3api list-buckets --endpoint-url http://127.0.0.1:9000
```

Presigned URLs are the only way to share access — there is no anonymous
public-read mode by design.

## License

Apache-2.0.
