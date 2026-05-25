# syntax=docker/dockerfile:1.7
# Multi-stage build: cross-compile a static musl binary, then drop it into
# a tiny distroless runtime. The result is ~10 MB total — one statically
# linked Rust binary plus the distroless layer.

# Builder runs on TARGETPLATFORM (Docker/QEMU emulates non-native arch).
# This means `musl-tools` and `rustup target add` pull the *host* arch's
# musl toolchain — no cross-compile setup needed; QEMU handles the rest.
FROM rust:1-bookworm AS builder
ARG TARGETARCH
RUN apt-get update \
    && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/*
RUN case "$TARGETARCH" in \
      amd64) TARGET=x86_64-unknown-linux-musl ;; \
      arm64) TARGET=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported TARGETARCH=$TARGETARCH"; exit 1 ;; \
    esac && rustup target add "$TARGET" && echo "$TARGET" > /tmp/target

WORKDIR /build
# Dependency cache layer: build a dummy main against the manifest so cargo
# resolves and compiles every dep once; source-only changes hit this cache.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo 'fn main(){}' > src/main.rs \
    && cargo build --release --target "$(cat /tmp/target)" --bin s3lite \
    && rm -rf src target/*/release/deps/s3lite-* target/*/release/s3lite

COPY src ./src
RUN cargo build --release --target "$(cat /tmp/target)" --bin s3lite \
    && cp "target/$(cat /tmp/target)/release/s3lite" /tmp/s3lite \
    && strip /tmp/s3lite || true

# Pre-create /data with nonroot ownership + 0700 so a Docker-managed named
# volume inherits these on first mount (and so auto_command doesn't need to
# chmod a non-owned VOLUME mountpoint).
RUN mkdir -p /rootfs/data && chown 65532:65532 /rootfs/data && chmod 700 /rootfs/data

# Distroless static base — ~2 MB, no shell, no libc, runs as UID 65532.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder --chown=nonroot:nonroot /tmp/s3lite /usr/local/bin/s3lite
COPY --from=builder --chown=nonroot:nonroot /rootfs/data /data

# /data is the persistent state — bind-mount a host dir owned by 65532 or
# let Docker manage a named volume.
VOLUME ["/data"]
EXPOSE 9000

ENV S3LITE_DATA_DIR=/data \
    S3LITE_LISTEN_ADDR=0.0.0.0:9000

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/s3lite", "auto"]
