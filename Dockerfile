# syntax=docker/dockerfile:1
# shapeshift OSS CLI image.
#
# The lean, default build (Snappy-only, C-free, no cloud SDKs) as a single static
# musl binary on `scratch`: no libc, no shell, no package manager, no runtime deps
# — the container form of the README's "one relocatable binary" promise.
#
# Multi-arch (linux/amd64, linux/arm64): buildx emulates each target platform, so
# the musl build stage is always NATIVE to its arch — no cross-linker is needed
# because the default feature set is pure Rust and links rustup's self-contained
# musl. (The opt-in `object_store` / `zstd` fat builds link C and are NOT in this
# image; build those from source.)
#
# Build context is the crate root (the directory that holds Cargo.toml and the
# shapeshift-* crates):
#
#   docker build -t shapeshift .
#   docker run --rm -v "$PWD:/data" shapeshift shape -i /data/events.jsonl -o /data/out.parquet

# ---- builder -----------------------------------------------------------------
# rust:1-bookworm = latest stable 1.x (>= 1.85: a transitive dep needs the
# edition2024 cargo feature). Under buildx emulation `uname -m` is the target arch.
FROM rust:1-bookworm AS builder
WORKDIR /src
COPY . .
RUN set -eux; \
    arch="$(uname -m)"; \
    target="${arch}-unknown-linux-musl"; \
    rustup target add "$target"; \
    cargo build -p shapeshift-cli --release --locked --target "$target"; \
    cp "target/${target}/release/shapeshift" /shapeshift

# ---- runtime (scratch: the static binary and nothing else) -------------------
FROM scratch
COPY --from=builder /shapeshift /shapeshift
WORKDIR /data
VOLUME ["/data"]
ENTRYPOINT ["/shapeshift"]
