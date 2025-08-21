# Multi-target Dockerfile for KairosDB Services

FROM rust:1.89-slim AS builder

# Install system dependencies
RUN <<EOF
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    ca-certificates
apt-get clean
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
EOF

WORKDIR /app

# Copy workspace files for better layer caching
COPY Cargo.toml Cargo.lock ./
COPY kairosdb-core ./kairosdb-core
COPY kairosdb-ingest ./kairosdb-ingest
COPY kairosdb-query ./kairosdb-query
COPY tests ./tests

# Build both applications and strip the binaries
RUN <<EOF
cargo build --release --package kairosdb-ingest --bin kairosdb-ingest
cargo build --release --package kairosdb-query --bin kairosdb-query
strip target/release/kairosdb-ingest
strip target/release/kairosdb-query
EOF

# Runtime base stage
FROM debian:trixie-slim AS runtime-base

# Install runtime dependencies and create user in single layer
RUN <<EOF
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    tini
apt-get clean
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
useradd -r -s /bin/false -u 1001 kairosdb
mkdir -p /app/config
chown -R kairosdb:kairosdb /app
EOF

# Switch to app user
USER kairosdb

# Set working directory
WORKDIR /app

# Ingest service target
FROM runtime-base AS ingest

# Copy binary from builder stage
COPY --from=builder --chown=kairosdb:kairosdb /app/target/release/kairosdb-ingest /usr/local/bin/kairosdb-ingest

# Expose port
EXPOSE 8080

# Add metadata labels
LABEL org.opencontainers.image.title="KairosDB Ingest Service" \
      org.opencontainers.image.description="High-performance Rust implementation of KairosDB ingestion service" \
      org.opencontainers.image.vendor="KairosDB" \
      org.opencontainers.image.licenses="Apache-2.0"

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Use tini as init system for proper signal handling
ENTRYPOINT ["/usr/bin/tini", "--"]

# Run the application
CMD ["kairosdb-ingest"]

# Query service target
FROM runtime-base AS query

# Copy binary from builder stage
COPY --from=builder --chown=kairosdb:kairosdb /app/target/release/kairosdb-query /usr/local/bin/kairosdb-query

# Expose port
EXPOSE 8082

# Add metadata labels
LABEL org.opencontainers.image.title="KairosDB Query Service" \
      org.opencontainers.image.description="High-performance Rust implementation of KairosDB query service" \
      org.opencontainers.image.vendor="KairosDB" \
      org.opencontainers.image.licenses="Apache-2.0"

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8082/health || exit 1

# Use tini as init system for proper signal handling
ENTRYPOINT ["/usr/bin/tini", "--"]

# Run the application
CMD ["kairosdb-query"]