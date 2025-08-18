# Multi-stage Dockerfile for KairosDB Ingestion Service

FROM rust:1.89-slim as builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY kairosdb-core ./kairosdb-core
COPY kairosdb-ingest ./kairosdb-ingest

# Build the application
RUN cargo build --release --bin kairosdb-ingest

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd -r -s /bin/false kairosdb

# Copy binary from builder stage
COPY --from=builder /app/target/release/kairosdb-ingest /usr/local/bin/

# Change ownership
RUN chown kairosdb:kairosdb /usr/local/bin/kairosdb-ingest

# Switch to app user
USER kairosdb

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Run the application
CMD ["kairosdb-ingest"]