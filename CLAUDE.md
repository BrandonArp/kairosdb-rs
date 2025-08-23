# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Essential Commands

KairosDB-rs uses [cargo-make](https://github.com/sagiegurari/cargo-make) for task automation, providing npm-like script functionality for Rust projects.

### Quick Setup
```bash
# Install cargo-make (one-time setup)
cargo install cargo-make

# Or use our setup task
cargo make dev-setup
```

### Primary Commands (CI Equivalent)
```bash
# 🚀 Check if your build will pass CI (most important command)
cargo make ci

# 🔄 Run before committing changes
cargo make pre-commit

# 🌐 Full CI pipeline with integration tests (requires Tilt)
cargo make ci-full

# 📋 See all available commands
cargo make help
```

**Note**: The Jenkins CI pipeline uses the same `cargo make` commands, ensuring complete consistency between local development and CI environments.

### Individual Tasks
```bash
# Building
cargo make build              # Build all workspace members
cargo make build-release      # Release build with optimizations

# Testing
cargo make test              # Unit tests only
cargo make test-integration  # Integration tests (requires Tilt)
cargo make test-all          # All tests
cargo make coverage-report   # Generate coverage report

# Code Quality
cargo make format            # Format code
cargo make format-check      # Check formatting
cargo make lint              # Run clippy lints
cargo make check             # Run format, lint, and build checks

# Services
cargo make run-ingest        # Run ingest service
cargo make run-query         # Run query service
cargo make run-datastore-demo # Run datastore demo

# Benchmarks
cargo make bench             # Run all benchmarks
cargo make bench-ingestion   # Run ingestion benchmarks

# Performance Testing
cargo make perf-test         # Run complete performance test suite
cargo make perf-test-small   # Run quick performance validation (30s)
cargo make perf-test-medium  # Run CI-appropriate test (2m)
cargo make perf-test-large   # Run load testing scenario (5m)
cargo make perf-test-stress  # Run stress test to find limits (10m)
cargo make perf-monitor      # Start continuous monitoring
```

### Legacy Commands (still supported)
```bash
# Direct cargo commands (bypass task runner)
cargo build --bin kairosdb-ingest
cargo build --bin kairosdb-query
cargo test --lib --workspace
cargo nextest run --profile ci --workspace --lib
cargo clippy --all-targets --all-features -- -D warnings
```

### Development with Tilt
```bash
# Start development environment with hot reload
tilt up

# View Tilt dashboard
tilt up --web-mode=local
```

## Architecture Overview

KairosDB-rs is a high-performance Rust rewrite of KairosDB with microservices architecture:

### Workspace Structure
- **kairosdb-core**: Shared library containing common data types, Cassandra integration, error handling, and utilities
- **kairosdb-ingest**: HTTP ingestion service that receives time series data and writes to Cassandra  
- **kairosdb-query**: Query service for retrieving and aggregating time series data

### Key Design Patterns

**Cassandra Integration**: Core library provides unified Cassandra client with connection pooling, prepared statements, and async operations. Services use trait-based abstractions for testability.

**Configuration**: Environment variable-based configuration with YAML file support for development. All services share common Cassandra configuration patterns.

**Error Handling**: Unified error types in `kairosdb-core::error` with proper HTTP status code mappings and structured error responses.

**Data Validation**: Comprehensive input validation with configurable limits for metric names, tag counts, batch sizes, and data types.

**Async Processing**: Tokio-based async runtime with batched processing, backpressure handling, and graceful shutdown support.

## Service Configuration

### Environment Variables (Common)
- `KAIROSDB_CASSANDRA_CONTACT_POINTS`: Cassandra hosts (default: localhost:9042)
- `KAIROSDB_CASSANDRA_KEYSPACE`: Keyspace name (default: kairosdb)
- `KAIROSDB_CASSANDRA_USERNAME`: Auth username
- `KAIROSDB_CASSANDRA_PASSWORD`: Auth password

### Ingest Service Specific
- `KAIROSDB_BIND_ADDRESS`: HTTP bind address (default: 0.0.0.0:8080)
- `KAIROSDB_MAX_BATCH_SIZE`: Max data points per batch (default: 10000)
- `KAIROSDB_WORKER_THREADS`: Processing threads (default: 4)
- `KAIROSDB_MAX_MEMORY_MB`: Memory limit for backpressure (default: 1024)

### Query Service Specific  
- `KAIROSDB_QUERY_BIND_ADDRESS`: HTTP bind address (default: 0.0.0.0:8081)
- `KAIROSDB_QUERY_TIMEOUT_MS`: Query timeout (default: 30000)
- `KAIROSDB_CACHE_ENABLE`: Enable result caching (default: true)

## API Compatibility

This implementation maintains 100% API compatibility with Java KairosDB plus histogram extensions:

**Ingest Endpoints**:
- `POST /api/v1/datapoints` - Main ingestion endpoint
- `POST /api/v1/datapoints/gzip` - Compressed ingestion
- `GET /health`, `/health/ready`, `/health/live` - Health checks
- `GET /metrics` - Prometheus metrics

**Data Format**: Supports all KairosDB JSON formats including single metrics, metric arrays, all data types (long, double, text, complex, binary, histogram), and TTL values.

### Histogram Support

KairosDB-rs includes comprehensive histogram datapoint support, addressing a key functionality gap:

**Histogram Data Types**:
- Pre-aggregated histogram metrics with bucket counts
- Statistical metadata (sum, count, min/max values)
- Percentile calculation and aggregation support

**Ingestion Formats**:
1. **Direct Format**: `{"boundaries": [0, 10, 50], "counts": [5, 8, 2], "total_count": 15, "sum": 340.5}`
2. **Prometheus Format**: `{"buckets": [{"le": 10, "count": 5}, {"le": 50, "count": 13}], "count": 15, "sum": 340.5}`

**Storage**: Histograms are efficiently serialized as binary data in Cassandra with the `kairos_histogram` data type.

## Development Workflow

### Local Development
1. Start Cassandra: `docker run -d --name cassandra -p 9042:9042 cassandra:3.11`
2. Wait for Cassandra to be ready
3. Run service: `cargo run --bin kairosdb-ingest`
4. Test with: `curl -X POST http://localhost:8080/api/v1/datapoints -H "Content-Type: application/json" -d '[{"name":"test.metric","datapoints":[[1640995200000,42]],"tags":{"host":"test"}}]'`

### Tilt Development
Use Tilt for integrated development with Kubernetes:
- Cassandra, Java KairosDB, and Rust services
- Hot reload for Rust changes  
- Port forwarding: Cassandra (9042), Java KairosDB (8080), Rust Ingest (8081)

### Testing Strategy
- Unit tests focus on core logic and data structures
- Integration tests use real Cassandra for HTTP API testing
- `scripts/test-all.sh` provides comprehensive test suite with Docker setup
- Performance benchmarks available with `cargo bench`

## Code Patterns to Follow

### Error Handling
Use `KairosResult<T>` and `KairosError` from core library. Convert external errors appropriately and include context.

### Async Patterns
Prefer async/await with proper error propagation. Use structured concurrency with tokio::spawn for independent tasks.

### Configuration
Use builder pattern for configuration with environment variable overrides. Validate configuration at startup.

### Cassandra Operations  
Use the shared Cassandra client from core library. Batch operations when possible and handle connection failures gracefully.

### Histogram Usage
```rust
// Create histogram datapoints
let hist = HistogramBuilder::new()
    .boundaries(vec![0.1, 1.0, 5.0, 10.0])
    .counts(vec![10, 50, 25, 5])
    .total_count(90)
    .sum(195.5)
    .build()?;

let dp = DataPoint::new_histogram("response_time", Timestamp::now(), hist);

// Calculate percentiles
if let DataPointValue::Histogram(h) = &dp.value {
    let p95 = h.percentile(0.95); // 95th percentile
    let mean = h.mean();          // Average value
}

// Merge histograms
hist1.merge(&hist2)?;
```

### JSON Ingestion Examples
```json
// Direct format
{
    "name": "http_request_duration",
    "datapoints": [[1634567890000, {
        "boundaries": [0.1, 0.5, 1.0, 5.0],
        "counts": [100, 50, 30, 5],
        "total_count": 185,
        "sum": 89.5,
        "min": 0.01,
        "max": 4.8
    }]],
    "tags": {"service": "api"}
}

// Prometheus format  
{
    "name": "response_latency",
    "datapoints": [[1634567890000, {
        "buckets": [
            {"le": 0.1, "count": 100},
            {"le": 0.5, "count": 150}, 
            {"le": 1.0, "count": 180},
            {"le": 5.0, "count": 185}
        ],
        "count": 185,
        "sum": 89.5
    }]],
    "tags": {"endpoint": "/users"}
}
```

### Monitoring
Expose Prometheus metrics for all operations. Include counters, histograms for timing, and gauge metrics for system state.

## Performance Testing Framework

KairosDB-rs includes a comprehensive end-to-end performance testing framework designed for histogram-heavy workloads and high-cardinality scenarios.

### Quick Start
```bash
# Run quick validation test (30 seconds)
cargo make perf-test-small

# Run full performance test suite 
cargo make perf-test

# Run custom scenarios
cd tests && cargo run --bin perf_test -- run large_scale --duration 600
```

### Test Scenarios
- **small_scale**: 50 metrics, 30s (local development)
- **medium_scale**: 500 metrics, 2m (CI/staging) 
- **large_scale**: 2k metrics, 5m (production load)
- **stress_test**: 5k metrics, 10m (find breaking points)
- **high_cardinality**: 100 metrics with 1000 tag combinations each
- **high_frequency**: Many small batches for latency testing
- **large_batch**: Fewer large batches for throughput testing  
- **memory_pressure**: Large histograms (1k-5k samples each)

### Key Features
- **Histogram-Focused**: Generates realistic histogram data with configurable sample counts (10s to thousands)
- **High Cardinality**: Thousands of metrics with consistent tag patterns (service, environment, region, etc.)
- **Realistic Distributions**: Normal, exponential, and bimodal sample distributions
- **Comprehensive Reporting**: Latency stats (P95, P99), throughput, success rates, bottleneck analysis
- **Trending Data**: CSV output for tracking performance over time
- **Continuous Monitoring**: Long-running tests for stability validation

### Performance Metrics Collected
- Throughput: datapoints/second, requests/second
- Latency: mean, median, P95, P99, min, max
- Success rate and error analysis
- Efficiency scoring and bottleneck identification
- Memory and resource utilization estimates

### Logging Configuration for Performance Testing
Services use appropriate logging levels optimized for development and performance:
```bash
# Development default (INFO level - good balance)
cargo make run-ingest

# Performance testing (minimal logging)
RUST_LOG=warn cargo make perf-test-small

# Development debugging (detailed but not per-operation)
RUST_LOG=debug cargo make perf-test-small

# Maximum verbosity (per-datapoint logging - severely impacts performance)
RUST_LOG=trace cargo make perf-test-small
```

**Logging Level Guidelines:**
- **INFO**: Service startup, batch summaries, important events
- **DEBUG**: Development debugging, method entry/exit, validation steps  
- **TRACE**: Per-datapoint operations, per-request details, detailed timing
- **WARN/ERROR**: Performance testing and production

⚠️ **Performance Impact**: TRACE logging can reduce throughput by 10x+ due to per-datapoint output.

### CLI Usage Examples
```bash
# List all available scenarios
cd tests && cargo run --bin perf_test -- list

# Run with custom parameters
cd tests && cargo run --bin perf_test -- run medium_scale \
  --metrics 1000 --duration 300 --batch-size 200

# Run test suite excluding stress tests
cd tests && cargo run --bin perf_test -- suite --skip stress_test

# Start continuous monitoring (every 5 minutes)
cd tests && cargo run --bin perf_test -- monitor \
  --interval 300 --scenario medium_scale

# Generate sample config file
cd tests && cargo run --bin perf_test -- config
```

### Integration with CI/CD
Performance tests are designed to integrate with CI pipelines:
- **perf-test-small**: Quick validation suitable for PR checks
- **perf-test-medium**: More comprehensive testing for staging deployments
- Reports saved as JSON and CSV for trending analysis
- Configurable failure thresholds and performance regression detection