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
# Automatically builds ingestion service with jemalloc for memory optimization
tilt up

# View Tilt dashboard
tilt up --web-mode=local
```

**Note**: The Tilt environment automatically builds the ingestion service with both `profiling` and `jemalloc` features enabled for optimal development experience with memory management improvements.

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
- `KAIROSDB_DEFAULT_SYNC`: Default sync-to-disk behavior (default: false)
- `KAIROSDB_HEALTH_LOAD_BALANCER_DETECTION_DELAY_SECONDS`: Load balancer detection delay (default: 3)

### Health Check & Graceful Shutdown
The ingest service supports configurable graceful shutdown behavior to work optimally with load balancers:

#### Load Balancer Detection Delay
```yaml
health:
  load_balancer_detection_delay_seconds: 3  # Default development
  # load_balancer_detection_delay_seconds: 5  # Recommended for production
```

**Purpose**: This delay allows load balancers to detect that the service has become unhealthy before starting to drain existing connections. This prevents new requests from being routed to a shutting-down instance.

**Shutdown Sequence**:
1. Service receives shutdown signal (SIGTERM, SIGINT)
2. Health checks immediately start failing (returns 503 status)
3. Wait `load_balancer_detection_delay_seconds` for load balancers to notice
4. Begin draining existing connections gracefully
5. Complete shutdown after all connections are closed

**Configuration Options**:
- **Development**: 3 seconds (faster local testing)
- **Production**: 5+ seconds (conservative for enterprise load balancers)
- **Environment Variable**: `KAIROSDB_HEALTH_LOAD_BALANCER_DETECTION_DELAY_SECONDS=10`

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

**Query Parameters**:
- `sync=true/false` - Force sync to disk before returning success (overrides default_sync config)
- `perf_mode=parse_only/parse_and_store/no_parse` - Performance testing modes

**Durability Control Examples**:
```bash
# Fast ingestion (default) - buffered writes for performance
curl -X POST http://localhost:8080/api/v1/datapoints -d '[{"name":"cpu.usage","datapoints":[[1640995200000,75.2]],"tags":{"host":"web1"}}]'

# Durable ingestion - guaranteed fsync to disk before 200 OK 
curl -X POST http://localhost:8080/api/v1/datapoints?sync=true -d '[{"name":"cpu.usage","datapoints":[[1640995200000,75.2]],"tags":{"host":"web1"}}]'

# Set default durability via environment variable
KAIROSDB_DEFAULT_SYNC=true ./kairosdb-ingest
```

**Configuration Options**:
```yaml
# config/development.yaml - Fast development mode
ingestion:
  default_sync: false  # Optimize for performance

# config/production.yaml - Durable production mode  
ingestion:
  default_sync: true   # Optimize for durability
```

**Implementation Details**:
- Uses Fjall's `PersistMode::SyncAll` for guaranteed durability when `sync=true`
- Buffered writes with periodic persistence for `sync=false` (performance mode)  
- HTTP 200 OK response only sent after successful disk persistence when sync enabled
- Configuration precedence: Query parameter > Environment variable > YAML config > Default

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

### Memory Management

KairosDB-rs supports jemalloc as an alternative memory allocator to address memory management issues:

**Using jemalloc**:
```bash
# Build with jemalloc for better memory management
cargo build --release --features jemalloc

# Run ingestion service with jemalloc
cargo run --bin kairosdb-ingest --features jemalloc
```

**Benefits**:
- Better memory reclamation to the operating system
- Reduced memory fragmentation under high load
- Improved performance in high-throughput scenarios
- More predictable memory usage patterns

**Configuration**:
- jemalloc is an optional feature, enabled with `--features jemalloc`
- Service logs indicate which allocator is in use at startup
- No configuration changes required - it's a drop-in replacement

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

KairosDB-rs now uses **OpenTelemetry** for comprehensive metrics collection, providing both real-time push capabilities and Prometheus scraping compatibility.

#### OpenTelemetry Metrics Setup

The service supports dual metrics export:
- **OTLP Export**: Real-time delta metrics pushed to OpenTelemetry collectors
- **Prometheus Export**: Traditional scraping endpoint for Prometheus compatibility

#### Key Metrics Available:
- **Queue Metrics**: Current size, enqueue/dequeue rates, processing latency
- **HTTP Metrics**: Request counts, response times, error rates by endpoint
- **Ingestion Metrics**: Datapoints/second, batch sizes, validation errors
- **Cassandra Metrics**: Write latencies, retry counts, error rates
- **Cache Metrics**: Hit/miss ratios, evictions, memory usage
- **System Metrics**: Memory usage, processing times, error counts

#### Configuration

Environment variables for OpenTelemetry:
```bash
# OTLP endpoint for real-time metrics (optional)
export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4317"

# Export interval for OTLP metrics (default: 10 seconds)
export OTEL_METRIC_EXPORT_INTERVAL="10"

# Use exponential histograms (default: true)
export OTEL_USE_EXPONENTIAL_HISTOGRAMS="true"

# Deployment environment tag
export DEPLOYMENT_ENV="production"
```

YAML configuration options:
```yaml
# OpenTelemetry configuration
opentelemetry:
  # OTLP endpoint for sending metrics (leave empty to disable OTLP export)
  otlp_endpoint: "http://otel-collector:4317"
  
  # Export interval in seconds
  export_interval_seconds: 10
  
  # Enable Prometheus exporter for scraping compatibility
  enable_prometheus_exporter: true
  
  # Use exponential histograms (better resolution, automatic buckets)
  use_exponential_histograms: true
  
  # Resource attributes
  service_name: "kairosdb-ingest"
  deployment_environment: "production"
```

#### Metrics Endpoints:
- `GET /metrics` - Prometheus format metrics (OpenTelemetry exported)
- `GET /health` - Health check with monitoring details

#### Observability Features:
- **Delta Temporality**: OTLP export sends delta/incremental values for counters and histograms
- **Exponential Histograms**: Automatic bucket management with better resolution than fixed buckets
- **Real-time Metrics**: OTLP export pushes metrics every 10-30 seconds
- **Prometheus Compatibility**: `/metrics` endpoint works with existing Prometheus scrapers
- **Rich Metadata**: Service name, version, environment tags included
- **HTTP Request Tracking**: Automatic instrumentation of all HTTP endpoints
- **Error Categorization**: 4xx vs 5xx errors, validation vs system errors

#### Delta vs Cumulative Metrics:
- **Counters** (e.g., requests_total, errors_total): Use **Delta** temporality - send increments only
- **Histograms** (e.g., request_duration, processing_time): Use **Delta** temporality - send bucket deltas
- **Gauges** (e.g., active_requests, queue_size): Use **Cumulative** temporality - send absolute values

This means OTLP receivers get only the incremental changes, not cumulative totals, enabling efficient real-time monitoring.

**Disk Usage Tracking**: Queue disk usage metrics are automatically updated after garbage collection sweeps to provide accurate storage consumption data for monitoring and capacity planning.

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
- **Queue Processing Analytics**: Comprehensive tracking of queue drain behavior with throughput estimates
- **Comprehensive Reporting**: Latency stats (P95, P99), ingestion throughput, queue processing metrics, bottleneck analysis
- **Trending Data**: CSV output for tracking performance over time
- **Continuous Monitoring**: Long-running tests for stability validation

### Performance Metrics Collected

**Ingestion Metrics:**
- Throughput: datapoints/second, requests/second
- Latency: mean, median, P95, P99, min, max
- Success rate and error analysis
- Efficiency scoring and bottleneck identification

**Queue Processing Metrics:**
- Queue size tracking (initial, peak, final)
- Items processed and processing time
- Processing rates (items/sec, batches/sec, datapoints/sec)
- Status check frequency and timeout behavior
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

## Performance Comparison Framework

KairosDB-rs includes a dedicated performance comparison tool to benchmark the Rust implementation against the Java reference implementation, both running via Tilt.

### Quick Start
```bash
# Check that both services are healthy and accessible
cargo make perf-compare-check

# Run quick comparison (30 seconds, suitable for development)
cargo make perf-compare-quick

# Run full comparison test suite across all scenarios
cargo make perf-compare
```

### Service Setup
The comparison tool expects both services running via Tilt:
- **Java KairosDB**: http://localhost:8080 (reference implementation)
- **Rust Ingest Service**: http://localhost:8081 (new implementation)

Start both services:
```bash
tilt up
# Wait for all services to be ready
cargo make perf-compare-check  # Verify health
```

### Comparison Modes
```bash
# Sequential testing (default - more stable, avoids resource contention)
cd tests && cargo run --bin perf_compare -- run small_scale

# Parallel testing (faster but may cause resource contention on same machine)
cd tests && cargo run --bin perf_compare -- run small_scale --parallel

# Custom scenarios with overrides
cd tests && cargo run --bin perf_compare -- run medium_scale --duration 180 --batch-size 50
```

### Available Scenarios
All performance test scenarios can be used for comparison:
- `small_scale`: Quick validation (50 metrics, 30s)
- `medium_scale`: Standard comparison (500 metrics, 2m)
- `large_scale`: Load testing (2k metrics, 5m)  
- `stress_test`: Breaking point analysis (5k metrics, 10m)
- `high_cardinality`: Tag explosion testing
- `high_frequency`: Many small batches
- `large_batch`: Fewer large batches
- `memory_pressure`: Large histogram testing

### Comparison Metrics
The tool analyzes and compares:
- **Throughput**: Datapoints per second, requests per second
- **Latency**: P95, P99, mean response times
- **Success Rate**: Percentage of successful requests
- **Error Analysis**: Detailed failure breakdown
- **Winner Determination**: Based on throughput and success rates

### Reporting
Results are saved in multiple formats:
- **Console Output**: Real-time comparison summary
- **JSON Reports**: `target/perf_compare_reports/comparison_<scenario>_detailed.json`
- **CSV Trending**: `target/perf_compare_reports/comparison_trends.csv`

### CLI Usage Examples
```bash
# List available scenarios
cd tests && cargo run --bin perf_compare -- list

# Run specific scenario
cd tests && cargo run --bin perf_compare -- run large_scale

# Run test suite excluding stress tests  
cd tests && cargo run --bin perf_compare -- suite --skip stress_test

# Run only specific scenarios
cd tests && cargo run --bin perf_compare -- suite --only "small_scale,medium_scale"

# Custom service URLs
cd tests && cargo run --bin perf_compare -- \
  --rust-url http://localhost:8081 \
  --java-url http://localhost:8080 \
  run medium_scale
```

### Integration with CI/CD
Comparison tests can be integrated into development workflows:
- **perf-compare-quick**: Quick validation suitable for PR checks
- **perf-compare**: Comprehensive testing for release validation
- JSON and CSV reports for tracking performance regression over time
- Clear winner determination and performance improvement ratios