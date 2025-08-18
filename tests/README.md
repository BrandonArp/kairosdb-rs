# End-to-End Integration Tests

This directory contains end-to-end integration tests that validate the complete data flow from the Rust kairosdb-ingest service to the Java KairosDB query service.

## Prerequisites

Before running these tests, you must have the complete KairosDB environment running via Tilt:

1. **Start Tilt**: `tilt up`
2. **Wait for all services to be ready**:
   - Cassandra (port 9042)
   - Java KairosDB (port 8080)
   - Rust kairosdb-ingest (port 8081)

You can verify services are ready by checking:
- Ingest health: http://localhost:8081/health
- Java KairosDB health: http://localhost:8080/api/v1/health/check

## Running the Tests

### Run All E2E Tests
```bash
cargo test -p kairosdb-e2e-tests -- --ignored
```

### Run Specific Test Categories

**Health Checks Only:**
```bash
cargo test -p kairosdb-e2e-tests health -- --ignored
```

**Data Flow Tests Only:**
```bash
cargo test -p kairosdb-e2e-tests data_flow -- --ignored
```

### Run Individual Tests

**Basic data flow:**
```bash
cargo test -p kairosdb-e2e-tests test_basic_data_flow -- --ignored
```

**Histogram data flow:**
```bash
cargo test -p kairosdb-e2e-tests test_histogram_data_flow -- --ignored
```

**Batch ingestion:**
```bash
cargo test -p kairosdb-e2e-tests test_multiple_metrics_batch -- --ignored
```

**Complex tags:**
```bash
cargo test -p kairosdb-e2e-tests test_data_flow_with_complex_tags -- --ignored
```

### Verbose Output
Add `--nocapture` to see test output:
```bash
cargo test -p kairosdb-e2e-tests -- --ignored --nocapture
```

## Test Structure

- **`src/common.rs`**: Shared utilities and configuration
- **`src/health.rs`**: Service health check tests
- **`src/data_flow.rs`**: End-to-end data flow validation tests

## What the Tests Validate

### Health Tests
- ✅ Rust ingest service is running and healthy
- ✅ Java KairosDB service is running and healthy
- ✅ All services can respond to health checks

### Data Flow Tests
- ✅ **Basic Flow**: Send simple metric data → Verify retrieval
- ✅ **Histogram Flow**: Send histogram data → Verify retrieval
- ✅ **Batch Processing**: Send multiple metrics → Verify all retrieved
- ✅ **Complex Tags**: Send data with many tags → Verify tag filtering works

## Test Data Isolation

Each test run uses unique metric names with timestamps to avoid conflicts:
- Pattern: `e2e_test_{timestamp}.{test_suffix}`
- Example: `e2e_test_1640995200.basic_flow`

## Troubleshooting

### Test Failures

**Connection refused errors:**
- Verify Tilt is running: `tilt get uiresource`
- Check service status in Tilt UI: http://localhost:10350
- Ensure port forwards are active

**Service not ready:**
- Wait longer for services to start (Cassandra can take 2-3 minutes)
- Check individual service health endpoints

**Data not found in queries:**
- Check if Cassandra keyspace is properly initialized
- Verify ingest service is writing to the correct Cassandra instance
- Check for any ingestion errors in service logs

### Environment Variables

You can customize test behavior with these environment variables:

```bash
# Custom service URLs (if not using default Tilt setup)
export INGEST_BASE_URL="http://localhost:8081"
export JAVA_KAIROSDB_BASE_URL="http://localhost:8080"
```

### Debugging

1. **Check Tilt logs:**
   ```bash
   tilt logs kairosdb-ingest
   tilt logs kairosdb
   tilt logs cassandra
   ```

2. **Manual health checks:**
   ```bash
   curl http://localhost:8081/health
   curl http://localhost:8080/api/v1/health/check
   ```

3. **Test data manually:**
   ```bash
   # Send test data
   curl -X POST http://localhost:8081/api/v1/datapoints \
     -H "Content-Type: application/json" \
     -d '[{"name":"manual_test","datapoints":[[1640995200000,42]],"tags":{"test":"manual"}}]'
   
   # Query test data
   curl -X POST http://localhost:8080/api/v1/datapoints/query \
     -H "Content-Type: application/json" \
     -d '{"start_relative":{"value":1,"unit":"hours"},"metrics":[{"name":"manual_test"}]}'
   ```

## Integration with CI/CD

These tests are designed to run in CI environments where the full KairosDB stack is deployed. The `--ignored` flag ensures they don't run in regular unit test suites but can be explicitly invoked in integration test stages.