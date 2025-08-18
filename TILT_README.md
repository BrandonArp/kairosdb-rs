# KairosDB Rust Development with Tilt

This project includes a Tilt setup for local development that spins up:

1. **Cassandra** (single node) on port 9042
2. **KairosDB** (Java version) on port 8080 
3. **KairosDB Rust Ingestion Service** on port 8081

## Prerequisites

- [Tilt](https://tilt.dev/) installed
- [Docker](https://docker.com/) installed  
- [kubectl](https://kubernetes.io/docs/tasks/tools/) configured with a local cluster (e.g., kind, minikube, Docker Desktop)
- [Rust](https://rustup.rs/) toolchain installed

## Quick Start

1. **Start the development environment:**
   ```bash
   tilt up
   ```

2. **Access the services:**
   - **Tilt Dashboard**: http://localhost:10350
   - **KairosDB Dashboard**: http://localhost:8080
   - **Rust Ingestion Service Health**: http://localhost:8081/health
   - **Rust Ingestion Service Metrics**: http://localhost:8081/metrics

3. **Test data ingestion:**
   ```bash
   # Send data to the Rust ingestion service
   curl -X POST http://localhost:8081/api/v1/datapoints \
     -H "Content-Type: application/json" \
     -d '{
       "name": "test.metric",
       "datapoints": [[1634567890000, 42]],
       "tags": {"host": "localhost"}
     }'

   # Query data from KairosDB
   curl -X POST http://localhost:8080/api/v1/datapoints/query \
     -H "Content-Type: application/json" \
     -d '{
       "start_relative": {"value": 1, "unit": "hours"},
       "metrics": [{"name": "test.metric"}]
     }'
   ```

## Development Workflow

### Hot Reload
- **Rust code changes**: Automatically trigger rebuild and restart of the ingestion service
- **Config changes**: Live-synced to the running container

### Monitoring
- Use the Tilt dashboard to monitor logs and resource status
- Check service health endpoints
- View Prometheus metrics from the Rust service

### Database Access
- **Cassandra CQL**: Connect directly to `localhost:9042`
- **KairosDB REST API**: Available at `localhost:8080/api/v1/`

## Service Details

### Cassandra
- **Image**: `cassandra:4.1`
- **Port**: 9042
- **Keyspace**: `kairosdb` (auto-created)
- **Storage**: 2GB persistent volume

### KairosDB (Java)
- **Image**: `inscopemetrics/kairosdb-extensions:2.3.10`
- **Port**: 8080
- **Connected to**: Cassandra cluster
- **Health Check**: `/api/v1/health/check`

### Rust Ingestion Service
- **Built from**: Local source code
- **Port**: 8081
- **Config**: `config/development.yaml`
- **Connected to**: Same Cassandra cluster
- **Endpoints**:
  - `GET /health` - Health check
  - `GET /metrics` - Prometheus metrics
  - `POST /api/v1/datapoints` - Data ingestion

## Troubleshooting

### Services not starting
1. Check Tilt dashboard for errors
2. Verify Kubernetes cluster is running
3. Check resource constraints

### Connection issues
1. **Cassandra startup**: Wait for Cassandra readiness probe (60-90 seconds)
2. **KairosDB startup**: Wait for KairosDB readiness probe (120-180 seconds after Cassandra is ready)
3. Check service logs in Tilt dashboard for connection errors
4. Verify port forwards are active
5. Ensure services can resolve DNS names (cassandra:9042)

### Build issues
1. Ensure Rust toolchain is installed
2. Run `cargo build` manually to check for errors
3. Check file sync in Tilt dashboard

### KairosDB Configuration Issues
1. **Custom configuration**: KairosDB uses a custom `kairosdb.properties` file mounted as ConfigMap
2. **Cassandra connection**: Configuration explicitly sets `kairosdb.datastore.cassandra.host_list=cassandra:9042`
3. **Service startup order**: KairosDB waits for Cassandra dependency before starting
4. **Health checks**: Increased timeouts to allow proper startup sequence

### Common Error Messages
- **"All host(s) tried for query failed"**: Indicates Cassandra is not ready or reachable
  - Solution: Wait longer for Cassandra startup, check Cassandra logs
- **"Cannot connect to localhost:9042"**: Indicates configuration not properly overridden
  - Solution: Verify ConfigMap is mounted correctly and configuration is applied

## Stopping the Environment

```bash
tilt down
```

This will stop all services and clean up resources (persistent volumes are preserved).
