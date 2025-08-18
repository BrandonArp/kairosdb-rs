load('ext://restart_process', 'docker_build_with_restart')

# Build the Rust ingestion service with hot reload
docker_build_with_restart(
  'kairosdb-ingest',
  './',
  entrypoint=['/app/kairosdb-ingest'],
  dockerfile='Dockerfile.dev',
  live_update=[
    sync('./target/debug/kairosdb-ingest', '/app/kairosdb-ingest'),
    sync('./config/development.yaml', '/app/config/development.yaml'),
  ],
)

# Local resource to build the Rust binary
local_resource('build-kairosdb-ingest', 'cargo build --bin kairosdb-ingest',
  deps=[
    'kairosdb-ingest/src', 
    'kairosdb-core/src', 
    'Cargo.toml', 
    'kairosdb-ingest/Cargo.toml',
    'kairosdb-core/Cargo.toml',
    'config'
  ],
  labels=["kairosdb"],
)

# Deploy Cassandra
k8s_yaml('./tilt/cassandra.yaml')
k8s_resource(
  workload='cassandra',
  port_forwards=[
    port_forward(9042, 9042, name='cql-direct')
  ],
  labels=["kairosdb", "database"],
)

# Deploy KairosDB (Java version for comparison/integration)
k8s_yaml('./tilt/kairosdb.yaml')
k8s_resource(
  workload='kairosdb',
  port_forwards=[
    port_forward(8080, 8080, name='http-direct')
  ],
  resource_deps=[
    'cassandra',
  ],
  links=[
    link('http://localhost:8080', 'KairosDB Dashboard'),
    link('http://localhost:8080/api/v1/health/check', 'KairosDB Health'),
  ],
  labels=["kairosdb", "service"],
)

# Deploy Rust Ingestion Service
k8s_yaml('./tilt/kairosdb-ingest.yaml')

k8s_resource(
  workload='kairosdb-ingest',
  port_forwards=[
    port_forward(8081, 8081, name='ingest-direct')
  ],
  resource_deps=[
    'cassandra',
    'build-kairosdb-ingest',
  ],
  links=[
    link('http://localhost:8081/health', 'Ingest Health'),
    link('http://localhost:8081/metrics', 'Ingest Metrics'),
    link('http://localhost:8081/api/v1/metrics', 'Ingest Metrics JSON'),
  ],
  labels=["kairosdb", "service"],
)