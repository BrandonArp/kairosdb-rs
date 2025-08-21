load('ext://restart_process', 'docker_build_with_restart')

# Build the Rust ingestion service with hot reload
docker_build_with_restart(
  'kairosdb-ingest',
  './',
  entrypoint=['/app/kairosdb-ingest'],
  dockerfile='Dockerfile.multi.dev',
  target='ingest',
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

# Build the Rust query service with hot reload
docker_build_with_restart(
  'kairosdb-query',
  './',
  entrypoint=['/app/kairosdb-query'],
  dockerfile='Dockerfile.multi.dev',
  target='query',
  live_update=[
    sync('./target/debug/kairosdb-query', '/app/kairosdb-query'),
    sync('./config/development.yaml', '/app/config/development.yaml'),
  ],
)

# Local resource to build the Rust query binary
local_resource('build-kairosdb-query', 'cargo build --bin kairosdb-query',
  deps=[
    'kairosdb-query/src', 
    'kairosdb-core/src', 
    'Cargo.toml', 
    'kairosdb-query/Cargo.toml',
    'kairosdb-core/Cargo.toml',
    'config'
  ],
  labels=["kairosdb"],
)

# Deploy Rust Query Service
k8s_yaml('./tilt/kairosdb-query.yaml')

k8s_resource(
  workload='kairosdb-query',
  port_forwards=[
    port_forward(8082, 8082, name='query-direct')
  ],
  resource_deps=[
    'cassandra',
    'build-kairosdb-query',
  ],
  links=[
    link('http://localhost:8082/health', 'Query Health'),
    link('http://localhost:8082/metrics', 'Query Metrics'),
    link('http://localhost:8082/api/v1/metricnames', 'Query Metric Names'),
  ],
  labels=["kairosdb", "service"],
)