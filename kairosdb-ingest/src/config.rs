use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::env;
use std::time::Duration;

/// Performance testing modes for isolating pipeline stages
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PerformanceMode {
    /// Skip all processing - just return success immediately
    NoParseMode,
    /// Parse and validate JSON but don't store to Cassandra
    ParseOnlyMode,
    /// Full pipeline - parse and store to Cassandra (normal mode)
    ParseAndStoreMode,
}

/// Configuration for the ingest service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestConfig {
    /// Address to bind the HTTP server to
    pub bind_address: String,

    /// Cassandra configuration
    pub cassandra: CassandraConfig,

    /// Ingestion limits and settings
    pub ingestion: IngestionConfig,

    /// Metrics and monitoring configuration
    pub metrics: MetricsConfig,

    /// Performance and resource limits
    pub performance: PerformanceConfig,

    /// Health check configuration
    pub health: HealthConfig,
}

/// Cassandra connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CassandraConfig {
    /// Cassandra contact points
    pub contact_points: Vec<String>,

    /// Keyspace name
    pub keyspace: String,

    /// Connection timeout in milliseconds
    pub connection_timeout_ms: u64,

    /// Query timeout in milliseconds
    pub query_timeout_ms: u64,

    /// Maximum number of connections
    pub max_connections: usize,

    /// Maximum concurrent requests per connection
    pub max_concurrent_requests_per_connection: usize,

    /// Username for authentication
    pub username: Option<String>,

    /// Password for authentication
    pub password: Option<String>,

    /// Replication factor for keyspace
    pub replication_factor: usize,

    /// Health check query
    pub cassandra_health_query: String,
}

/// Ingestion-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionConfig {
    /// Maximum batch size for processing
    pub max_batch_size: usize,

    /// Batch timeout in milliseconds
    pub batch_timeout_ms: u64,

    /// Maximum queue size for pending batches
    pub max_queue_size: usize,

    /// Maximum disk usage for queue in bytes (default: 10GB)
    pub max_queue_disk_bytes: u64,

    /// Number of worker threads for ingestion
    pub worker_threads: usize,

    /// Enable validation of incoming data
    pub enable_validation: bool,

    /// Maximum request size in bytes
    pub max_request_size: usize,

    /// Performance testing mode
    pub performance_mode: PerformanceMode,
}

/// Metrics and monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Enable Prometheus metrics endpoint
    pub enable_prometheus: bool,

    /// Metrics endpoint path
    pub metrics_path: String,

    /// Enable detailed timing metrics
    pub enable_detailed_metrics: bool,

    /// Metrics collection interval in seconds
    pub collection_interval_seconds: u64,

    /// Enable memory usage metrics
    pub enable_memory_metrics: bool,

    /// Enable per-metric ingestion rates
    pub enable_per_metric_rates: bool,
}

/// Performance and resource limits configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Maximum concurrent requests
    pub max_concurrent_requests: usize,

    /// Request timeout in milliseconds
    pub request_timeout_ms: u64,

    /// Maximum memory usage in MB before backpressure
    pub max_memory_mb: usize,

    /// Enable request compression
    pub enable_compression: bool,

    /// Maximum decompressed payload size in bytes
    pub max_decompressed_size: usize,

    /// Connection keep-alive timeout
    pub keep_alive_timeout_seconds: u64,
}

/// Health check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    /// Health check endpoint path
    pub health_path: String,

    /// Readiness check endpoint path
    pub readiness_path: String,

    /// Liveness check endpoint path
    pub liveness_path: String,

    /// Health check timeout in milliseconds
    pub check_timeout_ms: u64,

    /// Cassandra health check query
    pub cassandra_health_query: String,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".to_string(),
            cassandra: CassandraConfig::default(),
            ingestion: IngestionConfig::default(),
            metrics: MetricsConfig::default(),
            performance: PerformanceConfig::default(),
            health: HealthConfig::default(),
        }
    }
}

impl Default for CassandraConfig {
    fn default() -> Self {
        Self {
            contact_points: vec!["127.0.0.1:9042".to_string()],
            keyspace: "kairosdb".to_string(),
            connection_timeout_ms: 5000,
            query_timeout_ms: 10000,
            max_connections: 20, // Increased for better concurrency
            max_concurrent_requests_per_connection: 100, // High concurrency per connection
            username: None,
            password: None,
            replication_factor: 1,
            cassandra_health_query: "SELECT now() FROM system.local".to_string(),
        }
    }
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 10000,
            batch_timeout_ms: 1000,
            max_queue_size: 10_000_000, // 10M items default for disk-backed queue
            max_queue_disk_bytes: 10 * 1024 * 1024 * 1024, // 10GB default
            worker_threads: 4,
            enable_validation: true,
            max_request_size: 100 * 1024 * 1024, // 100MB
            performance_mode: PerformanceMode::ParseAndStoreMode, // Default to normal operation
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enable_prometheus: true,
            metrics_path: "/metrics".to_string(),
            enable_detailed_metrics: false,
            collection_interval_seconds: 60,
            enable_memory_metrics: true,
            enable_per_metric_rates: false,
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            max_concurrent_requests: 1000,
            request_timeout_ms: 30000,
            max_memory_mb: 1024,
            enable_compression: true,
            max_decompressed_size: 100 * 1024 * 1024, // 100MB
            keep_alive_timeout_seconds: 75,
        }
    }
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            health_path: "/health".to_string(),
            readiness_path: "/health/ready".to_string(),
            liveness_path: "/health/live".to_string(),
            check_timeout_ms: 5000,
            cassandra_health_query: "SELECT now() FROM system.local".to_string(),
        }
    }
}

impl IngestConfig {
    /// Load configuration from file, environment variables, and defaults
    pub fn load() -> Result<Self> {
        let mut config = Self::default();

        // Try to load from config file first
        if let Ok(config_path) = env::var("CONFIG_PATH") {
            config = Self::load_from_file(&config_path)?;
        } else if std::path::Path::new("config/development.yaml").exists() {
            config = Self::load_from_file("config/development.yaml")?;
        } else if std::path::Path::new("config/production.yaml").exists() {
            config = Self::load_from_file("config/production.yaml")?;
        }

        // Override with environment variables if present
        if let Ok(bind_addr) = env::var("KAIROSDB_BIND_ADDRESS") {
            config.bind_address = bind_addr;
        }

        if let Ok(contact_points) = env::var("KAIROSDB_CASSANDRA_CONTACT_POINTS") {
            config.cassandra.contact_points = contact_points
                .split(',')
                .map(|s| s.trim().to_string())
                .collect();
        }

        if let Ok(keyspace) = env::var("KAIROSDB_CASSANDRA_KEYSPACE") {
            config.cassandra.keyspace = keyspace;
        }

        if let Ok(username) = env::var("KAIROSDB_CASSANDRA_USERNAME") {
            config.cassandra.username = Some(username);
        }

        if let Ok(password) = env::var("KAIROSDB_CASSANDRA_PASSWORD") {
            config.cassandra.password = Some(password);
        }

        if let Ok(max_batch_size) = env::var("KAIROSDB_MAX_BATCH_SIZE") {
            config.ingestion.max_batch_size = max_batch_size.parse()?;
        }

        if let Ok(worker_threads) = env::var("KAIROSDB_WORKER_THREADS") {
            config.ingestion.worker_threads = worker_threads.parse()?;
        }

        if let Ok(enable_validation) = env::var("KAIROSDB_ENABLE_VALIDATION") {
            config.ingestion.enable_validation = enable_validation.parse()?;
        }

        if let Ok(perf_mode) = env::var("KAIROSDB_PERFORMANCE_MODE") {
            config.ingestion.performance_mode = match perf_mode.to_lowercase().as_str() {
                "no_parse" => PerformanceMode::NoParseMode,
                "parse_only" => PerformanceMode::ParseOnlyMode,
                "parse_and_store" => PerformanceMode::ParseAndStoreMode,
                _ => return Err(anyhow::anyhow!("Invalid performance mode: {}. Valid options: no_parse, parse_only, parse_and_store", perf_mode)),
            };
        }

        // Performance configuration
        if let Ok(max_concurrent) = env::var("KAIROSDB_MAX_CONCURRENT_REQUESTS") {
            config.performance.max_concurrent_requests = max_concurrent.parse()?;
        }

        if let Ok(request_timeout) = env::var("KAIROSDB_REQUEST_TIMEOUT_MS") {
            config.performance.request_timeout_ms = request_timeout.parse()?;
        }

        if let Ok(max_memory) = env::var("KAIROSDB_MAX_MEMORY_MB") {
            config.performance.max_memory_mb = max_memory.parse()?;
        }

        if let Ok(enable_compression) = env::var("KAIROSDB_ENABLE_COMPRESSION") {
            config.performance.enable_compression = enable_compression.parse()?;
        }

        // Metrics configuration
        if let Ok(enable_prometheus) = env::var("KAIROSDB_ENABLE_PROMETHEUS") {
            config.metrics.enable_prometheus = enable_prometheus.parse()?;
        }

        if let Ok(metrics_path) = env::var("KAIROSDB_METRICS_PATH") {
            config.metrics.metrics_path = metrics_path;
        }

        if let Ok(enable_detailed_metrics) = env::var("KAIROSDB_ENABLE_DETAILED_METRICS") {
            config.metrics.enable_detailed_metrics = enable_detailed_metrics.parse()?;
        }

        // Health configuration
        if let Ok(health_path) = env::var("KAIROSDB_HEALTH_PATH") {
            config.health.health_path = health_path;
        }

        // Validate the loaded configuration
        config.validate()?;

        Ok(config)
    }

    /// Load configuration from a YAML file
    pub fn load_from_file(path: &str) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;

        let config: Self = serde_yaml::from_str(&contents)
            .with_context(|| format!("Failed to parse config file: {}", path))?;

        Ok(config)
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.cassandra.contact_points.is_empty() {
            return Err(anyhow::anyhow!(
                "At least one Cassandra contact point is required"
            ));
        }

        if self.cassandra.keyspace.is_empty() {
            return Err(anyhow::anyhow!("Cassandra keyspace cannot be empty"));
        }

        if self.ingestion.max_batch_size == 0 {
            return Err(anyhow::anyhow!("Max batch size must be greater than 0"));
        }

        if self.ingestion.worker_threads == 0 {
            return Err(anyhow::anyhow!("Worker threads must be greater than 0"));
        }

        if self.ingestion.max_queue_size == 0 {
            return Err(anyhow::anyhow!("Max queue size must be greater than 0"));
        }

        if self.performance.max_concurrent_requests == 0 {
            return Err(anyhow::anyhow!(
                "Max concurrent requests must be greater than 0"
            ));
        }

        if self.performance.request_timeout_ms == 0 {
            return Err(anyhow::anyhow!("Request timeout must be greater than 0"));
        }

        Ok(())
    }

    /// Get the batch timeout as a Duration
    pub fn batch_timeout(&self) -> Duration {
        Duration::from_millis(self.ingestion.batch_timeout_ms)
    }

    /// Get the request timeout as a Duration
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.performance.request_timeout_ms)
    }

    /// Get the connection timeout as a Duration
    pub fn connection_timeout(&self) -> Duration {
        Duration::from_millis(self.cassandra.connection_timeout_ms)
    }

    /// Get the query timeout as a Duration
    pub fn query_timeout(&self) -> Duration {
        Duration::from_millis(self.cassandra.query_timeout_ms)
    }

    /// Get the health check timeout as a Duration
    pub fn health_check_timeout(&self) -> Duration {
        Duration::from_millis(self.health.check_timeout_ms)
    }

    /// Check if compression is enabled
    pub fn is_compression_enabled(&self) -> bool {
        self.performance.enable_compression
    }

    /// Check if Prometheus metrics are enabled
    pub fn is_prometheus_enabled(&self) -> bool {
        self.metrics.enable_prometheus
    }

    /// Get the maximum memory usage in bytes
    pub fn max_memory_bytes(&self) -> usize {
        self.performance.max_memory_mb * 1024 * 1024
    }
}
