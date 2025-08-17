use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::env;

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
    
    /// Username for authentication
    pub username: Option<String>,
    
    /// Password for authentication
    pub password: Option<String>,
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
    
    /// Number of worker threads for ingestion
    pub worker_threads: usize,
    
    /// Enable validation of incoming data
    pub enable_validation: bool,
    
    /// Maximum request size in bytes
    pub max_request_size: usize,
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
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".to_string(),
            cassandra: CassandraConfig::default(),
            ingestion: IngestionConfig::default(),
            metrics: MetricsConfig::default(),
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
            max_connections: 10,
            username: None,
            password: None,
        }
    }
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 10000,
            batch_timeout_ms: 1000,
            max_queue_size: 100,
            worker_threads: 4,
            enable_validation: true,
            max_request_size: 100 * 1024 * 1024, // 100MB
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enable_prometheus: true,
            metrics_path: "/metrics".to_string(),
            enable_detailed_metrics: false,
        }
    }
}

impl IngestConfig {
    /// Load configuration from environment variables and defaults
    pub fn load() -> Result<Self> {
        let mut config = Self::default();
        
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
        
        Ok(config)
    }
    
    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.cassandra.contact_points.is_empty() {
            return Err(anyhow::anyhow!("At least one Cassandra contact point is required"));
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
        
        Ok(())
    }
}