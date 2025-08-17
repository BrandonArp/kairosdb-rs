use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::env;

/// Configuration for the query service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Address to bind the HTTP server to
    pub bind_address: String,
    
    /// Cassandra configuration
    pub cassandra: CassandraConfig,
    
    /// Query limits and settings
    pub query: QueryLimitsConfig,
    
    /// Caching configuration
    pub cache: CacheConfig,
    
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
    
    /// Consistency level for reads
    pub read_consistency: String,
}

/// Query limits and configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryLimitsConfig {
    /// Maximum number of data points per query
    pub max_data_points: usize,
    
    /// Maximum query time range in milliseconds
    pub max_query_range_ms: i64,
    
    /// Maximum number of metrics per query
    pub max_metrics_per_query: usize,
    
    /// Query timeout in milliseconds
    pub query_timeout_ms: u64,
    
    /// Maximum number of concurrent queries
    pub max_concurrent_queries: usize,
    
    /// Default limit for unbounded queries
    pub default_limit: usize,
}

/// Caching configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Enable query result caching
    pub enable_caching: bool,
    
    /// Maximum cache size in MB
    pub max_cache_size_mb: usize,
    
    /// Default cache TTL in seconds
    pub default_ttl_seconds: u64,
    
    /// Maximum cache TTL in seconds
    pub max_ttl_seconds: u64,
    
    /// Cache key prefix
    pub key_prefix: String,
}

/// Metrics and monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Enable Prometheus metrics endpoint
    pub enable_prometheus: bool,
    
    /// Metrics endpoint path
    pub metrics_path: String,
    
    /// Enable detailed query metrics
    pub enable_detailed_metrics: bool,
    
    /// Enable slow query logging
    pub enable_slow_query_log: bool,
    
    /// Slow query threshold in milliseconds
    pub slow_query_threshold_ms: u64,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8081".to_string(),
            cassandra: CassandraConfig::default(),
            query: QueryLimitsConfig::default(),
            cache: CacheConfig::default(),
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
            read_consistency: "LOCAL_ONE".to_string(),
        }
    }
}

impl Default for QueryLimitsConfig {
    fn default() -> Self {
        Self {
            max_data_points: 1_000_000,
            max_query_range_ms: 365 * 24 * 60 * 60 * 1000, // 1 year
            max_metrics_per_query: 100,
            query_timeout_ms: 30000, // 30 seconds
            max_concurrent_queries: 100,
            default_limit: 10000,
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enable_caching: true,
            max_cache_size_mb: 512,
            default_ttl_seconds: 300, // 5 minutes
            max_ttl_seconds: 3600, // 1 hour
            key_prefix: "kairosdb:query:".to_string(),
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enable_prometheus: true,
            metrics_path: "/metrics".to_string(),
            enable_detailed_metrics: false,
            enable_slow_query_log: true,
            slow_query_threshold_ms: 1000, // 1 second
        }
    }
}

impl QueryConfig {
    /// Load configuration from environment variables and defaults
    pub fn load() -> Result<Self> {
        let mut config = Self::default();
        
        // Override with environment variables if present
        if let Ok(bind_addr) = env::var("KAIROSDB_QUERY_BIND_ADDRESS") {
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
        
        if let Ok(max_data_points) = env::var("KAIROSDB_QUERY_MAX_DATA_POINTS") {
            config.query.max_data_points = max_data_points.parse()?;
        }
        
        if let Ok(query_timeout) = env::var("KAIROSDB_QUERY_TIMEOUT_MS") {
            config.query.query_timeout_ms = query_timeout.parse()?;
        }
        
        if let Ok(enable_caching) = env::var("KAIROSDB_CACHE_ENABLE") {
            config.cache.enable_caching = enable_caching.parse()?;
        }
        
        if let Ok(cache_size) = env::var("KAIROSDB_CACHE_MAX_SIZE_MB") {
            config.cache.max_cache_size_mb = cache_size.parse()?;
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
        
        if self.query.max_data_points == 0 {
            return Err(anyhow::anyhow!("Max data points must be greater than 0"));
        }
        
        if self.query.max_query_range_ms <= 0 {
            return Err(anyhow::anyhow!("Max query range must be positive"));
        }
        
        if self.query.query_timeout_ms == 0 {
            return Err(anyhow::anyhow!("Query timeout must be greater than 0"));
        }
        
        if self.query.max_concurrent_queries == 0 {
            return Err(anyhow::anyhow!("Max concurrent queries must be greater than 0"));
        }
        
        if self.cache.max_cache_size_mb == 0 {
            return Err(anyhow::anyhow!("Cache size must be greater than 0"));
        }
        
        Ok(())
    }
}