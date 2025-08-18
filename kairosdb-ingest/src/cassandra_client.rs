//! Production-grade Cassandra client for KairosDB
//! 
//! This client provides robust, high-performance data persistence using CDRS-tokio
//! with proper error handling, connection management, and KairosDB schema compatibility.

use anyhow::{Context, Result};
use async_trait::async_trait;
use kairosdb_core::{
    datapoint::{DataPointBatch, DataPointValue},
    error::{KairosError, KairosResult},
};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tracing::{debug, error, info, warn};

// CDRS-tokio imports
use cdrs_tokio::authenticators::StaticPasswordAuthenticatorProvider;
use cdrs_tokio::cluster::session::{Session, TcpSessionBuilder, SessionBuilder};
use cdrs_tokio::cluster::{NodeTcpConfigBuilder, NodeAddress};
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::transport::TransportTcp;
use cdrs_tokio::cluster::TcpConnectionManager;
use cdrs_tokio::query_values;

use crate::cassandra::{CassandraClient, CassandraStats};
use crate::config::CassandraConfig;

// Type alias for the session
type CassandraSession = Session<TransportTcp, TcpConnectionManager, RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>>;

/// Production Cassandra client implementation
pub struct CassandraClientImpl {
    session: CassandraSession,
    config: Arc<CassandraConfig>,
    stats: CassandraClientStats,
}

/// Internal statistics tracking
struct CassandraClientStats {
    total_queries: AtomicU64,
    failed_queries: AtomicU64,
    total_datapoints: AtomicU64,
    connection_errors: AtomicU64,
}

impl Default for CassandraClientStats {
    fn default() -> Self {
        Self {
            total_queries: AtomicU64::new(0),
            failed_queries: AtomicU64::new(0),
            total_datapoints: AtomicU64::new(0),
            connection_errors: AtomicU64::new(0),
        }
    }
}

impl CassandraClientImpl {
    /// Create a new production Cassandra client
    pub async fn new(config: Arc<CassandraConfig>) -> Result<Self> {
        info!("Initializing production Cassandra client");
        
        // Get the first contact point
        let contact_point = config.contact_points.first()
            .ok_or_else(|| anyhow::anyhow!("No Cassandra contact points configured"))?;
        
        info!("Connecting to Cassandra at: {}", contact_point);
        
        // Set up authentication (empty credentials for no auth)
        let auth = StaticPasswordAuthenticatorProvider::new("", "");
        
        // Parse contact point
        let socket_addr: std::net::SocketAddr = contact_point.parse()
            .context("Invalid contact point format - expected 'host:port'")?;
        let node_address = NodeAddress::from(socket_addr);
        
        // Build node configuration
        let node_config = NodeTcpConfigBuilder::new()
            .with_contact_point(node_address)
            .with_authenticator_provider(Arc::new(auth))
            .build()
            .await
            .context("Failed to build Cassandra node configuration")?;
        
        // Create session
        let session_builder = TcpSessionBuilder::new(
            RoundRobinLoadBalancingStrategy::new(),
            node_config,
        );
        
        let session = session_builder.build().await
            .context("Failed to establish Cassandra session")?;
        
        info!("Successfully connected to Cassandra");
        
        let client = Self {
            session,
            config,
            stats: CassandraClientStats::default(),
        };
        
        // Initialize schema
        client.ensure_schema().await
            .context("Failed to initialize KairosDB schema")?;
        
        Ok(client)
    }
    
    /// Execute a CQL query with error handling and metrics
    async fn execute_query(&self, query: &str) -> KairosResult<()> {
        self.stats.total_queries.fetch_add(1, Ordering::Relaxed);
        
        debug!("Executing CQL: {}", query);
        
        match self.session.query(query).await {
            Ok(_) => {
                debug!("Query executed successfully");
                Ok(())
            }
            Err(e) => {
                error!("Query failed: {} - Error: {:?}", query, e);
                self.stats.failed_queries.fetch_add(1, Ordering::Relaxed);
                Err(KairosError::cassandra(format!("CQL query failed: {}", e)))
            }
        }
    }
    
    /// Execute a parameterized query
    async fn execute_query_with_values<T>(&self, query: &str, values: T) -> KairosResult<()> 
    where
        T: Into<cdrs_tokio::query::QueryValues>,
    {
        self.stats.total_queries.fetch_add(1, Ordering::Relaxed);
        
        debug!("Executing parameterized CQL query");
        
        match self.session.query_with_values(query, values).await {
            Ok(_) => {
                debug!("Parameterized query executed successfully");
                Ok(())
            }
            Err(e) => {
                error!("Parameterized query failed: {:?}", e);
                self.stats.failed_queries.fetch_add(1, Ordering::Relaxed);
                Err(KairosError::cassandra(format!("Parameterized query failed: {}", e)))
            }
        }
    }
}

#[async_trait]
impl CassandraClient for CassandraClientImpl {
    async fn write_batch(&self, batch: &DataPointBatch) -> KairosResult<()> {
        if batch.points.is_empty() {
            return Ok(());
        }
        
        info!("Writing a batch of {} data points to Cassandra", batch.points.len());
        
        for point in &batch.points {
            // Create a KairosDB-compatible row key
            let metric_name = point.metric.as_str();
            let timestamp = point.timestamp.timestamp_millis();
            
            // Format tags consistently
            let mut tags_vec: Vec<String> = point.tags.iter()
                .map(|(k, v)| format!("{}:{}", k.as_str(), v.as_str()))
                .collect();
            tags_vec.sort(); // Ensure deterministic ordering
            let tags_string = tags_vec.join(",");
            
            // Create a composite row key
            let row_key = format!("{}:{}", metric_name, tags_string);
            let row_key_bytes = row_key.as_bytes().to_vec();
            
            // Create a column key (timestamp + data type)
            let data_type = point.data_type();
            let column_key = format!("{}:{}", timestamp, data_type);
            let column_key_bytes = column_key.as_bytes().to_vec();
            
            // Serialize value based on type
            let value_bytes = match &point.value {
                DataPointValue::Long(v) => v.to_le_bytes().to_vec(),
                DataPointValue::Double(v) => v.to_le_bytes().to_vec(),
                DataPointValue::Text(v) => v.as_bytes().to_vec(),
                DataPointValue::Complex { real, imaginary } => {
                    let mut bytes = Vec::new();
                    bytes.extend_from_slice(&real.to_le_bytes());
                    bytes.extend_from_slice(&imaginary.to_le_bytes());
                    bytes
                },
                DataPointValue::Binary(v) => v.clone(),
                DataPointValue::Histogram(h) => {
                    // Use KairosDB V2 Protocol Buffers format
                    h.to_v2_bytes()
                },
            };
            
            // Insert into the data_points table
            let insert_query = "INSERT INTO data_points (key, column1, value) VALUES (?, ?, ?)";
            
            self.execute_query_with_values(
                insert_query,
                query_values!(row_key_bytes, column_key_bytes, value_bytes)
            ).await?;
            
            self.stats.total_datapoints.fetch_add(1, Ordering::Relaxed);
        }
        
        info!("Successfully wrote a batch of {} data points", batch.points.len());
        Ok(())
    }
    
    async fn health_check(&self) -> KairosResult<bool> {
        match self.execute_query("SELECT now() FROM system.local").await {
            Ok(_) => {
                debug!("Cassandra health check passed");
                Ok(true)
            }
            Err(_) => {
                warn!("Cassandra health check failed");
                Ok(false) // Don't propagate the error, just return health status
            }
        }
    }
    
    fn get_stats(&self) -> CassandraStats {
        let total_queries = self.stats.total_queries.load(Ordering::Relaxed);
        let total_datapoints = self.stats.total_datapoints.load(Ordering::Relaxed);
        
        CassandraStats {
            total_queries,
            failed_queries: self.stats.failed_queries.load(Ordering::Relaxed),
            total_datapoints_written: total_datapoints,
            avg_batch_size: if total_queries > 0 { 
                total_datapoints as f64 / total_queries as f64 
            } else { 
                0.0 
            },
            connection_errors: self.stats.connection_errors.load(Ordering::Relaxed),
        }
    }
    
    async fn ensure_schema(&self) -> KairosResult<()> {
        info!("Initializing KairosDB schema in keyspace '{}'", self.config.keyspace);
        
        // Create keyspace
        let create_keyspace = format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': {}}}",
            self.config.keyspace,
            self.config.replication_factor
        );
        self.execute_query(&create_keyspace).await?;
        
        // Use keyspace
        let use_keyspace = format!("USE {}", self.config.keyspace);
        self.execute_query(&use_keyspace).await?;
        
        // Create the data_points table (KairosDB format)
        let create_data_points = r#"
            CREATE TABLE IF NOT EXISTS data_points (
                key blob,
                column1 blob,
                value blob,
                PRIMARY KEY (key, column1)
            ) WITH COMPACT STORAGE
        "#;
        self.execute_query(create_data_points).await?;
        
        // Create row_keys table (for metric metadata)
        let create_row_keys = r#"
            CREATE TABLE IF NOT EXISTS row_keys (
                metric text,
                row_time timestamp,
                data_type text,
                tags map<text,text>,
                PRIMARY KEY (metric, row_time, data_type, tags)
            )
        "#;
        self.execute_query(create_row_keys).await?;
        
        // Create string_index table (for tag indexing)
        let create_string_index = r#"
            CREATE TABLE IF NOT EXISTS string_index (
                key blob,
                column1 text,
                value blob,
                PRIMARY KEY (key, column1)
            )
        "#;
        self.execute_query(create_string_index).await?;
        
        info!("KairosDB schema initialized successfully");
        Ok(())
    }
}