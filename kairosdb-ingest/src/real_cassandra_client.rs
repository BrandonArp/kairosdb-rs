//! Real CDRS-tokio Cassandra client using official API patterns
//!
//! Based on the official CDRS-tokio example at:
//! https://raw.githubusercontent.com/krojew/cdrs-tokio/refs/heads/master/cdrs-tokio/examples/crud_operations.rs

use anyhow::{Context, Result};
use kairosdb_core::{
    datapoint::DataPointBatch,
    error::{KairosError, KairosResult},
};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tracing::{debug, error, info, warn};

// CDRS-tokio imports following the official example
use cdrs_tokio::authenticators::StaticPasswordAuthenticatorProvider;
use cdrs_tokio::cluster::session::{Session, TcpSessionBuilder, SessionBuilder};
use cdrs_tokio::cluster::{NodeTcpConfigBuilder, NodeAddress};
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::transport::TransportTcp;
use cdrs_tokio::cluster::TcpConnectionManager;
use cdrs_tokio::query_values;

use crate::config::CassandraConfig;

// Type alias for our session following the example pattern
type CurrentSession = Session<TransportTcp, TcpConnectionManager, RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>>;

/// Statistics for the real Cassandra client
#[derive(Debug, Clone, Default)]
pub struct RealCassandraStats {
    pub total_queries: u64,
    pub failed_queries: u64,
}

/// Real Cassandra client using proper CDRS-tokio API
pub struct RealCassandraClient {
    session: CurrentSession,
    config: Arc<CassandraConfig>,
    stats: Arc<AtomicU64>,
    failed_stats: Arc<AtomicU64>,
}

impl RealCassandraClient {
    /// Create a new real Cassandra client using official CDRS-tokio patterns
    pub async fn new(config: Arc<CassandraConfig>) -> Result<Self> {
        info!("Creating real CDRS Cassandra client...");
        
        // Use the first contact point
        let contact_point = config.contact_points.first()
            .ok_or_else(|| anyhow::anyhow!("No Cassandra contact points provided"))?;
        
        info!("Connecting to Cassandra at: {}", contact_point);
        
        // Create authentication provider (using None/empty for no authentication)
        let auth = StaticPasswordAuthenticatorProvider::new("", "");
        
        // Parse contact point into NodeAddress
        let socket_addr: std::net::SocketAddr = contact_point.parse()
            .context("Invalid contact point format")?;
        let node_address = NodeAddress::from(socket_addr);
        
        // Build node configuration following the official example
        let node_config = NodeTcpConfigBuilder::new()
            .with_contact_point(node_address)
            .with_authenticator_provider(Arc::new(auth))
            .build()
            .await
            .context("Failed to build node config")?;
        
        // Create session using the TcpSessionBuilder pattern
        let session = TcpSessionBuilder::new(
            RoundRobinLoadBalancingStrategy::new(),
            node_config,
        );
        
        // Connect to get the actual session
        let session: CurrentSession = session.build().await
            .context("Failed to create Cassandra session")?;
        
        info!("Successfully connected to Cassandra");
        
        let client = Self {
            session,
            config: config.clone(),
            stats: Arc::new(AtomicU64::new(0)),
            failed_stats: Arc::new(AtomicU64::new(0)),
        };
        
        // Initialize keyspace and tables
        client.ensure_keyspace_and_tables().await?;
        
        Ok(client)
    }
    
    /// Execute a CQL query
    async fn execute_query(&self, query: &str) -> Result<()> {
        let start = std::time::Instant::now();
        
        debug!("Executing CQL: {}", query);
        
        let result = self.session.query(query).await;
        
        let elapsed = start.elapsed();
        self.stats.fetch_add(1, Ordering::Relaxed);
        
        match result {
            Ok(_) => {
                debug!("Query executed successfully in {:?}", elapsed);
                Ok(())
            }
            Err(e) => {
                error!("Query failed: {} - Error: {:?}", query, e);
                self.failed_stats.fetch_add(1, Ordering::Relaxed);
                Err(anyhow::anyhow!("Query failed: {:?}", e))
            }
        }
    }
    
    /// Ensure keyspace and tables exist (KairosDB compatible schema)
    async fn ensure_keyspace_and_tables(&self) -> Result<()> {
        info!("Ensuring KairosDB keyspace '{}' exists", self.config.keyspace);
        
        // Create keyspace
        let create_keyspace = format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': {}}}",
            self.config.keyspace,
            self.config.replication_factor
        );
        
        self.execute_query(&create_keyspace).await
            .context("Failed to create keyspace")?;
        
        // Use keyspace
        let use_keyspace = format!("USE {}", self.config.keyspace);
        self.execute_query(&use_keyspace).await
            .context("Failed to use keyspace")?;
        
        // Create KairosDB data_points table (compatible with KairosDB schema)
        let create_data_points = r#"
            CREATE TABLE IF NOT EXISTS data_points (
                key blob,
                column1 blob,
                value blob,
                PRIMARY KEY (key, column1)
            ) WITH COMPACT STORAGE
        "#;
        
        self.execute_query(create_data_points).await
            .context("Failed to create data_points table")?;
        
        // Create KairosDB row_keys table (for metric metadata)
        let create_row_keys = r#"
            CREATE TABLE IF NOT EXISTS row_keys (
                metric text,
                row_time timestamp,
                data_type text,
                tags map<text,text>,
                PRIMARY KEY (metric, row_time, data_type, tags)
            )
        "#;
        
        self.execute_query(create_row_keys).await
            .context("Failed to create row_keys table")?;
        
        // Create KairosDB string_index table (for tag indexing)
        let create_string_index = r#"
            CREATE TABLE IF NOT EXISTS string_index (
                key blob,
                column1 text,
                value blob,
                PRIMARY KEY (key, column1)
            )
        "#;
        
        self.execute_query(create_string_index).await
            .context("Failed to create string_index table")?;
        
        info!("KairosDB keyspace and tables initialized successfully");
        Ok(())
    }
    
    /// Write a batch of data points to Cassandra in KairosDB format
    pub async fn write_batch(&self, batch: &DataPointBatch) -> KairosResult<()> {
        if batch.points.is_empty() {
            return Ok(());
        }
        
        info!("Writing batch of {} data points to Cassandra", batch.points.len());
        
        for point in &batch.points {
            // Create KairosDB-compatible key and column formats
            let metric_name = point.metric.as_str();
            let timestamp = point.timestamp.timestamp_millis();
            
            // Convert tags to KairosDB format
            let mut tags_vec: Vec<String> = point.tags.iter()
                .map(|(k, v)| format!("{}:{}", k.as_str(), v.as_str()))
                .collect();
            tags_vec.sort(); // Ensure consistent ordering
            let tags_string = tags_vec.join(",");
            
            // Create row key (metric + tags hash)
            let row_key = format!("{}:{}", metric_name, tags_string);
            let row_key_bytes = row_key.as_bytes().to_vec();
            
            // Create column key (timestamp + data type)
            let data_type = match point.value {
                kairosdb_core::datapoint::DataPointValue::Long(_) => "kairos_long",
                kairosdb_core::datapoint::DataPointValue::Double(_) => "kairos_double",
                kairosdb_core::datapoint::DataPointValue::Text(_) => "kairos_string",
                kairosdb_core::datapoint::DataPointValue::Complex { .. } => "kairos_complex",
                kairosdb_core::datapoint::DataPointValue::Binary(_) => "kairos_binary",
            };
            
            let column_key = format!("{}:{}", timestamp, data_type);
            let column_key_bytes = column_key.as_bytes().to_vec();
            
            // Create value bytes
            let value_bytes = match &point.value {
                kairosdb_core::datapoint::DataPointValue::Long(v) => v.to_le_bytes().to_vec(),
                kairosdb_core::datapoint::DataPointValue::Double(v) => v.to_le_bytes().to_vec(),
                kairosdb_core::datapoint::DataPointValue::Text(v) => v.as_bytes().to_vec(),
                kairosdb_core::datapoint::DataPointValue::Complex { real, imaginary } => {
                    let mut bytes = Vec::new();
                    bytes.extend_from_slice(&real.to_le_bytes());
                    bytes.extend_from_slice(&imaginary.to_le_bytes());
                    bytes
                },
                kairosdb_core::datapoint::DataPointValue::Binary(v) => v.clone(),
            };
            
            // Insert into data_points table using parameterized query
            let insert_query = "INSERT INTO data_points (key, column1, value) VALUES (?, ?, ?)";
            
            let query_result = self.session
                .query_with_values(insert_query, query_values!(row_key_bytes, column_key_bytes, value_bytes))
                .await;
            
            if let Err(e) = query_result {
                error!("Failed to insert data point for metric {}: {}", metric_name, e);
                self.failed_stats.fetch_add(1, Ordering::Relaxed);
                return Err(KairosError::cassandra(format!("Insert failed: {}", e)));
            }
            
            debug!("Successfully inserted data point for metric: {}", metric_name);
        }
        
        info!("Successfully wrote batch of {} data points to Cassandra", batch.points.len());
        Ok(())
    }
    
    /// Get health check
    pub async fn health_check(&self) -> KairosResult<bool> {
        match self.execute_query("SELECT now() FROM system.local").await {
            Ok(_) => {
                debug!("Cassandra health check passed");
                Ok(true)
            }
            Err(e) => {
                warn!("Cassandra health check failed: {}", e);
                Ok(false)
            }
        }
    }
    
    /// Get statistics
    pub fn get_stats(&self) -> RealCassandraStats {
        RealCassandraStats {
            total_queries: self.stats.load(Ordering::Relaxed),
            failed_queries: self.failed_stats.load(Ordering::Relaxed),
        }
    }
}