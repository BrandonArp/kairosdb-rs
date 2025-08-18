//! Real Cassandra client using CDRS-tokio for actual data writes
//!
//! This module provides a working Cassandra client that actually writes data
//! to Cassandra using the correct KairosDB schema format.

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
    time::Duration,
};
use tracing::{debug, error, info, warn};
use cdrs_tokio::{
    authenticators::NoneAuthenticatorProvider,
    cluster::{ClusterTcpConfig, session::new as session_new},
    compression::Compression,
    load_balancing::RoundRobinLoadBalancingStrategy,
    query_values,
};

type CurrentSession = cdrs_tokio::cluster::session::Session<
    cdrs_tokio::transport::TransportTcp,
    cdrs_tokio::cluster::TcpConnectionManager,
    cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy<
        cdrs_tokio::transport::TransportTcp,
        cdrs_tokio::cluster::TcpConnectionManager,
    >,
>;

use crate::config::CassandraConfig;

/// Statistics for the real Cassandra client
#[derive(Debug, Clone, Default)]
pub struct RealCassandraStats {
    pub total_queries: u64,
    pub failed_queries: u64,
    pub avg_query_time_ms: u64,
}

/// Real Cassandra client that writes data to actual Cassandra
pub struct RealCassandraClient {
    session: CurrentSession,
    config: Arc<CassandraConfig>,
    stats: Arc<AtomicU64>,
}

impl RealCassandraClient {
    /// Create a new real Cassandra client
    pub async fn new(config: Arc<CassandraConfig>) -> Result<Self> {
        info!("Creating real Cassandra client...");
        
        // Use the first contact point
        let contact_point = config.contact_points.first()
            .ok_or_else(|| anyhow::anyhow!("No Cassandra contact points provided"))?;
        
        info!("Connecting to Cassandra at: {}", contact_point);
        
        // Parse the address:port
        let parts: Vec<&str> = contact_point.split(':').collect();
        let host = parts[0];
        let port = if parts.len() > 1 { 
            parts[1].parse().unwrap_or(9042) 
        } else { 
            9042 
        };
        
        // Create a session using the builder pattern
        let cluster_config = ClusterTcpConfig::build()
            .server((host, port))
            .authenticator_class(NoneAuthenticatorProvider)
            .compression(Compression::None)
            .connection_pool_size(1)
            .load_balancing(RoundRobinLoadBalancingStrategy::new())
            .finish()
            .context("Failed to build cluster config")?;
        
        let session = cluster_config.connect().await
            .context("Failed to connect to Cassandra")?;
        
        info!("Successfully connected to Cassandra");
        
        let client = Self {
            session,
            config: config.clone(),
            stats: Arc::new(AtomicU64::new(0)),
        };
        
        // Initialize keyspace and tables
        client.ensure_keyspace_and_tables().await?;
        
        Ok(client)
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
    
    /// Execute a CQL query
    async fn execute_query(&self, query: &str) -> Result<()> {
        let start = std::time::Instant::now();
        
        let result = tokio::time::timeout(
            Duration::from_millis(self.config.query_timeout_ms),
            self.session.query(query)
        ).await;
        
        let elapsed = start.elapsed();
        self.stats.fetch_add(1, Ordering::Relaxed);
        
        match result {
            Ok(Ok(_)) => {
                debug!("Query executed successfully in {:?}: {}", elapsed, query);
                Ok(())
            }
            Ok(Err(e)) => {
                error!("Query failed: {} - Error: {:?}", query, e);
                Err(anyhow::anyhow!("Query failed: {:?}", e))
            }
            Err(_) => {
                error!("Query timed out after {:?}: {}", elapsed, query);
                Err(anyhow::anyhow!("Query timed out"))
            }
        }
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
            
            // Convert tags to KairosDB format using the public API
            let mut tags_vec: Vec<String> = point.tags.iter()
                .map(|(k, v)| format!("{}:{}", k.as_str(), v.as_str()))
                .collect();
            tags_vec.sort(); // Ensure consistent ordering
            let tags_string = tags_vec.join(",");
            
            // Create row key (metric + tags hash)
            let row_key = format!("{}:{}", metric_name, tags_string);
            let row_key_bytes = row_key.as_bytes();
            
            // Create column key (timestamp + data type)
            let data_type = match point.value {
                kairosdb_core::datapoint::DataPointValue::Long(_) => "kairos_long",
                kairosdb_core::datapoint::DataPointValue::Double(_) => "kairos_double",
                kairosdb_core::datapoint::DataPointValue::Text(_) => "kairos_string",
                kairosdb_core::datapoint::DataPointValue::Complex { .. } => "kairos_complex",
                kairosdb_core::datapoint::DataPointValue::Binary(_) => "kairos_binary",
            };
            
            let column_key = format!("{}:{}", timestamp, data_type);
            let column_key_bytes = column_key.as_bytes();
            
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
            
            // Insert into data_points table using prepared statement approach
            let insert_query = "INSERT INTO data_points (key, column1, value) VALUES (?, ?, ?)";
            
            let query_result = self.session
                .query_with_values(insert_query, query_values!(row_key_bytes, column_key_bytes, value_bytes))
                .await;
            
            if let Err(e) = query_result {
                error!("Failed to insert data point for metric {}: {}", metric_name, e);
                return Err(KairosError::cassandra(format!("Insert failed: {}", e)));
            }
            
            // Also insert into row_keys table for metadata
            let tags_map: std::collections::HashMap<String, String> = point.tags.iter()
                .map(|(k, v)| (k.as_str().to_string(), v.as_str().to_string()))
                .collect();
            
            let row_time = chrono::DateTime::from_timestamp_millis(timestamp)
                .unwrap_or_else(chrono::Utc::now);
            
            let row_keys_query = "INSERT INTO row_keys (metric, row_time, data_type, tags) VALUES (?, ?, ?, ?)";
            
            let row_keys_result = self.session
                .query_with_values(row_keys_query, query_values!(metric_name, row_time, data_type, tags_map))
                .await;
            
            if let Err(e) = row_keys_result {
                warn!("Failed to insert row key metadata for metric {}: {}", metric_name, e);
                // Continue - this is not critical for data storage
            }
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
            failed_queries: 0, // TODO: Track failed queries
            avg_query_time_ms: 0, // TODO: Track average query time
        }
    }
}