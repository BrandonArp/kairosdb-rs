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
    collections::HashSet,
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
        
        // Validate contact point format
        let parts: Vec<&str> = contact_point.split(':').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!("Invalid contact point format - expected 'host:port', got '{}'", contact_point));
        }
        
        // Validate port number
        let _port: u16 = parts[1].parse()
            .context("Invalid port number in contact point")?;
        
        // Build node configuration (CDRS handles hostname resolution internally)
        let node_config = NodeTcpConfigBuilder::new()
            .with_contact_point(contact_point.into())
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

impl CassandraClientImpl {
    /// Write a single data point to the data_points table
    async fn write_data_point(&self, point: &kairosdb_core::datapoint::DataPoint) -> KairosResult<()> {
        // Create KairosDB-compatible row key using the proper binary format
        let row_key = kairosdb_core::cassandra::RowKey::from_data_point(point);
        let row_key_bytes = row_key.to_bytes();
        
        // Create column key as 4-byte integer (timestamp offset left-shifted by 1)
        let column_name = kairosdb_core::cassandra::ColumnName::from_timestamp(point.timestamp);
        let column_key_bytes = column_name.to_bytes();
        
        // Serialize value using KairosDB-compatible format
        let cassandra_value = kairosdb_core::cassandra::CassandraValue::from_data_point_value(&point.value, None);
        let value_bytes = cassandra_value.bytes;
        
        // Use direct CQL to bypass CDRS blob encoding issues - same problem as string_index
        let direct_query = format!(
            "INSERT INTO data_points (key, column1, value) VALUES (0x{}, 0x{}, 0x{})",
            hex::encode(&row_key_bytes),
            hex::encode(&column_key_bytes),
            hex::encode(&value_bytes)
        );
        
        debug!("Data point CQL: {}", direct_query);
        self.execute_query(&direct_query).await?;
        
        self.stats.total_datapoints.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    
    /// Write row key metadata to row_keys table
    async fn write_row_key_metadata(&self, point: &kairosdb_core::datapoint::DataPoint) -> KairosResult<()> {
        let row_time = point.timestamp.row_time();
        let metric_name = point.metric.as_str();
        let data_type = point.data_type();
        
        // Convert tags to map format for Cassandra
        let tags_map: std::collections::HashMap<String, String> = point.tags.iter()
            .map(|(k, v)| (k.as_str().to_string(), v.as_str().to_string()))
            .collect();
        
        // Insert into row_keys table - Java KairosDB compatible format
        let row_keys_query = r#"
            INSERT INTO row_keys (metric, table_name, row_time, data_type, tags, mtime) 
            VALUES (?, ?, ?, ?, ?, now())
        "#;
        
        self.execute_query_with_values(
            row_keys_query,
            query_values!(
                metric_name,
                "data_points",
                row_time.timestamp_millis(),
                data_type,
                tags_map
            )
        ).await?;
        
        // Also insert into row_key_time_index for time-based queries
        let time_index_query = r#"
            INSERT INTO row_key_time_index (metric, table_name, row_time) 
            VALUES (?, ?, ?)
        "#;
        
        self.execute_query_with_values(
            time_index_query,
            query_values!(
                metric_name,
                "data_points", 
                row_time.timestamp_millis()
            )
        ).await?;
        
        Ok(())
    }
    
    /// Write indexing information to string_index table
    async fn write_indexes(&self, 
        metrics: &HashSet<&str>, 
        tag_keys: &HashSet<&str>, 
        tag_values: &HashSet<&str>
    ) -> KairosResult<()> {
        
        // Index metric names - use Vec<u8> for blob parameters  
        let metric_names_key = "metric_names".as_bytes().to_vec();
        debug!("Metric names key bytes: {:?}", metric_names_key);
        for metric in metrics {
            debug!("Indexing metric name: '{}' (as string: '{}')", metric, metric.to_string());
            let index_query = "INSERT INTO string_index (key, column1, value) VALUES (?, ?, ?)";
            debug!("Executing index query: {} with key={:?}, column1='{}', value={:?}", 
                   index_query, metric_names_key, metric.to_string(), vec![0u8]);
            
            // Use direct CQL to bypass CDRS parameter encoding issues
            let direct_query = format!(
                "INSERT INTO string_index (key, column1, value) VALUES (0x{}, '{}', 0x00)",
                hex::encode(&metric_names_key),
                metric.to_string().replace("'", "''")  // Escape single quotes
            );
            debug!("Using direct CQL: {}", direct_query);
            self.execute_query(&direct_query).await?;
            debug!("Successfully indexed metric name: {}", metric);
        }
        
        // Index tag keys - use direct CQL to bypass CDRS encoding issues
        let tag_names_key = "tag_names".as_bytes();
        let tag_names_hex = hex::encode(tag_names_key);
        for tag_key in tag_keys {
            let direct_query = format!(
                "INSERT INTO string_index (key, column1, value) VALUES (0x{}, '{}', 0x00)",
                tag_names_hex,
                tag_key.to_string().replace("'", "''")
            );
            self.execute_query(&direct_query).await?;
        }
        
        // Index tag values - use direct CQL to bypass CDRS encoding issues
        let tag_values_key = "tag_values".as_bytes();
        let tag_values_hex = hex::encode(tag_values_key);
        for tag_value in tag_values {
            let direct_query = format!(
                "INSERT INTO string_index (key, column1, value) VALUES (0x{}, '{}', 0x00)",
                tag_values_hex,
                tag_value.to_string().replace("'", "''")
            );
            self.execute_query(&direct_query).await?;
        }
        
        debug!("Wrote indexes for {} metrics, {} tag keys, {} tag values", 
               metrics.len(), tag_keys.len(), tag_values.len());
        Ok(())
    }
    
    /// Write row key indexes to row_key_index table for query performance
    async fn write_row_key_indexes(&self, points: &[kairosdb_core::datapoint::DataPoint]) -> KairosResult<()> {
        for point in points {
            let metric_name_bytes = point.metric.as_str().as_bytes();
            let metric_name_hex = hex::encode(metric_name_bytes);
            
            // Create the row key for this data point
            let row_key = kairosdb_core::cassandra::RowKey::from_data_point(point);
            let row_key_bytes = row_key.to_bytes();
            let row_key_hex = hex::encode(&row_key_bytes);
            
            // Insert into row_key_index: key=metric_name, column1=row_key, value=empty
            let direct_query = format!(
                "INSERT INTO row_key_index (key, column1, value) VALUES (0x{}, 0x{}, 0x00)",
                metric_name_hex,
                row_key_hex
            );
            self.execute_query(&direct_query).await?;
        }
        
        debug!("Wrote row key indexes for {} points", points.len());
        Ok(())
    }
}

#[async_trait]
impl CassandraClient for CassandraClientImpl {
    async fn write_batch(&self, batch: &DataPointBatch) -> KairosResult<()> {
        if batch.points.is_empty() {
            return Ok(());
        }
        
        info!("Writing a batch of {} data points to Cassandra", batch.points.len());
        
        // Collect unique values for indexing (avoid duplicates in a batch)
        let mut unique_metrics = HashSet::new();
        let mut unique_tag_keys = HashSet::new();
        let mut unique_tag_values = HashSet::new();
        
        for point in &batch.points {
            // Write the data point
            self.write_data_point(point).await?;
            
            // Write row keys metadata
            self.write_row_key_metadata(point).await?;
            
            // Collect unique values for indexing
            unique_metrics.insert(point.metric.as_str());
            for (key, value) in point.tags.iter() {
                unique_tag_keys.insert(key.as_str());
                unique_tag_values.insert(value.as_str());
            }
        }
        
        // Write indexes for unique values in this batch
        self.write_indexes(&unique_metrics, &unique_tag_keys, &unique_tag_values).await?;
        
        // Write row key indexes for query performance
        self.write_row_key_indexes(&batch.points).await?;
        
        info!("Successfully wrote a batch of {} data points with indexes", batch.points.len());
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
        
        // Create row_keys table (for metric metadata) - Java KairosDB compatible
        let create_row_keys = r#"
            CREATE TABLE IF NOT EXISTS row_keys (
                metric text,
                table_name text, 
                row_time timestamp,
                data_type text,
                tags frozen<map<text, text>>,
                mtime timeuuid static,
                value text,
                PRIMARY KEY ((metric, table_name, row_time), data_type, tags)
            )
        "#;
        self.execute_query(create_row_keys).await?;
        
        // Create row_key_index table (Java KairosDB compatibility)
        let create_row_key_index = r#"
            CREATE TABLE IF NOT EXISTS row_key_index (
                key blob,
                column1 blob,
                value blob,
                PRIMARY KEY ((key), column1)
            )
        "#;
        self.execute_query(create_row_key_index).await?;
        
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