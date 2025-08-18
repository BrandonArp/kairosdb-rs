//! Simplified Cassandra client compatible with current CDRS-tokio API
//!
//! This module provides a simplified Cassandra client that works with the current
//! version of CDRS-tokio, avoiding the complex connection management issues.

use anyhow::{Context, Result};
use cdrs_tokio::{
    authenticators::NoneAuthenticator,
    cluster::{TcpConnectionManager, session::Session},
    load_balancing::RoundRobinLoadBalancingStrategy,
    transport::TransportTcp,
    cluster::KeyspaceHolder,
    compression::Compression,
    frame::Version,
    frame_encoding::{FrameEncodingFactory, PlainFrameEncodingFactory},
    query_values,
};
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
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::config::CassandraConfig;

/// Simple statistics for the Cassandra client
#[derive(Debug, Clone, Default)]
pub struct SimpleStats {
    pub total_queries: u64,
    pub failed_queries: u64,
    pub avg_query_time_ms: u64,
}

/// Simplified Cassandra client that focuses on core functionality
pub struct SimpleCassandraClient {
    session: Session<TransportTcp, TcpConnectionManager, RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>>,
    config: Arc<CassandraConfig>,
    stats: Arc<AtomicU64>, // Simple query counter
}

impl SimpleCassandraClient {
    /// Create a new simplified Cassandra client
    pub async fn new(config: Arc<CassandraConfig>) -> Result<Self> {
        info!("Connecting to Cassandra with simplified client...");
        
        // Use the first contact point for simplicity
        let contact_point = config.contact_points.first()
            .ok_or_else(|| anyhow::anyhow!("No Cassandra contact points provided"))?;
        
        info!("Connecting to Cassandra at: {}", contact_point);
        
        // Parse the address:port to extract host and port
        let parts: Vec<&str> = contact_point.split(':').collect();
        let host = parts[0];
        let port = if parts.len() > 1 { 
            parts[1].parse().unwrap_or(9042) 
        } else { 
            9042 
        };
        
        // Create connection manager with updated API
        let (keyspace_tx, keyspace_rx) = tokio::sync::watch::channel(None);
        let keyspace_holder = Arc::new(KeyspaceHolder::new(keyspace_rx));
        let frame_encoder = Box::new(PlainFrameEncodingFactory) as Box<dyn FrameEncodingFactory + Send + Sync>;
        let compression = Compression::None;
        let tcp_nodelay = true;
        let protocol_version = Version::V4;
        
        let manager = TcpConnectionManager::new(
            (host, port),
            keyspace_holder,
            frame_encoder,
            compression,
            4, // Connection pool size
            tcp_nodelay,
            protocol_version,
        );
        
        let load_balancer = RoundRobinLoadBalancingStrategy::new();
        
        // Create session with updated API
        let session = Session::new(manager, load_balancer);
        
        info!("Successfully connected to Cassandra");
        
        // Initialize keyspace and tables
        let client = Self {
            session,
            config: config.clone(),
            stats: Arc::new(AtomicU64::new(0)),
        };
        
        client.ensure_keyspace_and_tables().await?;
        
        Ok(client)
    }
    
    /// Ensure keyspace and tables exist
    async fn ensure_keyspace_and_tables(&self) -> Result<()> {
        info!("Ensuring keyspace '{}' exists", self.config.keyspace);
        
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
        
        // Create data_points table (simplified version)
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
        
        // Create row_keys table (simplified version)  
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
        
        info!("Keyspace and tables initialized successfully");
        Ok(())
    }
    
    /// Execute a simple query
    async fn execute_query(&self, query: &str) -> Result<()> {
        let start = std::time::Instant::now();
        
        let result = timeout(
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
    
    /// Write a batch of data points (simplified implementation)
    pub async fn write_batch(&self, batch: &DataPointBatch) -> KairosResult<()> {
        if batch.points.is_empty() {
            return Ok(());
        }
        
        info!("Writing batch of {} data points", batch.points.len());
        
        // For simplicity, we'll insert into a basic table structure
        // In production, you'd want to use the full KairosDB schema
        for point in &batch.points {
            let insert_query = format!(
                "INSERT INTO data_points (key, column1, value) VALUES (textAsBlob('{}'), textAsBlob('{}'), textAsBlob('{:?}'))",
                point.metric.as_str(),
                point.timestamp.timestamp_millis(),
                point.value
            );
            
            if let Err(e) = self.execute_query(&insert_query).await {
                error!("Failed to insert data point: {}", e);
                return Err(KairosError::cassandra(format!("Insert failed: {}", e)));
            }
        }
        
        info!("Successfully wrote batch of {} data points", batch.points.len());
        Ok(())
    }
    
    /// Get simple health check
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
    
    /// Get simple statistics
    pub fn get_stats(&self) -> SimpleStats {
        SimpleStats {
            total_queries: self.stats.load(Ordering::Relaxed),
            failed_queries: 0, // Simplified for now
            avg_query_time_ms: 0, // Simplified for now
        }
    }
}