//! Mock Cassandra client for development and testing
//!
//! This provides a simple mock implementation that allows the ingestion service
//! to compile and run without requiring a real Cassandra connection.

use anyhow::Result;
use kairosdb_core::{
    datapoint::{DataPoint, DataPointBatch},
    error::{KairosError, KairosResult},
};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::config::CassandraConfig;

/// Simple statistics for the mock client
#[derive(Debug, Clone, Default)]
pub struct MockStats {
    pub total_queries: u64,
    pub failed_queries: u64,
    pub avg_query_time_ms: u64,
}

/// Mock Cassandra client that simulates operations without a real database
pub struct MockCassandraClient {
    config: Arc<CassandraConfig>,
    stats: Arc<AtomicU64>,
    is_connected: bool,
}

impl MockCassandraClient {
    /// Create a new mock Cassandra client
    pub async fn new(config: Arc<CassandraConfig>) -> Result<Self> {
        info!("Creating mock Cassandra client...");
        
        // Simulate connection delay
        sleep(Duration::from_millis(100)).await;
        
        info!("Mock Cassandra client connected successfully");
        
        Ok(Self {
            config,
            stats: Arc::new(AtomicU64::new(0)),
            is_connected: true,
        })
    }
    
    /// Write a batch of data points (mock implementation)
    pub async fn write_batch(&self, batch: &DataPointBatch) -> KairosResult<()> {
        if batch.points.is_empty() {
            return Ok(());
        }
        
        debug!("Mock writing batch of {} data points", batch.points.len());
        
        // Simulate processing time
        let processing_time = Duration::from_millis(batch.points.len() as u64 / 10);
        sleep(processing_time).await;
        
        // Increment stats
        self.stats.fetch_add(1, Ordering::Relaxed);
        
        info!("Mock successfully wrote batch of {} data points", batch.points.len());
        Ok(())
    }
    
    /// Get mock health check
    pub async fn health_check(&self) -> KairosResult<bool> {
        debug!("Mock Cassandra health check");
        
        // Simulate health check delay
        sleep(Duration::from_millis(10)).await;
        
        if self.is_connected {
            debug!("Mock Cassandra health check passed");
            Ok(true)
        } else {
            warn!("Mock Cassandra health check failed");
            Ok(false)
        }
    }
    
    /// Get mock statistics
    pub fn get_stats(&self) -> MockStats {
        MockStats {
            total_queries: self.stats.load(Ordering::Relaxed),
            failed_queries: 0,
            avg_query_time_ms: 50, // Mock value
        }
    }
    
    /// Simulate connection failure for testing
    pub fn simulate_failure(&mut self) {
        self.is_connected = false;
    }
    
    /// Restore connection for testing
    pub fn restore_connection(&mut self) {
        self.is_connected = true;
    }
}