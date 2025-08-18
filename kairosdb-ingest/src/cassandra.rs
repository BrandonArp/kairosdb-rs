//! Cassandra client interface and implementations
//!
//! This module provides a clean abstraction for Cassandra operations with
//! both production and testing implementations.

use async_trait::async_trait;
use kairosdb_core::{datapoint::DataPointBatch, error::KairosResult};
use std::sync::Arc;

/// Statistics for Cassandra client operations
#[derive(Debug, Clone, Default)]
pub struct CassandraStats {
    pub total_queries: u64,
    pub failed_queries: u64,
    pub total_datapoints_written: u64,
    pub avg_batch_size: f64,
    pub connection_errors: u64,
}

/// Trait defining the interface for Cassandra clients
#[async_trait]
pub trait CassandraClient: Send + Sync {
    /// Write a batch of data points to Cassandra
    async fn write_batch(&self, batch: &DataPointBatch) -> KairosResult<()>;

    /// Perform a health check on the connection
    async fn health_check(&self) -> KairosResult<bool>;

    /// Get client statistics
    fn get_stats(&self) -> CassandraStats;

    /// Initialize/verify the KairosDB schema
    async fn ensure_schema(&self) -> KairosResult<()>;
}

/// Type alias for boxed client trait object
pub type BoxedCassandraClient = Arc<dyn CassandraClient>;
