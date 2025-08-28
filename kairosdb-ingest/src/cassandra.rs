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
    pub cache_in_overlap_period: bool,
    pub cache_primary_age_seconds: u64,
    pub cache_memory_capacity: u64,
    pub cache_disk_capacity: u64,
    pub cache_primary_memory_usage: u64,
    pub cache_secondary_memory_usage: Option<u64>,
    pub cache_total_memory_usage: u64,
    pub cache_primary_disk_usage: u64,
    pub cache_secondary_disk_usage: Option<u64>,
    pub cache_primary_hit_ratio: f64,
    pub cache_secondary_hit_ratio: Option<f64>,

    // Detailed Cassandra operation metrics
    pub datapoint_writes: u64,
    pub datapoint_write_errors: u64,
    pub index_writes: u64,
    pub index_write_errors: u64,
    pub prepared_statement_cache_hits: u64,
    pub prepared_statement_cache_misses: u64,

    // Concurrency metrics
    pub current_concurrent_requests: u64,
    pub max_concurrent_requests_reached: u64,
    pub avg_semaphore_wait_time_ms: f64,

    // Timing metrics (averaged per operation in milliseconds)
    pub avg_datapoint_write_time_ms: f64,
    pub avg_index_write_time_ms: f64,
    pub avg_batch_write_time_ms: f64,
}

/// Trait defining the interface for Cassandra clients
#[async_trait]
pub trait CassandraClient: Send + Sync {
    /// Write a batch of data points to Cassandra
    async fn write_batch(&self, batch: &DataPointBatch) -> KairosResult<()>;

    /// Perform a health check on the connection
    async fn health_check(&self) -> KairosResult<bool>;

    /// Get client statistics
    async fn get_stats(&self) -> CassandraStats;

    /// Get detailed client statistics with expensive calculations (for health checks)
    async fn get_detailed_stats(&self) -> CassandraStats;

    /// Initialize/verify the KairosDB schema
    async fn ensure_schema(&self) -> KairosResult<()>;
}

/// Type alias for boxed client trait object
pub type BoxedCassandraClient = Arc<dyn CassandraClient>;
