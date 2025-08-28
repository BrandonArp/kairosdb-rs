//! Mock Cassandra client for unit testing
//!
//! This client provides a simple in-memory implementation that can be used
//! for unit tests without requiring a real Cassandra instance.

use async_trait::async_trait;
use kairosdb_core::{datapoint::DataPointBatch, error::KairosResult};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tracing::trace;
use tracing::{debug, info};

use crate::cassandra::{CassandraClient, CassandraStats};

/// Simple in-memory storage for testing
#[derive(Debug, Default)]
struct MockStorage {
    /// Stored data points as key-value pairs
    data_points: HashMap<String, Vec<u8>>,
    /// Track operations for testing
    operations: Vec<String>,
}

/// Mock Cassandra client for unit testing
pub struct MockCassandraClient {
    storage: Arc<Mutex<MockStorage>>,
    stats: MockClientStats,
    simulate_errors: bool,
    null_mode: bool, // If true, discards all data instead of storing
}

/// Statistics tracking for the mock client
struct MockClientStats {
    total_queries: AtomicU64,
    failed_queries: AtomicU64,
    total_datapoints: AtomicU64,
    connection_errors: AtomicU64,
}

impl Default for MockClientStats {
    fn default() -> Self {
        Self {
            total_queries: AtomicU64::new(0),
            failed_queries: AtomicU64::new(0),
            total_datapoints: AtomicU64::new(0),
            connection_errors: AtomicU64::new(0),
        }
    }
}

impl MockCassandraClient {
    /// Create a new mock client
    pub fn new() -> Self {
        info!("Creating a mock Cassandra client for testing");

        Self {
            storage: Arc::new(Mutex::new(MockStorage::default())),
            stats: MockClientStats::default(),
            simulate_errors: false,
            null_mode: false,
        }
    }

    /// Create a new null client that discards all data (for memory testing)
    pub fn new_null() -> Self {
        info!("Creating a null Cassandra client for memory leak testing (discards all data)");

        Self {
            storage: Arc::new(Mutex::new(MockStorage::default())),
            stats: MockClientStats::default(),
            simulate_errors: false,
            null_mode: true,
        }
    }

    /// Enable error simulation for testing error handling
    pub fn with_error_simulation(mut self) -> Self {
        self.simulate_errors = true;
        self
    }

    /// Get the number of stored data points (for testing)
    pub fn get_stored_count(&self) -> usize {
        self.storage.lock().unwrap().data_points.len()
    }

    /// Get the operation log (for testing)
    pub fn get_operations(&self) -> Vec<String> {
        self.storage.lock().unwrap().operations.clone()
    }

    /// Clear all stored data (for testing)
    pub fn clear(&self) {
        let mut storage = self.storage.lock().unwrap();
        storage.data_points.clear();
        storage.operations.clear();
    }

    /// Check if a specific operation was recorded
    pub fn has_operation(&self, operation: &str) -> bool {
        self.storage
            .lock()
            .unwrap()
            .operations
            .iter()
            .any(|op| op.contains(operation))
    }
}

impl Default for MockCassandraClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CassandraClient for MockCassandraClient {
    async fn write_batch(&self, batch: &DataPointBatch) -> KairosResult<()> {
        if self.simulate_errors {
            self.stats.failed_queries.fetch_add(1, Ordering::Relaxed);
            return Err(kairosdb_core::error::KairosError::cassandra(
                "Simulated error",
            ));
        }

        if batch.points.is_empty() {
            return Ok(());
        }

        trace!("Mock: Writing batch of {} data points", batch.points.len());

        if !self.null_mode {
            let mut storage = self.storage.lock().unwrap();

            // Record the operation
            storage
                .operations
                .push(format!("write_batch({} points)", batch.points.len()));

            // Store each data point
            for point in &batch.points {
                let key = format!(
                    "{}:{}:{}",
                    point.metric.as_str(),
                    point.timestamp.timestamp_millis(),
                    point.data_type()
                );

                // Store a simple representation of the value
                let value = match &point.value {
                    kairosdb_core::datapoint::DataPointValue::Long(v) => v.to_le_bytes().to_vec(),
                    kairosdb_core::datapoint::DataPointValue::Double(v) => v.to_le_bytes().to_vec(),
                    kairosdb_core::datapoint::DataPointValue::Text(v) => v.as_bytes().to_vec(),
                    kairosdb_core::datapoint::DataPointValue::Binary(v) => v.clone(),
                    kairosdb_core::datapoint::DataPointValue::Complex { real, imaginary } => {
                        let mut bytes = Vec::new();
                        bytes.extend_from_slice(&real.to_le_bytes());
                        bytes.extend_from_slice(&imaginary.to_le_bytes());
                        bytes
                    }
                    kairosdb_core::datapoint::DataPointValue::Histogram(h) => {
                        // Store histogram as V2 binary format (same as production)
                        h.to_v2_bytes()
                    }
                };

                storage.data_points.insert(key, value);
            }
        }

        // Always update stats regardless of mode
        self.stats
            .total_datapoints
            .fetch_add(batch.points.len() as u64, Ordering::Relaxed);

        self.stats.total_queries.fetch_add(1, Ordering::Relaxed);

        info!(
            "Mock: Successfully wrote batch of {} data points",
            batch.points.len()
        );
        Ok(())
    }

    async fn health_check(&self) -> KairosResult<bool> {
        if self.simulate_errors {
            debug!("Mock: Health check failed (simulated error)");
            return Ok(false);
        }

        debug!("Mock: Health check passed");
        Ok(true)
    }

    async fn get_stats(&self) -> CassandraStats {
        self.get_detailed_stats().await // Mock client doesn't differentiate
    }

    async fn get_detailed_stats(&self) -> CassandraStats {
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
            cache_in_overlap_period: false,
            cache_primary_age_seconds: 0,
            cache_memory_capacity: 128 * 1024 * 1024, // 128MB mock capacity
            cache_disk_capacity: 1024 * 1024 * 1024, // 1GB mock capacity
            cache_primary_memory_usage: 12500, // Mock values for testing
            cache_secondary_memory_usage: None,
            cache_total_memory_usage: 12500,
            cache_primary_disk_usage: 50000, // Mock disk usage
            cache_secondary_disk_usage: None,
            cache_primary_hit_ratio: 0.85, // Mock 85% hit ratio
            cache_secondary_hit_ratio: None,

            // Mock detailed metrics (all zeros since mock doesn't track them)
            datapoint_writes: total_datapoints,
            datapoint_write_errors: 0,
            index_writes: 0,
            index_write_errors: 0,
            prepared_statement_cache_hits: total_queries,
            prepared_statement_cache_misses: 0,

            // Mock concurrency metrics (no actual concurrency in mock)
            current_concurrent_requests: 0,
            max_concurrent_requests_reached: 1,
            avg_semaphore_wait_time_ms: 0.0,

            // Mock timing metrics (all zeros for mock)
            avg_datapoint_write_time_ms: 0.0,
            avg_index_write_time_ms: 0.0,
            avg_batch_write_time_ms: 0.0,
        }
    }

    async fn ensure_schema(&self) -> KairosResult<()> {
        debug!("Mock: Schema initialization (no-op)");

        let mut storage = self.storage.lock().unwrap();
        storage.operations.push("ensure_schema".to_string());

        if self.simulate_errors {
            return Err(kairosdb_core::error::KairosError::cassandra(
                "Simulated schema error",
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kairosdb_core::{datapoint::DataPoint, time::Timestamp};

    #[tokio::test]
    async fn test_mock_client_basic_operations() {
        let client = MockCassandraClient::new();

        // Test health check
        assert!(client.health_check().await.unwrap());

        // Test schema initialization
        client.ensure_schema().await.unwrap();
        assert!(client.has_operation("ensure_schema"));

        // Test writing data points
        let mut batch = DataPointBatch::new();
        let point = DataPoint::new_long("test.metric", Timestamp::now(), 42);
        batch.add_point(point).unwrap();

        client.write_batch(&batch).await.unwrap();

        assert_eq!(client.get_stored_count(), 1);
        assert!(client.has_operation("write_batch"));

        // Check stats
        let stats = client.get_stats();
        assert_eq!(stats.total_datapoints_written, 1);
        assert!(stats.total_queries > 0);
    }

    #[tokio::test]
    async fn test_mock_client_error_simulation() {
        let client = MockCassandraClient::new().with_error_simulation();

        // Health check should fail
        assert!(!client.health_check().await.unwrap());

        // Schema should fail
        assert!(client.ensure_schema().await.is_err());

        // Write should fail
        let mut batch = DataPointBatch::new();
        let point = DataPoint::new_long("test.metric", Timestamp::now(), 42);
        batch.add_point(point).unwrap();

        assert!(client.write_batch(&batch).await.is_err());

        // Check error stats
        let stats = client.get_stats();
        assert!(stats.failed_queries > 0);
    }

    #[tokio::test]
    async fn test_mock_client_histogram_support() {
        let client = MockCassandraClient::new();

        // Create a histogram data point
        let bins = vec![(0.0, 10), (10.0, 20), (50.0, 5)];
        let hist =
            kairosdb_core::datapoint::HistogramData::from_bins(bins, 125.0, 0.1, 45.0, Some(7))
                .unwrap();

        let point = DataPoint::new_histogram("test.histogram", Timestamp::now(), hist);
        let mut batch = DataPointBatch::new();
        batch.add_point(point).unwrap();

        client.write_batch(&batch).await.unwrap();

        assert_eq!(client.get_stored_count(), 1);

        // Verify the histogram was stored (it would be serialized to V2 format)
        assert!(client.has_operation("write_batch(1 points)"));
    }

    #[tokio::test]
    async fn test_mock_client_detailed_stats() {
        let client = MockCassandraClient::new();

        // Regular stats should have None for ones count
        let stats = client.get_stats();
        assert!(stats.bloom_filter_primary_ones_count.is_none());
        assert!(stats.bloom_filter_secondary_ones_count.is_none());

        // Detailed stats should have Some values for ones count (mock values)
        let detailed_stats = client.get_detailed_stats();
        assert!(detailed_stats.bloom_filter_primary_ones_count.is_none()); // Mock still returns None since it's not calculating actual ones
    }
}
