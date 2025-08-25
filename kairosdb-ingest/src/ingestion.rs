//! High-performance ingestion service with async batch processing
//!
//! This module provides the core ingestion service that handles:
//! - Async batch processing with configurable limits
//! - Backpressure handling when memory limits are reached
//! - Connection pooling and error recovery
//! - Metrics collection and monitoring

use anyhow::{Context, Result};
use kairosdb_core::{
    datapoint::DataPointBatch,
    error::{KairosError, KairosResult},
};
use parking_lot::RwLock;
use prometheus::{register_counter, register_gauge, register_histogram, Counter, Gauge, Histogram};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};
use tracing::{error, info, trace, warn};

use crate::{
    cassandra::{BoxedCassandraClient, CassandraClient},
    config::IngestConfig,
    mock_client::MockCassandraClient,
    multi_writer_client::MultiWorkerCassandraClient,
    persistent_queue::PersistentQueue,
};

/// Comprehensive metrics for monitoring ingestion service
#[derive(Debug, Clone)]
pub struct IngestionMetrics {
    /// Total data points successfully ingested
    pub datapoints_ingested: Arc<AtomicU64>,
    /// Total batches processed
    pub batches_processed: Arc<AtomicU64>,
    /// Total ingestion errors
    pub ingestion_errors: Arc<AtomicU64>,
    /// Total validation errors
    pub validation_errors: Arc<AtomicU64>,
    /// Total Cassandra errors
    pub cassandra_errors: Arc<AtomicU64>,
    /// Current memory usage in bytes
    pub memory_usage: Arc<AtomicU64>,
    /// Average processing time per batch (ms)
    pub avg_batch_time_ms: Arc<AtomicU64>,
    /// Last batch processing time
    pub last_batch_time: Arc<RwLock<Option<Instant>>>,
    /// Prometheus metrics
    pub prometheus_metrics: PrometheusMetrics,
}

/// Prometheus metrics for external monitoring
#[derive(Debug, Clone)]
pub struct PrometheusMetrics {
    pub datapoints_total: Counter,
    pub batches_total: Counter,
    pub errors_total: Counter,
    pub memory_usage_gauge: Gauge,
    pub batch_duration_histogram: Histogram,
}

/// Main ingestion service that handles data point processing
pub struct IngestionService {
    /// Configuration
    config: Arc<IngestConfig>,

    /// Persistent queue for write-ahead logging
    persistent_queue: Arc<PersistentQueue>,

    /// Cassandra client for data persistence
    cassandra_client: BoxedCassandraClient,

    /// Metrics collection
    metrics: IngestionMetrics,

    /// Backpressure handler
    backpressure_active: Arc<AtomicU64>,
}

impl IngestionService {
    /// Create a new ingestion service
    pub async fn new(config: Arc<IngestConfig>) -> Result<Self> {
        config.validate().context("Invalid configuration")?;

        info!("Initializing ingestion service");

        // Initialize persistent queue for write-ahead logging
        let queue_dir = std::path::Path::new("./data/queue");
        let persistent_queue = Arc::new(
            PersistentQueue::new(queue_dir)
                .await
                .context("Failed to initialize persistent queue")?
        );

        // Initialize Cassandra client based on environment
        let cassandra_client: BoxedCassandraClient = if std::env::var("USE_MOCK_CASSANDRA")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false)
        {
            info!("Using mock Cassandra client for testing");
            Arc::new(MockCassandraClient::new())
        } else {
            info!("Using production multi-worker Cassandra client");
            let num_workers = config.cassandra.max_concurrent_requests;
            let client = MultiWorkerCassandraClient::new(config.cassandra.clone(), num_workers)
                .await
                .context("Failed to initialize multi-worker Cassandra client")?;

            // Ensure schema exists
            info!("Ensuring KairosDB schema exists");
            client
                .ensure_schema()
                .await
                .context("Failed to ensure Cassandra schema")?;

            info!("Multi-worker Cassandra client ready with {} workers", 
                  num_workers.unwrap_or(50));

            Arc::new(client)
        };

        // Initialize Prometheus metrics
        let prometheus_metrics = PrometheusMetrics::new()?;

        // Initialize ingestion metrics
        let metrics = IngestionMetrics {
            datapoints_ingested: Arc::new(AtomicU64::new(0)),
            batches_processed: Arc::new(AtomicU64::new(0)),
            ingestion_errors: Arc::new(AtomicU64::new(0)),
            validation_errors: Arc::new(AtomicU64::new(0)),
            cassandra_errors: Arc::new(AtomicU64::new(0)),
            memory_usage: Arc::new(AtomicU64::new(0)),
            avg_batch_time_ms: Arc::new(AtomicU64::new(0)),
            last_batch_time: Arc::new(RwLock::new(None)),
            prometheus_metrics,
        };

        // Initialize backpressure tracking
        let backpressure_active = Arc::new(AtomicU64::new(0));

        let service = Self {
            config: config.clone(),
            persistent_queue,
            cassandra_client,
            metrics,
            backpressure_active,
        };

        // Start background queue processing task
        info!("About to start queue processor task");
        let _queue_processor = service.start_queue_processor().await;
        info!("Queue processor task started successfully");
        
        info!(
            "Ingestion service initialized with {} worker threads and persistent queue processor",
            config.ingestion.worker_threads
        );
        Ok(service)
    }

    /// Create a new ingestion service with mock client for testing
    #[cfg(test)]
    pub async fn new_with_mock(config: Arc<IngestConfig>) -> Result<Self> {
        config.validate().context("Invalid configuration")?;

        info!("Initializing ingestion service with mock client for testing");

        // Initialize test persistent queue
        let queue_dir = tempfile::tempdir()?.path().to_path_buf();
        let persistent_queue = Arc::new(
            PersistentQueue::new(queue_dir)
                .await
                .context("Failed to initialize test persistent queue")?
        );

        // Use mock client for testing
        let cassandra_client: BoxedCassandraClient = Arc::new(MockCassandraClient::new());

        // Initialize Prometheus metrics
        let prometheus_metrics = PrometheusMetrics::new()?;

        // Initialize ingestion metrics
        let metrics = IngestionMetrics {
            datapoints_ingested: Arc::new(AtomicU64::new(0)),
            batches_processed: Arc::new(AtomicU64::new(0)),
            ingestion_errors: Arc::new(AtomicU64::new(0)),
            validation_errors: Arc::new(AtomicU64::new(0)),
            cassandra_errors: Arc::new(AtomicU64::new(0)),
            memory_usage: Arc::new(AtomicU64::new(0)),
            avg_batch_time_ms: Arc::new(AtomicU64::new(0)),
            last_batch_time: Arc::new(RwLock::new(None)),
            prometheus_metrics,
        };

        let backpressure_active = Arc::new(AtomicU64::new(0));

        let service = Self {
            config,
            persistent_queue,
            cassandra_client,
            metrics,
            backpressure_active,
        };

        // Start background queue processing task for testing
        let _queue_processor = service.start_queue_processor().await;
        
        info!("Test ingestion service initialized with mock client and persistent queue processor");
        Ok(service)
    }

    /// Submit a batch for ingestion with write-ahead logging (fire-and-forget)
    /// Uses the default sync setting from configuration
    pub fn ingest_batch(&self, batch: DataPointBatch) -> KairosResult<()> {
        self.ingest_batch_with_sync(batch, self.config.ingestion.default_sync)
    }

    /// Submit a batch for ingestion with optional sync control
    pub fn ingest_batch_with_sync(&self, batch: DataPointBatch, sync_to_disk: bool) -> KairosResult<()> {
        let start = Instant::now();

        // Check disk usage for backpressure (same as ingest_batch)
        let disk_usage = self.persistent_queue.get_disk_usage_bytes()
            .unwrap_or(0);
        let max_disk_bytes = self.config.ingestion.max_queue_disk_bytes;

        let current_queue_size = self.persistent_queue.size();
        let max_queue_size = self.config.ingestion.max_queue_size as u64;

        trace!(
            "Queue check (sync={}): items={}/{}, disk={:.2}MB/{:.2}MB",
            sync_to_disk,
            current_queue_size, max_queue_size,
            disk_usage as f64 / 1_048_576.0,
            max_disk_bytes as f64 / 1_048_576.0
        );

        self.persistent_queue.metrics().disk_usage_bytes.set(disk_usage as f64);

        // Check disk usage limit first
        if disk_usage > max_disk_bytes {
            self.backpressure_active.store(1, Ordering::Relaxed);
            warn!(
                "Queue disk usage limit exceeded: {:.2}MB > {:.2}MB, activating backpressure",
                disk_usage as f64 / 1_048_576.0,
                max_disk_bytes as f64 / 1_048_576.0
            );
            return Err(KairosError::rate_limit(
                format!("Queue disk usage limit exceeded: {:.2}MB", disk_usage as f64 / 1_048_576.0)
            ));
        }

        // Check item count limit
        if current_queue_size > max_queue_size {
            self.backpressure_active.store(1, Ordering::Relaxed);
            warn!(
                "Queue item count limit exceeded: {} > {}, activating backpressure",
                current_queue_size, max_queue_size
            );
            return Err(KairosError::rate_limit(
                format!("Queue size limit exceeded: {} items", current_queue_size)
            ));
        }

        // Validate batch if validation is enabled
        if self.config.ingestion.enable_validation {
            if let Err(e) = batch.validate_self() {
                self.metrics.validation_errors.fetch_add(1, Ordering::Relaxed);
                return Err(e);
            }
        }

        // Write batch to persistent queue with sync control
        if let Err(e) = self.persistent_queue.enqueue_batch_with_sync(batch.clone(), sync_to_disk) {
            error!("Failed to write batch to persistent queue: {}", e);
            self.metrics.ingestion_errors.fetch_add(1, Ordering::Relaxed);
            return Err(e);
        }

        // Update success metrics
        self.metrics.datapoints_ingested.fetch_add(batch.points.len() as u64, Ordering::Relaxed);
        self.metrics.batches_processed.fetch_add(1, Ordering::Relaxed);
        *self.metrics.last_batch_time.write() = Some(Instant::now());

        let elapsed = start.elapsed();
        self.metrics.avg_batch_time_ms.store(elapsed.as_millis() as u64, Ordering::Relaxed);

        trace!(
            "Successfully enqueued batch of {} points (sync={}) in {:?}",
            batch.points.len(),
            sync_to_disk,
            elapsed
        );

        Ok(())
    }

    /// Start the background queue processor task
    async fn start_queue_processor(&self) -> tokio::task::JoinHandle<()> {
        let persistent_queue = self.persistent_queue.clone();
        let cassandra_client = self.cassandra_client.clone();
        let config = self.config.clone();
        let metrics = self.metrics.clone();
        
        tokio::spawn(async move {
            info!("Persistent queue processor task spawned successfully");
            
            let interval_duration = std::time::Duration::from_millis(config.ingestion.batch_timeout_ms);
            info!("Creating interval timer with duration: {:?}", interval_duration);
            let mut interval = tokio::time::interval(interval_duration);
            
            info!("Starting persistent queue processor main loop with {}ms interval", config.ingestion.batch_timeout_ms);
            
            loop {
                // Continuously drain the queue until empty using new status tracking
                let mut batches_processed_this_cycle = 0;
                let current_queue_size = persistent_queue.size();
                if current_queue_size > 0 {
                    info!("Starting queue processing cycle, queue size: {}", current_queue_size);
                } else {
                    trace!("Starting queue processing cycle, queue size: {}", current_queue_size);
                }
                loop {
                    // Peek at the next work item without removing it
                    match persistent_queue.peek_next_work_item() {
                        Ok(Some(work_item)) => {
                            let batch_size = work_item.entry.batch.points.len();
                            trace!("Processing work item with {} points from queue", batch_size);
                            batches_processed_this_cycle += 1;
                            
                            // Write to Cassandra with timing
                            let cassandra_start = std::time::Instant::now();
                            trace!("Writing batch of {} points to Cassandra", batch_size);
                            
                            match cassandra_client.write_batch(&work_item.entry.batch).await {
                                Ok(_) => {
                                    let cassandra_duration = cassandra_start.elapsed();
                                    trace!("Successfully wrote batch of {} points to Cassandra in {:?}", batch_size, cassandra_duration);
                                    
                                    // Only remove from queue after successful Cassandra write
                                    match persistent_queue.remove_processed_item(&work_item.queue_key) {
                                        Ok(_) => {
                                            trace!("Successfully removed processed item from queue");
                                            // Update success metrics
                                            metrics.datapoints_ingested.fetch_add(batch_size as u64, Ordering::Relaxed);
                                        }
                                        Err(e) => {
                                            error!("Failed to remove processed item from queue (data was written to Cassandra): {}", e);
                                            // This is concerning but not fatal - Cassandra write succeeded
                                        }
                                    }
                                }
                                Err(e) => {
                                    let cassandra_duration = cassandra_start.elapsed();
                                    error!("Failed to write batch to Cassandra after {:?}: {}", cassandra_duration, e);
                                    metrics.cassandra_errors.fetch_add(1, Ordering::Relaxed);
                                    
                                    // Item stays in queue for retry - implement exponential backoff here
                                    warn!("Item remains in queue for retry (queue key: {})", work_item.queue_key);
                                    
                                    // Break to avoid tight retry loop - will retry on next interval
                                    break;
                                }
                            }
                        }
                        Ok(None) => {
                            // Queue is empty, break from inner loop
                            if batches_processed_this_cycle > 0 {
                                info!("Queue drained: processed {} batches this cycle", batches_processed_this_cycle);
                            } else {
                                trace!("Persistent queue is empty");
                            }
                            break;
                        }
                        Err(e) => {
                            error!("Failed to peek at next work item from persistent queue: {}", e);
                            metrics.ingestion_errors.fetch_add(1, Ordering::Relaxed);
                            break; // Exit inner loop on error
                        }
                    }
                }
                
                // Queue metrics are tracked by PersistentQueue internally
                
                // Update disk usage metric periodically
                if let Ok(disk_usage) = persistent_queue.get_disk_usage_bytes() {
                    persistent_queue.metrics().disk_usage_bytes.set(disk_usage as f64);
                }
                
                // Wait before checking queue again (queue is empty or error occurred)
                if batches_processed_this_cycle > 0 {
                    info!("Queue processing cycle complete, waiting for next interval tick");
                } else {
                    trace!("Queue processing cycle complete, waiting for next interval tick");
                }
                interval.tick().await;
                if persistent_queue.size() > 0 {
                    info!("Interval tick received, starting next cycle");
                } else {
                    trace!("Interval tick received, starting next cycle");
                }
            }
            
            error!("Queue processor task terminated unexpectedly!");
        })
    }

    /// Get current metrics snapshot
    pub fn get_metrics_snapshot(&self) -> IngestionMetricsSnapshot {
        IngestionMetricsSnapshot {
            datapoints_ingested: self.metrics.datapoints_ingested.load(Ordering::Relaxed),
            batches_processed: self.metrics.batches_processed.load(Ordering::Relaxed),
            ingestion_errors: self.metrics.ingestion_errors.load(Ordering::Relaxed),
            validation_errors: self.metrics.validation_errors.load(Ordering::Relaxed),
            cassandra_errors: self.metrics.cassandra_errors.load(Ordering::Relaxed),
            queue_size: self.persistent_queue.size() as usize,
            memory_usage: self.metrics.memory_usage.load(Ordering::Relaxed),
            avg_batch_time_ms: self.metrics.avg_batch_time_ms.load(Ordering::Relaxed),
            last_batch_time: *self.metrics.last_batch_time.read(),
            backpressure_active: self.backpressure_active.load(Ordering::Relaxed) > 0,
        }
    }

    /// Health check for the ingestion service
    pub async fn health_check(&self) -> Result<HealthStatus> {
        let cassandra_healthy = self.cassandra_client.health_check().await.unwrap_or(false);
        let backpressure_active = self.backpressure_active.load(Ordering::Relaxed) > 0;
        let queue_size = self.persistent_queue.size() as usize;
        let max_queue_size = self.config.ingestion.max_queue_size;

        let status = if cassandra_healthy && !backpressure_active && queue_size < max_queue_size {
            HealthStatus::Healthy
        } else if cassandra_healthy {
            HealthStatus::Degraded
        } else {
            HealthStatus::Unhealthy
        };

        Ok(status)
    }
}

impl PrometheusMetrics {
    /// Create new Prometheus metrics
    pub fn new() -> Result<Self> {
        Self::new_with_prefix("")
    }

    /// Create new Prometheus metrics with a prefix (useful for testing)
    pub fn new_with_prefix(prefix: &str) -> Result<Self> {
        let suffix = if prefix.is_empty() {
            String::new()
        } else {
            format!("_{}", prefix)
        };

        let datapoints_total = register_counter!(
            format!("kairosdb_datapoints_ingested_total{}", suffix),
            "Total number of data points ingested"
        )
        .unwrap_or_else(|_| {
            // If registration fails (e.g., in tests), use a default counter
            prometheus::Counter::new("test_counter", "test").unwrap()
        });

        let batches_total = register_counter!(
            format!("kairosdb_batches_processed_total{}", suffix),
            "Total number of batches processed"
        )
        .unwrap_or_else(|_| prometheus::Counter::new("test_counter2", "test").unwrap());

        let errors_total = register_counter!(
            format!("kairosdb_ingestion_errors_total{}", suffix),
            "Total number of ingestion errors"
        )
        .unwrap_or_else(|_| prometheus::Counter::new("test_counter3", "test").unwrap());

        let memory_usage_gauge = register_gauge!(
            format!("kairosdb_memory_usage_bytes{}", suffix),
            "Current memory usage in bytes"
        )
        .unwrap_or_else(|_| prometheus::Gauge::new("test_gauge2", "test").unwrap());

        let batch_duration_histogram = register_histogram!(
            format!("kairosdb_batch_duration_seconds{}", suffix),
            "Batch processing duration"
        )
        .unwrap_or_else(|_| {
            prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
                "test_histogram",
                "test",
            ))
            .unwrap()
        });

        Ok(Self {
            datapoints_total,
            batches_total,
            errors_total,
            memory_usage_gauge,
            batch_duration_histogram,
        })
    }
}

impl Default for IngestionMetrics {
    fn default() -> Self {
        use std::sync::atomic::AtomicU32;
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);

        Self {
            datapoints_ingested: Arc::new(AtomicU64::new(0)),
            batches_processed: Arc::new(AtomicU64::new(0)),
            ingestion_errors: Arc::new(AtomicU64::new(0)),
            validation_errors: Arc::new(AtomicU64::new(0)),
            cassandra_errors: Arc::new(AtomicU64::new(0)),
            memory_usage: Arc::new(AtomicU64::new(0)),
            avg_batch_time_ms: Arc::new(AtomicU64::new(0)),
            last_batch_time: Arc::new(RwLock::new(None)),
            prometheus_metrics: PrometheusMetrics::new_with_prefix(&format!("test_{}", id))
                .unwrap(),
        }
    }
}

/// Snapshot of metrics for API responses
#[derive(Debug, Clone, serde::Serialize)]
pub struct IngestionMetricsSnapshot {
    pub datapoints_ingested: u64,
    pub batches_processed: u64,
    pub ingestion_errors: u64,
    pub validation_errors: u64,
    pub cassandra_errors: u64,
    pub queue_size: usize,
    pub memory_usage: u64,
    pub avg_batch_time_ms: u64,
    #[serde(skip)]
    pub last_batch_time: Option<Instant>,
    pub backpressure_active: bool,
}

/// Health status for the service
#[derive(Debug, Clone, serde::Serialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ingestion_service_creation() {
        // Set environment variable to use mock Cassandra client
        std::env::set_var("USE_MOCK_CASSANDRA", "true");

        let config = Arc::new(IngestConfig::default());
        let service = IngestionService::new(config).await;
        assert!(service.is_ok());

        // Clean up environment variable
        std::env::remove_var("USE_MOCK_CASSANDRA");
    }

    #[tokio::test]
    async fn test_batch_ingestion() {
        use kairosdb_core::{datapoint::DataPoint, time::Timestamp};

        // Set environment variable to use mock Cassandra client
        std::env::set_var("USE_MOCK_CASSANDRA", "true");

        let mut config = IngestConfig::default();
        // Set high queue size limit for testing to avoid triggering backpressure
        config.ingestion.max_queue_size = 100000;
        let config = Arc::new(config);
        let service = IngestionService::new(config).await.unwrap();

        let mut batch = DataPointBatch::new();
        batch
            .add_point(DataPoint::new_long("test.metric", Timestamp::now(), 42))
            .unwrap();

        let result = service.ingest_batch(batch);
        assert!(result.is_ok(), "Batch ingestion failed: {:?}", result.err());

        let metrics = service.get_metrics_snapshot();
        assert_eq!(metrics.batches_processed, 1);
        assert_eq!(metrics.datapoints_ingested, 1);

        // Clean up environment variable
        std::env::remove_var("USE_MOCK_CASSANDRA");
    }

    #[test]
    fn test_health_status_serialization() {
        let status = HealthStatus::Healthy;
        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("Healthy"));
    }

    #[test]
    fn test_metrics_snapshot() {
        let metrics = IngestionMetrics::default();
        // This would normally require actual metrics values
        assert_eq!(metrics.datapoints_ingested.load(Ordering::Relaxed), 0);
    }
}
