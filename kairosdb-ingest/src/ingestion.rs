//! High-performance ingestion service with async batch processing
//!
//! This module provides the core ingestion service that handles:
//! - Async batch processing with configurable limits
//! - Backpressure handling when memory limits are reached
//! - Connection pooling and error recovery
//! - Metrics collection and monitoring

use anyhow::{Context, Result};
// Removed unused imports
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
    multi_writer_client::{MultiWorkerCassandraClient, WorkItem, WorkResponse},
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
    
    /// Direct channel to Cassandra workers (bypasses client interface)
    cassandra_work_tx: Option<flume::Sender<WorkItem>>,
    
    /// Response channel from Cassandra workers
    cassandra_response_rx: Option<flume::Receiver<WorkResponse>>,

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

        // Always use multi-worker Cassandra client with bidirectional channels
        info!("Using production multi-worker Cassandra client with bidirectional channels");
        let num_workers = config.cassandra.max_concurrent_requests;
        
        // Create shared MPMC channels for bidirectional communication
        let (work_tx, work_rx) = flume::bounded::<WorkItem>(1000);
        let (response_tx, response_rx) = flume::bounded::<WorkResponse>(1000);
        
        let cassandra_client = MultiWorkerCassandraClient::new_with_channels(
            config.cassandra.clone(), 
            num_workers,
            work_tx.clone(),
            work_rx,
            response_tx,
            response_rx.clone()
        )
        .await
        .context("Failed to initialize multi-worker Cassandra client")?;

        // Ensure schema exists
        info!("Ensuring KairosDB schema exists");
        cassandra_client
            .ensure_schema()
            .await
            .context("Failed to ensure Cassandra schema")?;

        info!("Multi-worker Cassandra client ready with {} workers and bidirectional channels", 
              num_workers.unwrap_or(200));

        let cassandra_client = Arc::new(cassandra_client) as BoxedCassandraClient;
        let cassandra_work_tx = work_tx;
        let cassandra_response_rx = response_rx;

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
            cassandra_work_tx: Some(cassandra_work_tx),
            cassandra_response_rx: Some(cassandra_response_rx),
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
            cassandra_work_tx: None,  // Mock client doesn't use direct channel
            cassandra_response_rx: None,  // Mock client doesn't use response channel
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
        let cassandra_work_tx = self.cassandra_work_tx.clone();
        let cassandra_response_rx = self.cassandra_response_rx.clone();
        let config = self.config.clone();
        let metrics = self.metrics.clone();
        
        tokio::spawn(async move {
            info!("Persistent queue processor task spawned successfully");
            
            let interval_duration = std::time::Duration::from_millis(config.ingestion.batch_timeout_ms);
            info!("Creating interval timer with duration: {:?}", interval_duration);
            let mut interval = tokio::time::interval(interval_duration);
            
            info!("Starting persistent queue processor with bidirectional MPMC channels");
            
            // Use bidirectional processor only
            let work_tx = cassandra_work_tx.expect("Multi-worker client should provide work channel");
            let response_rx = cassandra_response_rx.expect("Multi-worker client should provide response channel");
            
            info!("Starting optimized bidirectional channel mode");
            Self::run_bidirectional_processor(
                persistent_queue,
                work_tx,
                response_rx,
                config,
                metrics
            ).await;
        })
    }
    
    /// Run the optimized bidirectional processor with separate tasks
    async fn run_bidirectional_processor(
        persistent_queue: Arc<PersistentQueue>,
        work_tx: flume::Sender<WorkItem>,
        response_rx: flume::Receiver<WorkResponse>,
        config: Arc<IngestConfig>,
        metrics: IngestionMetrics,
    ) {
        info!("Starting separate response processor and work sender tasks");
        
        // Spawn dedicated response processing task
        let response_task = {
            let queue = persistent_queue.clone();
            let metrics = metrics.clone();
            tokio::spawn(async move {
                Self::run_response_processor(queue, response_rx, metrics).await;
            })
        };
        
        // Spawn dedicated work sending task
        let work_task = {
            let queue = persistent_queue.clone();
            let config = config.clone();
            tokio::spawn(async move {
                Self::run_work_sender(queue, work_tx, config).await;
            })
        };
        
        // Wait for either task to complete (shouldn't happen under normal conditions)
        tokio::select! {
            result = response_task => {
                error!("Response processor task completed unexpectedly: {:?}", result);
            }
            result = work_task => {
                error!("Work sender task completed unexpectedly: {:?}", result);
            }
        }
        
        error!("Bidirectional processor exiting - this should not happen");
    }
    
    /// Dedicated response processing task - processes responses as fast as possible
    async fn run_response_processor(
        persistent_queue: Arc<PersistentQueue>,
        response_rx: flume::Receiver<WorkResponse>,
        metrics: IngestionMetrics,
    ) {
        info!("Response processor task started");
        
        while let Ok(work_response) = response_rx.recv_async().await {
            match work_response.result {
                Ok(batch_size) => {
                    // Successful Cassandra write - remove from queue
                    if let Err(e) = persistent_queue.remove_processed_item(&work_response.queue_key, batch_size) {
                        error!("Failed to remove successful item from queue: {}", e);
                    } else {
                        metrics.datapoints_ingested.fetch_add(batch_size as u64, Ordering::Relaxed);
                    }
                }
                Err(error_msg) => {
                    // Cassandra failure - unclaim for retry
                    metrics.cassandra_errors.fetch_add(1, Ordering::Relaxed);
                    if let Err(e) = persistent_queue.mark_item_failed(&work_response.queue_key) {
                        error!("Failed to unclaim failed item: {}", e);
                    } else {
                        warn!("Unclaimed failed item {} for retry: {}", 
                              work_response.queue_key, error_msg);
                    }
                }
            }
        }
        
        error!("Response processor exiting - response channel closed");
    }
    
    /// Dedicated work sending task - sends work items based on channel capacity
    async fn run_work_sender(
        persistent_queue: Arc<PersistentQueue>,
        work_tx: flume::Sender<WorkItem>,
        config: Arc<IngestConfig>,
    ) {
        let interval_duration = std::time::Duration::from_millis(config.ingestion.batch_timeout_ms);
        let mut interval = tokio::time::interval(interval_duration);
        
        info!("Work sender task started with {}ms intervals", config.ingestion.batch_timeout_ms);
        
        loop {
            interval.tick().await;
            
            // Check how much room is available in the work channel
            let work_channel_len = work_tx.len();
            let work_channel_capacity = work_tx.capacity().unwrap_or(1000);
            let available_slots = work_channel_capacity.saturating_sub(work_channel_len);
            
            if available_slots == 0 {
                // Channel is completely full - workers are busy, skip this cycle
                continue;
            }
            
            // Only claim what we can actually send to avoid unclaiming issues
            let batch_size = available_slots.min(100); // Cap at 100 for reasonable batch size
            
            let work_items = match persistent_queue.claim_work_items_batch(30000, batch_size) {
                Ok(items) => items,
                Err(e) => {
                    error!("Failed to claim work items from queue: {}", e);
                    continue;
                }
            };
            
            if work_items.is_empty() {
                continue; // No work available
            }
            
            let mut items_sent = 0;
            for work_item in &work_items {
                let cassandra_work = WorkItem {
                    batch: work_item.entry.batch.clone(),
                    queue_key: work_item.queue_key.clone(),
                };
                
                // Since we calculated available slots, try_send should always succeed
                // But handle the edge case where another thread filled the channel
                match work_tx.try_send(cassandra_work) {
                    Ok(_) => {
                        items_sent += 1;
                    }
                    Err(flume::TrySendError::Full(_)) => {
                        // Unexpected - channel filled between our check and send
                        warn!("Work channel unexpectedly full, unclaiming remaining {} items", work_items.len() - items_sent);
                        // Unclaim this and all remaining items
                        for remaining_item in work_items.iter().skip(items_sent) {
                            if let Err(unclaim_err) = persistent_queue.mark_item_failed(&remaining_item.queue_key) {
                                error!("Failed to unclaim item after unexpected full: {}", unclaim_err);
                            }
                        }
                        break;
                    }
                    Err(flume::TrySendError::Disconnected(_)) => {
                        error!("Work channel disconnected, work sender exiting");
                        // Unclaim this and all remaining items
                        for remaining_item in work_items.iter().skip(items_sent) {
                            if let Err(unclaim_err) = persistent_queue.mark_item_failed(&remaining_item.queue_key) {
                                error!("Failed to unclaim item after disconnect: {}", unclaim_err);
                            }
                        }
                        return; // Exit task
                    }
                }
            }
            
            if items_sent > 0 {
                let current_channel_len = work_tx.len();
                let work_channel_fullness = (current_channel_len as f32 / work_channel_capacity as f32 * 100.0) as u32;
                
                info!("Sent {} work items to Cassandra workers (work channel: {}/{} items, {}% full, had {} slots available)", 
                      items_sent, current_channel_len, work_channel_capacity, work_channel_fullness, available_slots);
            }
        }
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
