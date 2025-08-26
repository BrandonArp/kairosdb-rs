//! High-performance ingestion service with async batch processing
//!
//! This module provides the core ingestion service that handles:
//! - Async batch processing with configurable limits
//! - Backpressure handling when memory limits are reached
//! - Connection pooling and error recovery
//! - Metrics collection and monitoring

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;
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
use tracing::{debug, error, info, trace, warn};

use crate::{
    cassandra::{BoxedCassandraClient, CassandraClient},
    config::IngestConfig,
    multi_writer_client::{MultiWorkerCassandraClient, WorkItem, WorkResponse},
    persistent_queue::PersistentQueue,
};

use crate::mock_client::MockCassandraClient;

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
    /// Work channel utilization percentage (0-100)
    pub work_channel_utilization: Arc<AtomicU64>,
    /// Response channel utilization percentage (0-100)
    pub response_channel_utilization: Arc<AtomicU64>,
    /// Rolling average of work channel utilization (simple exponential)
    pub work_channel_avg_utilization: Arc<AtomicU64>,
    /// Rolling average of response channel utilization (simple exponential)
    pub response_channel_avg_utilization: Arc<AtomicU64>,
    /// Bloom filter memory usage in bytes
    pub bloom_memory_usage: Arc<AtomicU64>,
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
    pub work_channel_utilization_gauge: Gauge,
    pub response_channel_utilization_gauge: Gauge,
    pub work_channel_avg_utilization_gauge: Gauge,
    pub response_channel_avg_utilization_gauge: Gauge,
    pub bloom_memory_usage_gauge: Gauge,
    pub bloom_filter_primary_ones_gauge: Gauge,
    pub bloom_filter_secondary_ones_gauge: Gauge,
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
    pub async fn new(config: Arc<IngestConfig>, shutdown_token: CancellationToken) -> Result<Self> {
        config.validate().context("Invalid configuration")?;

        info!("Initializing ingestion service");

        // Initialize persistent queue for write-ahead logging
        let queue_dir = std::path::Path::new("./data/queue");
        let persistent_queue = Arc::new(
            PersistentQueue::new(queue_dir)
                .await
                .context("Failed to initialize persistent queue")?,
        );

        // Check if we should use mock client for testing
        let (cassandra_client, cassandra_work_tx, cassandra_response_rx) =
            if std::env::var("USE_MOCK_CASSANDRA").unwrap_or_default() == "true" {
                info!("Using mock Cassandra client for testing");
                let mock_client = Arc::new(MockCassandraClient::new()) as BoxedCassandraClient;
                (mock_client, None, None)
            } else {
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
                    response_rx.clone(),
                    shutdown_token.clone(),
                )
                .await
                .context("Failed to initialize multi-worker Cassandra client")?;

                // Ensure schema exists
                info!("Ensuring KairosDB schema exists");
                cassandra_client
                    .ensure_schema()
                    .await
                    .context("Failed to ensure Cassandra schema")?;

                info!(
                "Multi-worker Cassandra client ready with {} workers and bidirectional channels",
                num_workers.unwrap_or(200)
            );

                let cassandra_client = Arc::new(cassandra_client) as BoxedCassandraClient;
                (cassandra_client, Some(work_tx), Some(response_rx))
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
            work_channel_utilization: Arc::new(AtomicU64::new(0)),
            response_channel_utilization: Arc::new(AtomicU64::new(0)),
            work_channel_avg_utilization: Arc::new(AtomicU64::new(0)),
            response_channel_avg_utilization: Arc::new(AtomicU64::new(0)),
            bloom_memory_usage: Arc::new(AtomicU64::new(0)),
            prometheus_metrics,
        };

        // Initialize backpressure tracking
        let backpressure_active = Arc::new(AtomicU64::new(0));

        let service = Self {
            config: config.clone(),
            persistent_queue,
            cassandra_client,
            cassandra_work_tx,
            cassandra_response_rx,
            metrics,
            backpressure_active,
        };

        // Queue processor will be started in main.rs with proper shutdown handling
        info!("Ingestion service initialized, queue processor will be started with shutdown token");

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
                .context("Failed to initialize test persistent queue")?,
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
            work_channel_utilization: Arc::new(AtomicU64::new(0)),
            response_channel_utilization: Arc::new(AtomicU64::new(0)),
            work_channel_avg_utilization: Arc::new(AtomicU64::new(0)),
            response_channel_avg_utilization: Arc::new(AtomicU64::new(0)),
            bloom_memory_usage: Arc::new(AtomicU64::new(0)),
            prometheus_metrics,
        };

        let backpressure_active = Arc::new(AtomicU64::new(0));

        let service = Self {
            config,
            persistent_queue,
            cassandra_client,
            cassandra_work_tx: None, // Mock client doesn't use direct channel
            cassandra_response_rx: None, // Mock client doesn't use response channel
            metrics,
            backpressure_active,
        };

        // Start background queue processing task for testing
        let shutdown_token = tokio_util::sync::CancellationToken::new();
        let _queue_processor = service.start_queue_processor(shutdown_token).await;

        info!("Test ingestion service initialized with mock client and persistent queue processor");
        Ok(service)
    }

    /// Submit a batch for ingestion with write-ahead logging (fire-and-forget)
    /// Uses the default sync setting from configuration
    pub fn ingest_batch(&self, batch: DataPointBatch) -> KairosResult<()> {
        self.ingest_batch_with_sync(batch, self.config.ingestion.default_sync)
    }

    /// Submit a batch for ingestion with optional sync control
    pub fn ingest_batch_with_sync(
        &self,
        batch: DataPointBatch,
        sync_to_disk: bool,
    ) -> KairosResult<()> {
        let start = Instant::now();

        // Check disk usage for backpressure (same as ingest_batch)
        let disk_usage = self.persistent_queue.get_disk_usage_bytes().unwrap_or(0);
        let max_disk_bytes = self.config.ingestion.max_queue_disk_bytes;

        let current_queue_size = self.persistent_queue.size();
        let max_queue_size = self.config.ingestion.max_queue_size as u64;

        trace!(
            "Queue check (sync={}): items={}/{}, disk={:.2}MB/{:.2}MB",
            sync_to_disk,
            current_queue_size,
            max_queue_size,
            disk_usage as f64 / 1_048_576.0,
            max_disk_bytes as f64 / 1_048_576.0
        );

        self.persistent_queue
            .metrics()
            .disk_usage_bytes
            .set(disk_usage as f64);

        // Check disk usage limit first
        if disk_usage > max_disk_bytes {
            self.backpressure_active.store(1, Ordering::Relaxed);
            warn!(
                "Queue disk usage limit exceeded: {:.2}MB > {:.2}MB, activating backpressure",
                disk_usage as f64 / 1_048_576.0,
                max_disk_bytes as f64 / 1_048_576.0
            );
            return Err(KairosError::rate_limit(format!(
                "Queue disk usage limit exceeded: {:.2}MB",
                disk_usage as f64 / 1_048_576.0
            )));
        }

        // Check item count limit
        if current_queue_size > max_queue_size {
            self.backpressure_active.store(1, Ordering::Relaxed);
            warn!(
                "Queue item count limit exceeded: {} > {}, activating backpressure",
                current_queue_size, max_queue_size
            );
            return Err(KairosError::rate_limit(format!(
                "Queue size limit exceeded: {} items",
                current_queue_size
            )));
        }

        // Validate batch if validation is enabled
        if self.config.ingestion.enable_validation {
            if let Err(e) = batch.validate_self() {
                self.metrics
                    .validation_errors
                    .fetch_add(1, Ordering::Relaxed);
                return Err(e);
            }
        }

        // Write batch to persistent queue with sync control
        if let Err(e) = self
            .persistent_queue
            .enqueue_batch_with_sync(batch.clone(), sync_to_disk)
        {
            error!("Failed to write batch to persistent queue: {}", e);
            self.metrics
                .ingestion_errors
                .fetch_add(1, Ordering::Relaxed);
            return Err(e);
        }

        // Update success metrics
        self.metrics
            .datapoints_ingested
            .fetch_add(batch.points.len() as u64, Ordering::Relaxed);
        self.metrics
            .batches_processed
            .fetch_add(1, Ordering::Relaxed);
        *self.metrics.last_batch_time.write() = Some(Instant::now());

        let elapsed = start.elapsed();
        self.metrics
            .avg_batch_time_ms
            .store(elapsed.as_millis() as u64, Ordering::Relaxed);

        trace!(
            "Successfully enqueued batch of {} points (sync={}) in {:?}",
            batch.points.len(),
            sync_to_disk,
            elapsed
        );

        Ok(())
    }

    /// Start the background queue processor task
    pub async fn start_queue_processor(
        &self,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let persistent_queue = self.persistent_queue.clone();
        let _cassandra_client = self.cassandra_client.clone();
        let cassandra_work_tx = self.cassandra_work_tx.clone();
        let cassandra_response_rx = self.cassandra_response_rx.clone();
        let config = self.config.clone();
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            info!("Persistent queue processor task spawned successfully");

            let interval_duration =
                std::time::Duration::from_millis(config.ingestion.batch_timeout_ms);
            info!(
                "Creating interval timer with duration: {:?}",
                interval_duration
            );
            let _interval = tokio::time::interval(interval_duration);

            info!("Starting persistent queue processor with bidirectional MPMC channels");

            // Use bidirectional processor only
            let work_tx =
                cassandra_work_tx.expect("Multi-worker client should provide work channel");
            let response_rx =
                cassandra_response_rx.expect("Multi-worker client should provide response channel");

            info!("Starting optimized bidirectional channel mode");
            Self::run_bidirectional_processor(
                persistent_queue,
                work_tx,
                response_rx,
                config,
                metrics,
                shutdown_token,
            )
            .await;
        })
    }

    /// Run the optimized bidirectional processor with separate tasks
    async fn run_bidirectional_processor(
        persistent_queue: Arc<PersistentQueue>,
        work_tx: flume::Sender<WorkItem>,
        response_rx: flume::Receiver<WorkResponse>,
        config: Arc<IngestConfig>,
        metrics: IngestionMetrics,
        shutdown_token: CancellationToken,
    ) {
        info!("Starting separate response processor and work sender tasks");

        // Spawn dedicated response processing task
        let response_task = {
            let queue = persistent_queue.clone();
            let metrics = metrics.clone();
            let shutdown_token_clone = shutdown_token.clone();
            tokio::spawn(async move {
                Self::run_response_processor(queue, response_rx, metrics, shutdown_token_clone)
                    .await;
            })
        };

        // Spawn dedicated work sending task
        let work_task = {
            let queue = persistent_queue.clone();
            let config = config.clone();
            let metrics_clone = metrics.clone();
            let shutdown_token_clone = shutdown_token.clone();
            tokio::spawn(async move {
                Self::run_work_sender(queue, work_tx, config, metrics_clone, shutdown_token_clone)
                    .await;
            })
        };

        // Wait for shutdown signal or task completion
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                info!("Shutdown requested, stopping queue processor tasks");
            }
            result = response_task => {
                warn!("Response processor task completed unexpectedly: {:?}", result);
            }
            result = work_task => {
                warn!("Work sender task completed unexpectedly: {:?}", result);
            }
        }

        info!("Queue processor shutdown complete");
    }

    /// Dedicated response processing task - processes responses with batched queue operations
    async fn run_response_processor(
        persistent_queue: Arc<PersistentQueue>,
        response_rx: flume::Receiver<WorkResponse>,
        metrics: IngestionMetrics,
        shutdown_token: CancellationToken,
    ) {
        info!("Response processor task started with batched queue operations");

        let mut successful_removals = Vec::new();
        let mut failed_unclaims = Vec::new();

        loop {
            // Check for shutdown signal
            if shutdown_token.is_cancelled() {
                info!("Response processor received shutdown signal, exiting");
                break;
            }
            // Record response channel utilization BEFORE collecting responses
            let response_channel_len = response_rx.len();
            let response_channel_capacity = response_rx.capacity().unwrap_or(1000);
            let response_channel_fullness =
                (response_channel_len as f32 / response_channel_capacity as f32 * 100.0) as u64;

            // Update response channel metrics
            metrics
                .response_channel_utilization
                .store(response_channel_fullness, Ordering::Relaxed);
            metrics
                .prometheus_metrics
                .response_channel_utilization_gauge
                .set(response_channel_fullness as f64);

            // Update rolling average for response channel
            let old_avg = metrics
                .response_channel_avg_utilization
                .load(Ordering::Relaxed);
            let new_avg =
                ((old_avg as f64 * 0.9) + (response_channel_fullness as f64 * 0.1)) as u64;
            metrics
                .response_channel_avg_utilization
                .store(new_avg, Ordering::Relaxed);
            metrics
                .prometheus_metrics
                .response_channel_avg_utilization_gauge
                .set(new_avg as f64);

            // Collect responses in batches - process immediately available ones
            let mut responses_collected = 0;

            // Always get at least one response (blocking)
            match response_rx.recv_async().await {
                Ok(work_response) => {
                    Self::collect_response(
                        &mut successful_removals,
                        &mut failed_unclaims,
                        work_response,
                    );
                    responses_collected += 1;
                }
                Err(_) => {
                    error!("Response processor exiting - response channel closed");
                    break;
                }
            }

            // Then collect all immediately available responses (non-blocking)
            while responses_collected < 100 {
                match response_rx.try_recv() {
                    Ok(work_response) => {
                        Self::collect_response(
                            &mut successful_removals,
                            &mut failed_unclaims,
                            work_response,
                        );
                        responses_collected += 1;
                    }
                    Err(flume::TryRecvError::Empty) => {
                        // No more responses available right now, process what we have
                        break;
                    }
                    Err(flume::TryRecvError::Disconnected) => {
                        error!("Response processor exiting - response channel closed");
                        break;
                    }
                }
            }

            // Process batched removals
            if !successful_removals.is_empty() {
                let total_points = Self::process_successful_batch(
                    &persistent_queue,
                    &successful_removals,
                    &metrics,
                )
                .await;
                if responses_collected == 1 {
                    // Single response - don't log batch info
                } else {
                    debug!("Processed batch of {} successful responses ({} total points, response channel: {}/{} items, {}% full, {}% avg)",
                          successful_removals.len(), total_points, response_channel_len, response_channel_capacity, response_channel_fullness, new_avg);
                }
                successful_removals.clear();
            }

            // Process batched unclaims
            if !failed_unclaims.is_empty() {
                Self::process_failed_batch(&persistent_queue, &failed_unclaims, &metrics).await;
                failed_unclaims.clear();
            }
        }
    }

    /// Collect a response into the appropriate batch
    fn collect_response(
        successful_removals: &mut Vec<(String, usize)>,
        failed_unclaims: &mut Vec<(String, String)>,
        work_response: WorkResponse,
    ) {
        match work_response.result {
            Ok(batch_size) => {
                successful_removals.push((work_response.queue_key, batch_size));
            }
            Err(error_msg) => {
                failed_unclaims.push((work_response.queue_key, error_msg));
            }
        }
    }

    /// Process a batch of successful responses
    async fn process_successful_batch(
        persistent_queue: &Arc<PersistentQueue>,
        successful_removals: &[(String, usize)],
        metrics: &IngestionMetrics,
    ) -> u64 {
        let mut total_points = 0;

        for (queue_key, batch_size) in successful_removals.iter() {
            if let Err(e) = persistent_queue.remove_processed_item(queue_key, *batch_size) {
                error!("Failed to remove successful item from queue: {}", e);
            } else {
                metrics
                    .datapoints_ingested
                    .fetch_add(*batch_size as u64, Ordering::Relaxed);
                total_points += *batch_size as u64;
            }
        }

        total_points
    }

    /// Process a batch of failed responses  
    async fn process_failed_batch(
        persistent_queue: &Arc<PersistentQueue>,
        failed_unclaims: &[(String, String)],
        metrics: &IngestionMetrics,
    ) {
        for (queue_key, error_msg) in failed_unclaims.iter() {
            metrics.cassandra_errors.fetch_add(1, Ordering::Relaxed);
            if let Err(e) = persistent_queue.mark_item_failed(queue_key) {
                error!("Failed to unclaim failed item: {}", e);
            } else {
                warn!(
                    "Unclaimed failed item {} for retry: {}",
                    queue_key, error_msg
                );
            }
        }
    }

    /// Dedicated work sending task - sends work items as fast as possible
    async fn run_work_sender(
        persistent_queue: Arc<PersistentQueue>,
        work_tx: flume::Sender<WorkItem>,
        config: Arc<IngestConfig>,
        metrics: IngestionMetrics,
        shutdown_token: CancellationToken,
    ) {
        let full_channel_delay =
            std::time::Duration::from_millis(config.ingestion.batch_timeout_ms);

        info!(
            "Work sender task started with no delays (except {}ms when channel full)",
            config.ingestion.batch_timeout_ms
        );

        loop {
            // Check for shutdown signal
            if shutdown_token.is_cancelled() {
                info!("Work sender received shutdown signal, exiting");
                break;
            }
            // Check how much room is available in the work channel
            let work_channel_len = work_tx.len();
            let work_channel_capacity = work_tx.capacity().unwrap_or(1000);
            let available_slots = work_channel_capacity.saturating_sub(work_channel_len);

            if available_slots == 0 {
                // Channel is completely full - workers are busy, wait before checking again
                tokio::time::sleep(full_channel_delay).await;
                continue;
            }

            // Use large batches to fill channel efficiently
            let batch_size = available_slots.min(900); // Up to 900 items per batch

            let work_items = match persistent_queue.claim_work_items_batch(30000, batch_size) {
                Ok(items) => items,
                Err(e) => {
                    error!("Failed to claim work items from queue: {}", e);
                    continue;
                }
            };

            if work_items.is_empty() {
                // No work available - add small delay to prevent CPU spinning
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                continue;
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
                        warn!(
                            "Work channel unexpectedly full, unclaiming remaining {} items",
                            work_items.len() - items_sent
                        );
                        // Unclaim this and all remaining items
                        for remaining_item in work_items.iter().skip(items_sent) {
                            if let Err(unclaim_err) =
                                persistent_queue.mark_item_failed(&remaining_item.queue_key)
                            {
                                error!(
                                    "Failed to unclaim item after unexpected full: {}",
                                    unclaim_err
                                );
                            }
                        }
                        break;
                    }
                    Err(flume::TrySendError::Disconnected(_)) => {
                        error!("Work channel disconnected, work sender exiting");
                        // Unclaim this and all remaining items
                        for remaining_item in work_items.iter().skip(items_sent) {
                            if let Err(unclaim_err) =
                                persistent_queue.mark_item_failed(&remaining_item.queue_key)
                            {
                                error!("Failed to unclaim item after disconnect: {}", unclaim_err);
                            }
                        }
                        return; // Exit task
                    }
                }
            }

            // Always record channel utilization metrics
            let current_channel_len = work_tx.len();
            let work_channel_fullness =
                (current_channel_len as f32 / work_channel_capacity as f32 * 100.0) as u64;

            // Update current utilization
            metrics
                .work_channel_utilization
                .store(work_channel_fullness, Ordering::Relaxed);
            metrics
                .prometheus_metrics
                .work_channel_utilization_gauge
                .set(work_channel_fullness as f64);

            // Update rolling average (simple exponential average: new_avg = 0.9 * old_avg + 0.1 * current)
            let old_avg = metrics.work_channel_avg_utilization.load(Ordering::Relaxed);
            let new_avg = ((old_avg as f64 * 0.9) + (work_channel_fullness as f64 * 0.1)) as u64;
            metrics
                .work_channel_avg_utilization
                .store(new_avg, Ordering::Relaxed);
            metrics
                .prometheus_metrics
                .work_channel_avg_utilization_gauge
                .set(new_avg as f64);

            if items_sent > 0 {
                debug!("Sent {} work items to Cassandra workers (work channel: {}/{} items, {}% full, {}% avg, had {} slots available)",
                      items_sent, current_channel_len, work_channel_capacity, work_channel_fullness, new_avg, available_slots);
            }

            // No delay - loop immediately to check for more work
        }
    }

    /// Update bloom filter memory usage metrics
    fn update_bloom_memory_metrics(&self) {
        // Get detailed bloom stats from the Cassandra client (includes expensive ones count)
        let cassandra_stats = self.cassandra_client.get_detailed_stats();

        // Update atomic metrics
        self.metrics.bloom_memory_usage.store(
            cassandra_stats.bloom_filter_total_memory_bytes,
            std::sync::atomic::Ordering::Relaxed,
        );

        // Update Prometheus metrics
        self.metrics
            .prometheus_metrics
            .bloom_memory_usage_gauge
            .set(cassandra_stats.bloom_filter_total_memory_bytes as f64);

        // Update bloom filter ones count metrics
        if let Some(primary_ones) = cassandra_stats.bloom_filter_primary_ones_count {
            self.metrics
                .prometheus_metrics
                .bloom_filter_primary_ones_gauge
                .set(primary_ones as f64);
        }

        if let Some(secondary_ones) = cassandra_stats.bloom_filter_secondary_ones_count {
            self.metrics
                .prometheus_metrics
                .bloom_filter_secondary_ones_gauge
                .set(secondary_ones as f64);
        } else {
            // Clear secondary gauge when no secondary filter exists
            self.metrics
                .prometheus_metrics
                .bloom_filter_secondary_ones_gauge
                .set(0.0);
        }
    }

    /// Get current metrics snapshot
    pub fn get_metrics_snapshot(&self) -> IngestionMetricsSnapshot {
        // Update bloom memory metrics when snapshot is requested
        self.update_bloom_memory_metrics();

        IngestionMetricsSnapshot {
            datapoints_ingested: self.metrics.datapoints_ingested.load(Ordering::Relaxed),
            batches_processed: self.metrics.batches_processed.load(Ordering::Relaxed),
            ingestion_errors: self.metrics.ingestion_errors.load(Ordering::Relaxed),
            validation_errors: self.metrics.validation_errors.load(Ordering::Relaxed),
            cassandra_errors: self.metrics.cassandra_errors.load(Ordering::Relaxed),
            queue_size: self.persistent_queue.size() as usize,
            memory_usage: self.metrics.memory_usage.load(Ordering::Relaxed),
            bloom_memory_usage: self.metrics.bloom_memory_usage.load(Ordering::Relaxed),
            avg_batch_time_ms: self.metrics.avg_batch_time_ms.load(Ordering::Relaxed),
            last_batch_time: *self.metrics.last_batch_time.read(),
            backpressure_active: self.backpressure_active.load(Ordering::Relaxed) > 0,
        }
    }

    /// Health check for the ingestion service
    pub async fn health_check(&self) -> Result<HealthStatus> {
        // Update bloom memory metrics during health check
        self.update_bloom_memory_metrics();

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

        let work_channel_utilization_gauge = register_gauge!(
            format!("kairosdb_work_channel_utilization{}", suffix),
            "Work channel utilization percentage (0-100)"
        )
        .unwrap_or_else(|_| prometheus::Gauge::new("test_gauge3", "test").unwrap());

        let response_channel_utilization_gauge = register_gauge!(
            format!("kairosdb_response_channel_utilization{}", suffix),
            "Response channel utilization percentage (0-100)"
        )
        .unwrap_or_else(|_| prometheus::Gauge::new("test_gauge4", "test").unwrap());

        let work_channel_avg_utilization_gauge = register_gauge!(
            format!("kairosdb_work_channel_avg_utilization{}", suffix),
            "Work channel average utilization percentage (0-100)"
        )
        .unwrap_or_else(|_| prometheus::Gauge::new("test_gauge5", "test").unwrap());

        let response_channel_avg_utilization_gauge = register_gauge!(
            format!("kairosdb_response_channel_avg_utilization{}", suffix),
            "Response channel average utilization percentage (0-100)"
        )
        .unwrap_or_else(|_| prometheus::Gauge::new("test_gauge6", "test").unwrap());

        let bloom_memory_usage_gauge = register_gauge!(
            format!("kairosdb_bloom_memory_usage_bytes{}", suffix),
            "Bloom filter memory usage in bytes"
        )
        .unwrap_or_else(|_| prometheus::Gauge::new("test_gauge7", "test").unwrap());

        let bloom_filter_primary_ones_gauge = register_gauge!(
            format!("kairosdb_bloom_filter_primary_ones{}", suffix),
            "Number of set bits in primary bloom filter"
        )
        .unwrap_or_else(|_| prometheus::Gauge::new("test_gauge8", "test").unwrap());

        let bloom_filter_secondary_ones_gauge = register_gauge!(
            format!("kairosdb_bloom_filter_secondary_ones{}", suffix),
            "Number of set bits in secondary bloom filter"
        )
        .unwrap_or_else(|_| prometheus::Gauge::new("test_gauge9", "test").unwrap());

        Ok(Self {
            datapoints_total,
            batches_total,
            errors_total,
            memory_usage_gauge,
            batch_duration_histogram,
            work_channel_utilization_gauge,
            response_channel_utilization_gauge,
            work_channel_avg_utilization_gauge,
            response_channel_avg_utilization_gauge,
            bloom_memory_usage_gauge,
            bloom_filter_primary_ones_gauge,
            bloom_filter_secondary_ones_gauge,
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
            work_channel_utilization: Arc::new(AtomicU64::new(0)),
            response_channel_utilization: Arc::new(AtomicU64::new(0)),
            work_channel_avg_utilization: Arc::new(AtomicU64::new(0)),
            response_channel_avg_utilization: Arc::new(AtomicU64::new(0)),
            bloom_memory_usage: Arc::new(AtomicU64::new(0)),
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
    pub bloom_memory_usage: u64,
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
        let config = Arc::new(IngestConfig::default());
        let service = IngestionService::new_with_mock(config).await;
        assert!(service.is_ok());
    }

    #[tokio::test]
    async fn test_batch_ingestion() {
        use kairosdb_core::{datapoint::DataPoint, time::Timestamp};

        let mut config = IngestConfig::default();
        // Set high queue size limit for testing to avoid triggering backpressure
        config.ingestion.max_queue_size = 100000;
        let config = Arc::new(config);
        let service = IngestionService::new_with_mock(config).await.unwrap();

        let mut batch = DataPointBatch::new();
        batch
            .add_point(DataPoint::new_long("test.metric", Timestamp::now(), 42))
            .unwrap();

        let result = service.ingest_batch(batch);
        assert!(result.is_ok(), "Batch ingestion failed: {:?}", result.err());

        let metrics = service.get_metrics_snapshot();
        assert_eq!(metrics.batches_processed, 1);
        assert_eq!(metrics.datapoints_ingested, 1);
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

    #[tokio::test]
    async fn test_bloom_memory_metrics() {
        let config = Arc::new(IngestConfig::default());
        let service = IngestionService::new_with_mock(config).await.unwrap();

        // Get initial metrics snapshot
        let metrics = service.get_metrics_snapshot();

        // Bloom memory usage should be calculated and non-zero
        assert!(
            metrics.bloom_memory_usage > 0,
            "Bloom memory usage should be non-zero"
        );
        assert!(
            metrics.bloom_memory_usage < 10_000_000,
            "Bloom memory should be less than 10MB"
        );

        // Test that Prometheus metrics are also updated
        let prometheus_value = service
            .metrics
            .prometheus_metrics
            .bloom_memory_usage_gauge
            .get();
        assert_eq!(
            prometheus_value as u64, metrics.bloom_memory_usage,
            "Prometheus metric should match atomic metric"
        );

        // Test that bloom filter ones count metrics are also updated (mock returns None, so should be 0)
        let _primary_ones = service
            .metrics
            .prometheus_metrics
            .bloom_filter_primary_ones_gauge
            .get();
        let secondary_ones = service
            .metrics
            .prometheus_metrics
            .bloom_filter_secondary_ones_gauge
            .get();
        
        // Mock client doesn't provide actual ones count, but the metrics should still be initialized
        // Primary ones gauge should be 0 (not set since mock returns None)
        // Secondary ones gauge should be 0 (explicitly set when None)
        assert_eq!(secondary_ones, 0.0, "Secondary ones gauge should be 0 when no secondary filter");
    }
}
