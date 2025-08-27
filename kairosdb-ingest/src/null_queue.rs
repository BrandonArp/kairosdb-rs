//! Null Queue Implementation for Memory Testing
//!
//! This module provides a null implementation of the persistent queue that
//! bypasses disk storage entirely and feeds data directly through channels.
//! Used for isolating Fjall LSM memory usage during testing.

use anyhow::Result;
use kairosdb_core::datapoint::{DataPoint, DataPointBatch};
use kairosdb_core::error::{KairosError, KairosResult};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::persistent_queue::{QueueEntry, QueueMetrics, QueueWorkItem};
use crate::queue_trait::Queue;

/// Null queue that feeds data directly to a channel without disk storage
pub struct NullQueue {
    /// Channel for sending batches directly to workers (bounded to prevent memory growth)
    work_tx: mpsc::Sender<QueueWorkItem>,
    /// Metrics (same interface as real queue)
    metrics: QueueMetrics,
    /// Counter for generating fake queue keys
    key_counter: AtomicU64,
}

impl NullQueue {
    /// Create a new null queue with direct channel to work processor
    pub async fn new(work_tx: mpsc::Sender<QueueWorkItem>) -> Result<Self> {
        info!("Initializing null queue (bypasses disk storage, feeds data directly to workers)");

        // Initialize metrics (same as real queue)
        let metrics = QueueMetrics::new_with_prefix("null")?;

        // Set initial metrics to 0
        metrics.current_size.set(0.0);
        metrics.disk_usage_bytes.set(0.0);
        metrics.oldest_entry_age_seconds.set(0.0);

        Ok(Self {
            work_tx,
            metrics,
            key_counter: AtomicU64::new(0),
        })
    }

    /// Enqueue batch - immediately sends to worker channel instead of disk (async version)
    pub async fn enqueue_batch_async(&self, batch: DataPointBatch) -> KairosResult<()> {
        let start_time = Instant::now();

        if batch.points.is_empty() {
            return Ok(());
        }

        let entry = QueueEntry::new_from_batch(batch.clone());
        let queue_key = format!("null_{}", self.key_counter.fetch_add(1, Ordering::Relaxed));

        let work_item = QueueWorkItem { entry, queue_key };

        // Send directly to worker channel instead of storing on disk
        // Await if channel is full (similar to real queue under load)
        if (self.work_tx.send(work_item).await).is_err() {
            warn!("Failed to send work item to null queue channel (channel closed)");
            self.metrics.enqueue_errors.inc();
            return Err(KairosError::internal("Null queue channel closed"));
        }

        // Update metrics
        self.metrics.enqueue_total.inc_by(batch.points.len() as f64);
        self.metrics
            .enqueue_duration
            .observe(start_time.elapsed().as_secs_f64());

        Ok(())
    }

    /// Enqueue batch with sync control (async version)
    pub async fn enqueue_batch_with_sync_async(
        &self,
        batch: DataPointBatch,
        _sync_to_disk: bool, // Ignored for null queue
    ) -> KairosResult<()> {
        self.enqueue_batch_async(batch).await
    }

    /// Enqueue batch - synchronous wrapper using spawn_blocking
    pub fn enqueue_batch(&self, batch: DataPointBatch) -> KairosResult<()> {
        let work_tx = self.work_tx.clone();
        let key_counter = &self.key_counter;
        let metrics = &self.metrics;

        let result = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let start_time = std::time::Instant::now();

                if batch.points.is_empty() {
                    return Ok(());
                }

                let entry = QueueEntry::new_from_batch(batch.clone());
                let queue_key = format!("null_{}", key_counter.fetch_add(1, Ordering::Relaxed));

                let work_item = QueueWorkItem { entry, queue_key };

                // Send directly to worker channel
                if (work_tx.send(work_item).await).is_err() {
                    warn!("Failed to send work item to null queue channel (channel closed)");
                    metrics.enqueue_errors.inc();
                    return Err(KairosError::internal("Null queue channel closed"));
                }

                // Update metrics
                metrics.enqueue_total.inc_by(batch.points.len() as f64);
                metrics
                    .enqueue_duration
                    .observe(start_time.elapsed().as_secs_f64());

                Ok(())
            })
        });

        result
    }

    /// Enqueue batch with sync control - synchronous wrapper
    pub fn enqueue_batch_with_sync(
        &self,
        batch: DataPointBatch,
        _sync_to_disk: bool,
    ) -> KairosResult<()> {
        self.enqueue_batch(batch)
    }

    /// Enqueue single data point (async version)
    pub async fn enqueue_async(&self, data_point: DataPoint) -> KairosResult<()> {
        let mut batch = DataPointBatch::new();
        batch.points.push(data_point);
        self.enqueue_batch_async(batch).await
    }

    /// Enqueue single data point - synchronous wrapper
    pub fn enqueue(&self, data_point: DataPoint) -> KairosResult<()> {
        let mut batch = DataPointBatch::new();
        batch.points.push(data_point);
        self.enqueue_batch(batch)
    }

    /// Claim work item - not needed for null queue since we send directly
    pub fn claim_next_work_item(&self, _timeout_ms: u64) -> KairosResult<Option<QueueWorkItem>> {
        // Null queue doesn't store items, so nothing to claim
        Ok(None)
    }

    /// Claim work items batch - not needed for null queue
    pub fn claim_work_items_batch(
        &self,
        _timeout_ms: u64,
        _max_items: usize,
    ) -> KairosResult<Vec<QueueWorkItem>> {
        // Null queue doesn't store items, so nothing to claim
        Ok(Vec::new())
    }

    /// Remove processed item - no-op for null queue
    pub fn remove_processed_item(&self, _queue_key: &str, batch_size: usize) -> KairosResult<()> {
        // Update dequeue metrics as if we removed an item
        self.metrics.dequeue_total.inc_by(batch_size as f64);
        Ok(())
    }

    /// Mark item as failed - no-op for null queue
    pub fn mark_item_failed(&self, _queue_key: &str) -> KairosResult<()> {
        // No-op since we don't store items
        Ok(())
    }

    /// Queue size - always 0 for null queue
    pub fn size(&self) -> u64 {
        0 // Null queue never stores anything
    }

    /// Check if empty - always true for null queue
    pub fn is_empty(&self) -> bool {
        true
    }

    /// Flush - no-op for null queue
    pub fn flush(&self) -> KairosResult<()> {
        Ok(())
    }

    /// Get metrics
    pub fn metrics(&self) -> &QueueMetrics {
        &self.metrics
    }

    /// Get disk usage - always 0 for null queue
    pub fn get_disk_usage_bytes(&self) -> Result<u64> {
        Ok(0)
    }
}

// Implement Queue trait for NullQueue
impl Queue for NullQueue {
    fn enqueue_batch(&self, batch: DataPointBatch) -> KairosResult<()> {
        self.enqueue_batch(batch)
    }

    fn enqueue_batch_with_sync(
        &self,
        batch: DataPointBatch,
        sync_to_disk: bool,
    ) -> KairosResult<()> {
        self.enqueue_batch_with_sync(batch, sync_to_disk)
    }

    fn enqueue(&self, data_point: DataPoint) -> KairosResult<()> {
        self.enqueue(data_point)
    }

    fn claim_next_work_item(&self, timeout_ms: u64) -> KairosResult<Option<QueueWorkItem>> {
        self.claim_next_work_item(timeout_ms)
    }

    fn claim_work_items_batch(
        &self,
        timeout_ms: u64,
        max_items: usize,
    ) -> KairosResult<Vec<QueueWorkItem>> {
        self.claim_work_items_batch(timeout_ms, max_items)
    }

    fn remove_processed_item(&self, queue_key: &str, batch_size: usize) -> KairosResult<()> {
        self.remove_processed_item(queue_key, batch_size)
    }

    fn mark_item_failed(&self, queue_key: &str) -> KairosResult<()> {
        self.mark_item_failed(queue_key)
    }

    fn size(&self) -> u64 {
        self.size()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn flush(&self) -> KairosResult<()> {
        self.flush()
    }

    fn metrics(&self) -> &QueueMetrics {
        self.metrics()
    }

    fn get_disk_usage_bytes(&self) -> anyhow::Result<u64> {
        self.get_disk_usage_bytes()
    }

    fn manual_garbage_collection(&self) -> KairosResult<()> {
        // No-op for null queue since there's no disk storage to garbage collect
        Ok(())
    }
}
