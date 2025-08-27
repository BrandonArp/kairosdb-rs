//! Queue trait for abstracting persistent and null queue implementations

use kairosdb_core::datapoint::{DataPoint, DataPointBatch};
use kairosdb_core::error::KairosResult;

use crate::persistent_queue::{QueueMetrics, QueueWorkItem};

/// Trait for queue implementations (persistent or null)
pub trait Queue: Send + Sync {
    /// Enqueue an entire batch
    fn enqueue_batch(&self, batch: DataPointBatch) -> KairosResult<()>;

    /// Enqueue batch with sync control
    fn enqueue_batch_with_sync(
        &self,
        batch: DataPointBatch,
        sync_to_disk: bool,
    ) -> KairosResult<()>;

    /// Enqueue single data point
    fn enqueue(&self, data_point: DataPoint) -> KairosResult<()>;

    /// Claim next work item for processing
    fn claim_next_work_item(&self, timeout_ms: u64) -> KairosResult<Option<QueueWorkItem>>;

    /// Claim multiple work items in batch
    fn claim_work_items_batch(
        &self,
        timeout_ms: u64,
        max_items: usize,
    ) -> KairosResult<Vec<QueueWorkItem>>;

    /// Remove processed item from queue
    fn remove_processed_item(&self, queue_key: &str, batch_size: usize) -> KairosResult<()>;

    /// Mark item as failed (unclaim for retry)
    fn mark_item_failed(&self, queue_key: &str) -> KairosResult<()>;

    /// Get current queue size
    fn size(&self) -> u64;

    /// Check if queue is empty
    fn is_empty(&self) -> bool;

    /// Flush any pending writes
    fn flush(&self) -> KairosResult<()>;

    /// Get queue metrics
    fn metrics(&self) -> &QueueMetrics;

    /// Get disk usage in bytes
    fn get_disk_usage_bytes(&self) -> anyhow::Result<u64>;

    /// Manual garbage collection and compaction (no-op for null queue)
    fn manual_garbage_collection(&self) -> KairosResult<()>;
}

/// Type alias for boxed queue trait object
pub type BoxedQueue = std::sync::Arc<dyn Queue>;
