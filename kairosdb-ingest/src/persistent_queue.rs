//! Persistent Queue Implementation using Fjall
//!
//! This module provides a write-ahead log for data points using Fjall LSM storage.
//! HTTP requests immediately write to disk and return success, while background
//! tasks drain the queue to Cassandra.

use anyhow::{Context, Result};
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use kairosdb_core::datapoint::{DataPoint, DataPointBatch};
use kairosdb_core::error::{KairosError, KairosResult};
use prometheus::{register_counter, register_gauge, register_histogram, Counter, Gauge, Histogram};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

/// Entry in the persistent queue - now stores batches instead of individual points
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueEntry {
    pub id: String,
    pub timestamp_ns: u64,
    pub batch: DataPointBatch,
    pub in_flight_since: Option<u64>,  // Timestamp when claimed, None if available
    pub processing_attempts: u32,      // Number of processing attempts
}

/// A work item that includes the queue entry and its key for status tracking
#[derive(Debug, Clone)]
pub struct QueueWorkItem {
    pub entry: QueueEntry,
    pub queue_key: String,
}

impl QueueEntry {
    pub fn new_from_batch(batch: DataPointBatch) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
            batch,
            in_flight_since: None,
            processing_attempts: 0,
        }
    }

    // Keep backward compatibility for single data points
    pub fn new(data_point: DataPoint) -> Self {
        let mut batch = DataPointBatch::new();
        batch.points.push(data_point);
        Self::new_from_batch(batch)
    }
    
    /// Generate a sortable key for queue ordering
    pub fn queue_key(&self) -> String {
        format!("{:020}_{}", self.timestamp_ns, self.id)
    }
}

/// Queue-specific metrics for monitoring
#[derive(Debug, Clone)]
pub struct QueueMetrics {
    // Counters
    pub enqueue_total: Counter,
    pub dequeue_total: Counter,
    pub enqueue_errors: Counter,
    pub dequeue_errors: Counter,
    
    // Gauges
    pub current_size: Gauge,
    pub oldest_entry_age_seconds: Gauge,
    pub disk_usage_bytes: Gauge,
    
    // Histograms
    pub enqueue_duration: Histogram,
    pub dequeue_duration: Histogram,
    pub batch_size: Histogram,
}

impl QueueMetrics {
    pub fn new() -> Result<Self> {
        Self::new_with_prefix("")
    }
    
    pub fn new_with_prefix(prefix: &str) -> Result<Self> {
        let suffix = if prefix.is_empty() {
            String::new()
        } else {
            format!("_{}", prefix)
        };

        let enqueue_total = register_counter!(
            format!("kairosdb_queue_datapoints_enqueued_total{}", suffix),
            "Total number of data points enqueued"
        ).unwrap_or_else(|_| Counter::new("test_counter", "test").unwrap());

        let dequeue_total = register_counter!(
            format!("kairosdb_queue_datapoints_dequeued_total{}", suffix),
            "Total number of data points dequeued"
        ).unwrap_or_else(|_| Counter::new("test_counter2", "test").unwrap());

        let enqueue_errors = register_counter!(
            format!("kairosdb_queue_enqueue_errors{}", suffix),
            "Total number of enqueue errors"
        ).unwrap_or_else(|_| Counter::new("test_counter3", "test").unwrap());

        let dequeue_errors = register_counter!(
            format!("kairosdb_queue_dequeue_errors{}", suffix),
            "Total number of dequeue errors"
        ).unwrap_or_else(|_| Counter::new("test_counter4", "test").unwrap());

        let current_size = register_gauge!(
            format!("kairosdb_queue_batches_pending{}", suffix),
            "Current number of batches pending in persistent queue"
        ).unwrap_or_else(|_| Gauge::new("test_gauge", "test").unwrap());

        let oldest_entry_age_seconds = register_gauge!(
            format!("kairosdb_queue_oldest_entry_age_seconds{}", suffix),
            "Age in seconds of the oldest entry in the queue"
        ).unwrap_or_else(|_| Gauge::new("test_gauge2", "test").unwrap());

        let disk_usage_bytes = register_gauge!(
            format!("kairosdb_queue_disk_usage_bytes{}", suffix),
            "Disk space used by persistent queue in bytes"
        ).unwrap_or_else(|_| Gauge::new("test_gauge3", "test").unwrap());

        let enqueue_duration = register_histogram!(
            format!("kairosdb_queue_enqueue_duration_seconds{}", suffix),
            "Time spent enqueuing data points"
        ).unwrap_or_else(|_| {
            prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
                "test_histogram", "test"
            )).unwrap()
        });

        let dequeue_duration = register_histogram!(
            format!("kairosdb_queue_dequeue_duration_seconds{}", suffix),
            "Time spent dequeuing batches"
        ).unwrap_or_else(|_| {
            prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
                "test_histogram2", "test"
            )).unwrap()
        });

        let batch_size = register_histogram!(
            format!("kairosdb_queue_batch_datapoints{}", suffix),
            "Number of data points per batch dequeued from queue"
        ).unwrap_or_else(|_| {
            prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
                "test_histogram3", "test"
            )).unwrap()
        });

        Ok(Self {
            enqueue_total,
            dequeue_total,
            enqueue_errors,
            dequeue_errors,
            current_size,
            oldest_entry_age_seconds,
            disk_usage_bytes,
            enqueue_duration,
            dequeue_duration,
            batch_size,
        })
    }
}

/// Persistent queue for data points using Fjall storage
pub struct PersistentQueue {
    keyspace: Arc<Keyspace>,
    partition: PartitionHandle,
    queue_size: AtomicU64,
    metrics: QueueMetrics,
    data_dir: std::path::PathBuf,
}

impl PersistentQueue {
    /// Create a new persistent queue
    pub async fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        
        info!("Initializing persistent queue at: {}", data_dir.display());
        
        // Create the data directory if it doesn't exist
        std::fs::create_dir_all(data_dir)
            .context("Failed to create persistent queue data directory")?;
        
        // Open Fjall keyspace
        let keyspace = Config::new(data_dir)
            .open()
            .context("Failed to open Fjall keyspace")?;
            
        // Open the queue partition
        let partition = keyspace
            .open_partition("datapoint_queue", PartitionCreateOptions::default())
            .context("Failed to open queue partition")?;
            
        // Initialize metrics
        let metrics = QueueMetrics::new()
            .context("Failed to initialize queue metrics")?;
        
        // Count existing entries for queue size metric
        let queue_size = partition.iter().count() as u64;
        info!("Persistent queue initialized with {} existing entries", queue_size);
        
        // Set initial metrics
        metrics.current_size.set(queue_size as f64);
        
        Ok(Self {
            keyspace: Arc::new(keyspace),
            partition,
            queue_size: AtomicU64::new(queue_size),
            metrics,
            data_dir: data_dir.to_path_buf(),
        })
    }
    
    /// Enqueue an entire batch (optimized write-ahead log)
    pub fn enqueue_batch(&self, batch: DataPointBatch) -> KairosResult<()> {
        let start_time = Instant::now();
        
        let entry = QueueEntry::new_from_batch(batch);
        let key = entry.queue_key();
        
        // Serialize the entire batch at once
        let data = rmp_serde::to_vec(&entry).map_err(|e| {
            KairosError::validation(format!("Failed to serialize queue entry: {}", e))
        })?;
        
        // Single write to Fjall for the entire batch
        self.partition.insert(&key, data).map_err(|e| {
            KairosError::validation(format!("Failed to write batch to queue: {}", e))
        })?;
        
        // Update metrics and queue size counter
        let batch_size = entry.batch.points.len();
        let new_size = self.queue_size.fetch_add(1, Ordering::Relaxed) + 1;
        let duration = start_time.elapsed();
        
        self.metrics.enqueue_duration.observe(duration.as_secs_f64());
        self.metrics.enqueue_total.inc_by(batch_size as f64);
        self.metrics.current_size.set(new_size as f64);
        
        trace!("Enqueued batch of {} points to persistent queue in {:?}", 
               batch_size, duration);
        Ok(())
    }

    /// Enqueue an entire batch with optional sync control
    pub fn enqueue_batch_with_sync(&self, batch: DataPointBatch, sync_to_disk: bool) -> KairosResult<()> {
        let start_time = Instant::now();
        
        let entry = QueueEntry::new_from_batch(batch);
        let key = entry.queue_key();
        
        // Serialize the entire batch at once
        let data = rmp_serde::to_vec(&entry).map_err(|e| {
            KairosError::validation(format!("Failed to serialize queue entry: {}", e))
        })?;
        
        // Single write to Fjall for the entire batch
        self.partition.insert(&key, data).map_err(|e| {
            KairosError::validation(format!("Failed to write batch to queue: {}", e))
        })?;
        
        // Conditionally persist to disk immediately 
        if sync_to_disk {
            self.keyspace.persist(PersistMode::SyncAll).map_err(|e| {
                KairosError::validation(format!("Failed to sync queue to disk: {}", e))
            })?;
        }
        
        // Update metrics and queue size counter
        let batch_size = entry.batch.points.len();
        let new_size = self.queue_size.fetch_add(1, Ordering::Relaxed) + 1;
        let duration = start_time.elapsed();
        
        self.metrics.enqueue_duration.observe(duration.as_secs_f64());
        self.metrics.enqueue_total.inc_by(batch_size as f64);
        self.metrics.current_size.set(new_size as f64);
        
        if sync_to_disk {
            trace!("Enqueued batch of {} points to persistent queue with sync in {:?}", 
                   batch_size, duration);
        } else {
            trace!("Enqueued batch of {} points to persistent queue (buffered) in {:?}", 
                   batch_size, duration);
        }
        Ok(())
    }

    /// Enqueue a single data point (backward compatibility)
    pub fn enqueue(&self, data_point: DataPoint) -> KairosResult<()> {
        let start_time = Instant::now();
        
        let entry = QueueEntry::new(data_point);
        let key = entry.queue_key();
        
        // Serialize the entry using MessagePack (supports untagged enums)
        let value = rmp_serde::to_vec(&entry)
            .map_err(|e| {
                self.metrics.enqueue_errors.inc();
                KairosError::internal(format!("Failed to serialize queue entry: {}", e))
            })?;
            
        // Write to persistent storage
        self.partition.insert(&key, value)
            .map_err(|e| {
                self.metrics.enqueue_errors.inc();
                KairosError::internal(format!("Failed to write to persistent queue: {}", e))
            })?;
            
        // Update metrics
        let new_size = self.queue_size.fetch_add(1, Ordering::Relaxed) + 1;
        self.metrics.enqueue_total.inc();
        self.metrics.current_size.set(new_size as f64);
        self.metrics.enqueue_duration.observe(start_time.elapsed().as_secs_f64());
        
        trace!("Enqueued data point with key: {}", key);
        Ok(())
    }
    
    /// Claim the next available work item (marks it as in-flight atomically)
    /// This prevents race conditions and ensures crash safety
    pub fn claim_next_work_item(&self, timeout_ms: u64) -> KairosResult<Option<QueueWorkItem>> {
        let start_time = Instant::now();
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        
        // Iterate through queue to find an available item
        let mut iter = self.partition.iter();
        loop {
            let item = match iter.next() {
                Some(item) => item,
                None => {
                    // Queue is empty
                    self.metrics.oldest_entry_age_seconds.set(0.0);
                    return Ok(None);
                }
            };
            
            let (key, value) = item
                .map_err(|e| {
                    self.metrics.dequeue_errors.inc();
                    KairosError::internal(format!("Failed to read from queue: {}", e))
                })?;
                
            // Deserialize the entry using MessagePack
            let mut entry: QueueEntry = match rmp_serde::from_slice(&value) {
                Ok(entry) => entry,
                Err(e) => {
                    self.metrics.dequeue_errors.inc();
                    // Remove corrupted entry
                    debug!("Failed to deserialize queue entry, removing corrupted entry: {}", e);
                    self.partition.remove(key)
                        .map_err(|e| KairosError::internal(format!("Failed to remove corrupted entry: {}", e)))?;
                    self.queue_size.fetch_sub(1, Ordering::Relaxed);
                    self.metrics.current_size.set(self.queue_size.load(Ordering::Relaxed) as f64);
                    continue; // Try next entry
                }
            };
            
            // Check if item is available or timed out
            let is_available = match entry.in_flight_since {
                None => true,  // Available
                Some(in_flight_time) => {
                    // Check if timed out (crashed worker)
                    let age_ms = (now_ns.saturating_sub(in_flight_time)) / 1_000_000;
                    if age_ms > timeout_ms {
                        warn!("Found timed-out in-flight item ({}ms old), reclaiming", age_ms);
                        true  // Reclaim timed-out item
                    } else {
                        false  // Still being processed
                    }
                }
            };
            
            if is_available {
                // Extract the queue key string before moving the key
                let queue_key_string = String::from_utf8_lossy(&key).to_string();
                
                // Claim this item atomically by marking it in-flight
                entry.in_flight_since = Some(now_ns);
                entry.processing_attempts += 1;
                
                // Serialize and update the entry in queue
                let updated_data = rmp_serde::to_vec(&entry).map_err(|e| {
                    KairosError::validation(format!("Failed to serialize updated entry: {}", e))
                })?;
                
                self.partition.insert(key, updated_data).map_err(|e| {
                    KairosError::validation(format!("Failed to update claimed entry: {}", e))
                })?;
                
                // Update oldest entry age
                let age_seconds = (now_ns.saturating_sub(entry.timestamp_ns)) as f64 / 1_000_000_000.0;
                self.metrics.oldest_entry_age_seconds.set(age_seconds);
                
                let batch_size = entry.batch.points.len();
                trace!("Claimed work item with {} points (attempt {}) in {:?}", 
                       batch_size, entry.processing_attempts, start_time.elapsed());
                
                return Ok(Some(QueueWorkItem {
                    entry,
                    queue_key: queue_key_string,
                }));
            }
            
            // This item is in-flight, continue to next
        }
    }
    
    /// Claim multiple work items for batch processing (up to batch_size items)
    pub fn claim_work_items_batch(&self, timeout_ms: u64, batch_size: usize) -> KairosResult<Vec<QueueWorkItem>> {
        let start_time = Instant::now();
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        
        let mut claimed_items = Vec::new();
        let mut iter = self.partition.iter();
        
        // Process items until we have enough or run out
        while claimed_items.len() < batch_size {
            let item = match iter.next() {
                Some(item) => item,
                None => {
                    // Queue is empty
                    if claimed_items.is_empty() {
                        self.metrics.oldest_entry_age_seconds.set(0.0);
                    }
                    break; // No more items
                }
            };
            
            let (key, value) = item
                .map_err(|e| {
                    self.metrics.dequeue_errors.inc();
                    KairosError::internal(format!("Failed to read from queue: {}", e))
                })?;
                
            // Deserialize the entry using MessagePack
            let mut entry: QueueEntry = match rmp_serde::from_slice(&value) {
                Ok(entry) => entry,
                Err(e) => {
                    self.metrics.dequeue_errors.inc();
                    // Remove corrupted entry
                    debug!("Failed to deserialize queue entry, removing corrupted entry: {}", e);
                    self.partition.remove(key)
                        .map_err(|e| KairosError::internal(format!("Failed to remove corrupted entry: {}", e)))?;
                    self.queue_size.fetch_sub(1, Ordering::Relaxed);
                    self.metrics.current_size.set(self.queue_size.load(Ordering::Relaxed) as f64);
                    continue; // Try next entry
                }
            };
            
            // Check if item is available or timed out
            let is_available = match entry.in_flight_since {
                None => true,  // Available
                Some(in_flight_time) => {
                    // Check if timed out (crashed worker)
                    let age_ms = (now_ns.saturating_sub(in_flight_time)) / 1_000_000;
                    if age_ms > timeout_ms {
                        warn!("Found timed-out in-flight item ({}ms old), reclaiming", age_ms);
                        true  // Reclaim timed-out item
                    } else {
                        false  // Still being processed
                    }
                }
            };
            
            if is_available {
                // Extract the queue key string before moving the key
                let queue_key_string = String::from_utf8_lossy(&key).to_string();
                
                // Claim this item atomically by marking it in-flight
                entry.in_flight_since = Some(now_ns);
                
                // Serialize and update the entry
                let updated_data = rmp_serde::to_vec(&entry).map_err(|e| {
                    KairosError::validation(format!("Failed to serialize updated entry: {}", e))
                })?;
                
                self.partition.insert(key, updated_data).map_err(|e| {
                    KairosError::validation(format!("Failed to update in-flight entry: {}", e))
                })?;
                
                // Update metrics for oldest entry age
                let entry_age_seconds = (now_ns.saturating_sub(entry.timestamp_ns)) as f64 / 1_000_000_000.0;
                self.metrics.oldest_entry_age_seconds.set(entry_age_seconds);
                
                claimed_items.push(QueueWorkItem {
                    entry,
                    queue_key: queue_key_string,
                });
            }
        }
        
        let duration = start_time.elapsed();
        self.metrics.dequeue_duration.observe(duration.as_secs_f64());
        
        if !claimed_items.is_empty() {
            trace!("Claimed {} work items from persistent queue in {:?}", claimed_items.len(), duration);
        }
        
        Ok(claimed_items)
    }
    
    /// Remove a specific item from the queue after successful processing
    pub fn remove_processed_item(&self, queue_key: &str, batch_size: usize) -> KairosResult<()> {
        let start_time = Instant::now();
        
        // Check if the item actually exists before trying to remove it
        match self.partition.get(queue_key.as_bytes()) {
            Ok(Some(_)) => {
                // Item exists, proceed with removal
            }
            Ok(None) => {
                // Item doesn't exist - likely already removed
                warn!("Attempted to remove non-existent queue item '{}'", queue_key);
                return Ok(()); // Not an error - item is already gone
            }
            Err(e) => {
                // Error accessing item
                warn!("Failed to check queue item '{}': {}", queue_key, e);
                return Ok(()); // Treat as already gone
            }
        }
        
        // Remove the processed entry by its key
        self.partition.remove(queue_key.as_bytes())
            .map_err(|e| {
                self.metrics.dequeue_errors.inc();
                KairosError::internal(format!("Failed to remove processed entry: {}", e))
            })?;
        
        // Update metrics including dequeue_total for the data points processed
        // Use saturating_sub to prevent underflow
        let old_size = self.queue_size.load(Ordering::Relaxed);
        if old_size > 0 {
            self.queue_size.fetch_sub(1, Ordering::Relaxed);
        } else {
            error!("Queue size already at 0, cannot decrement");
        }
        self.metrics.current_size.set(self.queue_size.load(Ordering::Relaxed) as f64);
        self.metrics.dequeue_duration.observe(start_time.elapsed().as_secs_f64());
        self.metrics.dequeue_total.inc_by(batch_size as f64);  // Track dequeued data points
        self.metrics.batch_size.observe(batch_size as f64);     // Track batch size
        
        trace!("Removed processed item '{}' with {} points from queue in {:?}", 
               queue_key, batch_size, start_time.elapsed());
        Ok(())
    }
    
    /// Mark an item as failed (unclaim it for retry)
    pub fn mark_item_failed(&self, queue_key: &str) -> KairosResult<()> {
        let start_time = Instant::now();
        
        // Get the current entry
        let value = match self.partition.get(queue_key.as_bytes()) {
            Ok(Some(value)) => value,
            Ok(None) => {
                warn!("Attempted to unclaim non-existent queue item '{}'", queue_key);
                return Ok(()); // Item doesn't exist, nothing to do
            }
            Err(e) => {
                warn!("Failed to get queue item '{}': {}", queue_key, e);
                return Ok(()); // Item doesn't exist, nothing to do
            }
        };
        
        // Deserialize the entry
        let mut entry: QueueEntry = rmp_serde::from_slice(&value).map_err(|e| {
            KairosError::validation(format!("Failed to deserialize entry for unclaiming: {}", e))
        })?;
        
        // Unclaim the item (make it available for retry)
        entry.in_flight_since = None;
        
        // Serialize and update
        let updated_data = rmp_serde::to_vec(&entry).map_err(|e| {
            KairosError::validation(format!("Failed to serialize unclaimed entry: {}", e))
        })?;
        
        self.partition.insert(queue_key.as_bytes(), updated_data).map_err(|e| {
            KairosError::validation(format!("Failed to update unclaimed entry: {}", e))
        })?;
        
        trace!("Unclaimed item '{}' for retry in {:?}", queue_key, start_time.elapsed());
        Ok(())
    }
    
    
    /// Get current queue size
    pub fn size(&self) -> u64 {
        self.queue_size.load(Ordering::Relaxed)
    }
    
    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }
    
    /// Flush any pending writes to disk
    pub fn flush(&self) -> KairosResult<()> {
        // Fjall handles this automatically with its LSM structure
        Ok(())
    }
    
    /// Get queue metrics reference
    pub fn metrics(&self) -> &QueueMetrics {
        &self.metrics
    }
    
    /// Get disk usage in bytes for the queue data directory
    pub fn get_disk_usage_bytes(&self) -> Result<u64> {
        let mut total_size = 0u64;
        
        // Recursively calculate size of all files in the data directory
        for entry in std::fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            
            if metadata.is_file() {
                total_size += metadata.len();
            } else if metadata.is_dir() {
                // Recursively calculate subdirectory sizes
                total_size += Self::calculate_dir_size(&entry.path())?;
            }
        }
        
        Ok(total_size)
    }
    
    /// Helper to recursively calculate directory size
    fn calculate_dir_size(dir: &Path) -> Result<u64> {
        let mut size = 0u64;
        
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            
            if metadata.is_file() {
                size += metadata.len();
            } else if metadata.is_dir() {
                size += Self::calculate_dir_size(&entry.path())?;
            }
        }
        
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kairosdb_core::time::Timestamp;
    use tempfile::TempDir;
    
    #[tokio::test]
    async fn test_persistent_queue_basic_operations() {
        use std::sync::atomic::AtomicU32;
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        
        let temp_dir = TempDir::new().unwrap();
        let queue = PersistentQueue::new(temp_dir.path().join(format!("test_{}", id))).await.unwrap();
        
        // Test enqueue
        let data_point = DataPoint::new_long("test.metric", Timestamp::now(), 42);
        queue.enqueue(data_point.clone()).unwrap();
        
        assert_eq!(queue.size(), 1);
        assert!(!queue.is_empty());
        
        // Test claim_work_items_batch (the proper crash-safe method)
        let work_items = queue.claim_work_items_batch(1000, 10).unwrap();
        assert_eq!(work_items.len(), 1);
        assert_eq!(work_items[0].entry.batch.points.len(), 1);
        assert_eq!(work_items[0].entry.batch.points[0].metric, "test.metric");
        
        // Remove the processed item
        queue.remove_processed_item(&work_items[0].queue_key, work_items[0].entry.batch.points.len()).unwrap();
        assert_eq!(queue.size(), 0);
        assert!(queue.is_empty());
    }
}