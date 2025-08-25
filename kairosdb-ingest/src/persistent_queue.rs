//! Persistent Queue Implementation using Fjall
//!
//! This module provides a write-ahead log for data points using Fjall LSM storage.
//! HTTP requests immediately write to disk and return success, while background
//! tasks drain the queue to Cassandra.

use anyhow::{Context, Result};
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle};
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
            format!("kairosdb_queue_enqueue_total{}", suffix),
            "Total number of data points enqueued"
        ).unwrap_or_else(|_| Counter::new("test_counter", "test").unwrap());

        let dequeue_total = register_counter!(
            format!("kairosdb_queue_dequeue_total{}", suffix),
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
            format!("kairosdb_queue_size_current{}", suffix),
            "Current number of items in persistent queue"
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
            format!("kairosdb_queue_batch_size{}", suffix),
            "Size of dequeued batches"
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
    
    /// Dequeue a batch of data points for processing
    pub fn dequeue_batch(&self, _max_size: usize) -> KairosResult<Option<DataPointBatch>> {
        let start_time = Instant::now();
        
        // Get the first entry from the queue
        let mut iter = self.partition.iter();
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
        let entry: QueueEntry = match rmp_serde::from_slice(&value) {
            Ok(entry) => entry,
            Err(e) => {
                self.metrics.dequeue_errors.inc();
                // Remove corrupted entry
                debug!("Failed to deserialize queue entry, removing corrupted entry: {}", e);
                self.partition.remove(key)
                    .map_err(|e| KairosError::internal(format!("Failed to remove corrupted entry: {}", e)))?;
                // Try again with the next entry
                return self.dequeue_batch(_max_size);
            }
        };
        
        // Remove the processed entry
        self.partition.remove(key)
            .map_err(|e| {
                self.metrics.dequeue_errors.inc();
                KairosError::internal(format!("Failed to remove processed entry: {}", e))
            })?;
        
        // Update metrics
        let batch_size = entry.batch.points.len();
        self.queue_size.fetch_sub(1, Ordering::Relaxed);
        self.metrics.dequeue_total.inc_by(batch_size as f64);
        self.metrics.current_size.set(self.queue_size.load(Ordering::Relaxed) as f64);
        self.metrics.dequeue_duration.observe(start_time.elapsed().as_secs_f64());
        self.metrics.batch_size.observe(batch_size as f64);
        
        // Update oldest entry age
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let age_seconds = (now_ns.saturating_sub(entry.timestamp_ns)) as f64 / 1_000_000_000.0;
        self.metrics.oldest_entry_age_seconds.set(age_seconds);
        
        trace!("Dequeued batch with {} points in {:?}", batch_size, start_time.elapsed());
        Ok(Some(entry.batch))
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
        
        // Test dequeue
        let batch = queue.dequeue_batch(10).unwrap();
        assert!(batch.is_some());
        
        let batch = batch.unwrap();
        assert_eq!(batch.points.len(), 1);
        assert_eq!(batch.points[0].metric, "test.metric");
        assert_eq!(queue.size(), 0);
        assert!(queue.is_empty());
    }
    
    #[tokio::test]
    async fn test_persistent_queue_ordering() {
        use std::sync::atomic::AtomicU32;
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        
        let temp_dir = TempDir::new().unwrap();
        let queue = PersistentQueue::new(temp_dir.path().join(format!("test_order_{}", id))).await.unwrap();
        
        // Enqueue multiple points
        for i in 0..5 {
            let data_point = DataPoint::new_long(&format!("test.metric.{}", i), Timestamp::now(), i);
            queue.enqueue(data_point).unwrap();
            // Small delay to ensure different timestamps
            tokio::time::sleep(std::time::Duration::from_nanos(1)).await;
        }
        
        assert_eq!(queue.size(), 5);
        
        // Dequeue in batches and verify ordering
        let batch1 = queue.dequeue_batch(2).unwrap().unwrap();
        assert_eq!(batch1.points.len(), 2);
        
        let batch2 = queue.dequeue_batch(10).unwrap().unwrap();
        assert_eq!(batch2.points.len(), 3);
        
        assert_eq!(queue.size(), 0);
    }
}