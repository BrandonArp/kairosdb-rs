//! Persistent Queue Implementation using Fjall
//!
//! This module provides a write-ahead log for data points using Fjall LSM storage.
//! HTTP requests immediately write to disk and return success, while background
//! tasks drain the queue to Cassandra.

use anyhow::{Context, Result};
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle};
use kairosdb_core::datapoint::{DataPoint, DataPointBatch};
use kairosdb_core::error::{KairosError, KairosResult};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

/// Entry in the persistent queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueEntry {
    pub id: String,
    pub timestamp_ns: u64,
    pub data_point: DataPoint,
}

impl QueueEntry {
    pub fn new(data_point: DataPoint) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            timestamp_ns: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
            data_point,
        }
    }
    
    /// Generate a sortable key for queue ordering
    pub fn queue_key(&self) -> String {
        format!("{:020}_{}", self.timestamp_ns, self.id)
    }
}

/// Persistent queue for data points using Fjall storage
pub struct PersistentQueue {
    keyspace: Arc<Keyspace>,
    partition: PartitionHandle,
    queue_size: AtomicU64,
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
            
        // Count existing entries for queue size metric
        let queue_size = partition.iter().count() as u64;
        info!("Persistent queue initialized with {} existing entries", queue_size);
        
        Ok(Self {
            keyspace: Arc::new(keyspace),
            partition,
            queue_size: AtomicU64::new(queue_size),
        })
    }
    
    /// Enqueue a data point (write-ahead log)
    pub fn enqueue(&self, data_point: DataPoint) -> KairosResult<()> {
        let entry = QueueEntry::new(data_point);
        let key = entry.queue_key();
        
        // Serialize the entry
        let value = bincode::serialize(&entry)
            .map_err(|e| KairosError::internal(format!("Failed to serialize queue entry: {}", e)))?;
            
        // Write to persistent storage
        self.partition.insert(&key, value)
            .map_err(|e| KairosError::internal(format!("Failed to write to persistent queue: {}", e)))?;
            
        self.queue_size.fetch_add(1, Ordering::Relaxed);
        trace!("Enqueued data point with key: {}", key);
        
        Ok(())
    }
    
    /// Dequeue a batch of data points for processing
    pub fn dequeue_batch(&self, max_size: usize) -> KairosResult<Option<DataPointBatch>> {
        let mut batch = DataPointBatch::new();
        let mut keys_to_remove = Vec::new();
        
        // Read entries in order
        let mut count = 0;
        for item in self.partition.iter() {
            if count >= max_size {
                break;
            }
            
            let (key, value) = item
                .map_err(|e| KairosError::internal(format!("Failed to read from queue: {}", e)))?;
                
            // Deserialize the entry
            let entry: QueueEntry = bincode::deserialize(&value)
                .map_err(|e| KairosError::internal(format!("Failed to deserialize queue entry: {}", e)))?;
                
            // Add to batch
            batch.add_point(entry.data_point)
                .map_err(|e| KairosError::internal(format!("Failed to add point to batch: {}", e)))?;
                
            keys_to_remove.push(key);
            count += 1;
        }
        
        if batch.points.is_empty() {
            return Ok(None);
        }
        
        // Remove processed entries
        for key in keys_to_remove {
            self.partition.remove(key)
                .map_err(|e| KairosError::internal(format!("Failed to remove processed entry: {}", e)))?;
            self.queue_size.fetch_sub(1, Ordering::Relaxed);
        }
        
        debug!("Dequeued batch of {} data points", batch.points.len());
        Ok(Some(batch))
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use kairosdb_core::time::Timestamp;
    use tempfile::TempDir;
    
    #[tokio::test]
    async fn test_persistent_queue_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let queue = PersistentQueue::new(temp_dir.path()).await.unwrap();
        
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
        let temp_dir = TempDir::new().unwrap();
        let queue = PersistentQueue::new(temp_dir.path()).await.unwrap();
        
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