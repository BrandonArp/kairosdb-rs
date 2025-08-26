//! High-performance in-memory queue with optional disk spillover
//!
//! This replaces the heavy Fjall LSM-tree based persistent queue which was consuming
//! 25-30% CPU time during load tests. This implementation prioritizes:
//! 1. Zero-copy in-memory operations for 99% of use cases
//! 2. Simple disk spillover only when memory limits are exceeded
//! 3. Minimal serialization overhead using efficient encoding

use kairosdb_core::{
    datapoint::DataPointBatch,
    error::{KairosError, KairosResult},
};
use parking_lot::RwLock;
use prometheus::{Counter, Gauge, Histogram, HistogramOpts};
use std::{
    collections::VecDeque,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    sync::Arc,
    time::Instant,
};
use tracing::{debug, error, info, trace, warn};

/// High-performance queue that keeps data in memory with disk spillover
#[derive(Clone)]
pub struct HighPerformanceQueue {
    inner: Arc<QueueInner>,
}

struct QueueInner {
    // In-memory queue (primary storage)
    memory_queue: RwLock<VecDeque<DataPointBatch>>,

    // Configuration
    max_memory_batches: usize,
    disk_dir: PathBuf,

    // Metrics
    pub metrics: QueueMetrics,

    // State tracking
    disk_file_counter: AtomicU64,
    memory_size: AtomicUsize,
    total_enqueued: AtomicU64,
    total_dequeued: AtomicU64,
}

#[derive(Clone)]
pub struct QueueMetrics {
    // Counters
    pub enqueue_ops: Counter,
    pub dequeue_ops: Counter,
    pub enqueue_errors: Counter,
    pub dequeue_errors: Counter,
    pub disk_spillovers: Counter,
    pub disk_recoveries: Counter,

    // Gauges
    pub queue_size: Gauge,
    pub memory_queue_size: Gauge,
    pub disk_queue_size: Gauge,
    pub memory_usage_bytes: Gauge,
    pub disk_usage_bytes: Gauge,

    // Histograms
    pub enqueue_duration: Histogram,
    pub dequeue_duration: Histogram,
    pub serialize_duration: Histogram,
    pub deserialize_duration: Histogram,
}

impl QueueMetrics {
    pub fn new() -> KairosResult<Self> {
        Ok(Self {
            enqueue_ops: Counter::new("queue_enqueue_ops_total", "Total enqueue operations")
                .map_err(|e| {
                    KairosError::validation(format!("Failed to create enqueue_ops metric: {}", e))
                })?,
            dequeue_ops: Counter::new("queue_dequeue_ops_total", "Total dequeue operations")
                .map_err(|e| {
                    KairosError::validation(format!("Failed to create dequeue_ops metric: {}", e))
                })?,
            enqueue_errors: Counter::new("queue_enqueue_errors_total", "Total enqueue errors")
                .map_err(|e| {
                    KairosError::validation(format!(
                        "Failed to create enqueue_errors metric: {}",
                        e
                    ))
                })?,
            dequeue_errors: Counter::new("queue_dequeue_errors_total", "Total dequeue errors")
                .map_err(|e| {
                    KairosError::validation(format!(
                        "Failed to create dequeue_errors metric: {}",
                        e
                    ))
                })?,
            disk_spillovers: Counter::new(
                "queue_disk_spillovers_total",
                "Total disk spillover events",
            )
            .map_err(|e| {
                KairosError::validation(format!("Failed to create disk_spillovers metric: {}", e))
            })?,
            disk_recoveries: Counter::new(
                "queue_disk_recoveries_total",
                "Total disk recovery events",
            )
            .map_err(|e| {
                KairosError::validation(format!("Failed to create disk_recoveries metric: {}", e))
            })?,

            queue_size: Gauge::new("queue_size", "Total items in queue").map_err(|e| {
                KairosError::validation(format!("Failed to create queue_size metric: {}", e))
            })?,
            memory_queue_size: Gauge::new("queue_memory_size", "Items in memory queue").map_err(
                |e| {
                    KairosError::validation(format!(
                        "Failed to create memory_queue_size metric: {}",
                        e
                    ))
                },
            )?,
            disk_queue_size: Gauge::new("queue_disk_size", "Items in disk queue").map_err(|e| {
                KairosError::validation(format!("Failed to create disk_queue_size metric: {}", e))
            })?,
            memory_usage_bytes: Gauge::new("queue_memory_usage_bytes", "Memory usage in bytes")
                .map_err(|e| {
                    KairosError::validation(format!(
                        "Failed to create memory_usage_bytes metric: {}",
                        e
                    ))
                })?,
            disk_usage_bytes: Gauge::new("queue_disk_usage_bytes", "Disk usage in bytes").map_err(
                |e| {
                    KairosError::validation(format!(
                        "Failed to create disk_usage_bytes metric: {}",
                        e
                    ))
                },
            )?,

            enqueue_duration: Histogram::with_opts(HistogramOpts::new(
                "queue_enqueue_duration_seconds",
                "Enqueue operation duration",
            ))
            .map_err(|e| {
                KairosError::validation(format!("Failed to create enqueue_duration metric: {}", e))
            })?,
            dequeue_duration: Histogram::with_opts(HistogramOpts::new(
                "queue_dequeue_duration_seconds",
                "Dequeue operation duration",
            ))
            .map_err(|e| {
                KairosError::validation(format!("Failed to create dequeue_duration metric: {}", e))
            })?,
            serialize_duration: Histogram::with_opts(HistogramOpts::new(
                "queue_serialize_duration_seconds",
                "Batch serialization duration",
            ))
            .map_err(|e| {
                KairosError::validation(format!(
                    "Failed to create serialize_duration metric: {}",
                    e
                ))
            })?,
            deserialize_duration: Histogram::with_opts(HistogramOpts::new(
                "queue_deserialize_duration_seconds",
                "Batch deserialization duration",
            ))
            .map_err(|e| {
                KairosError::validation(format!(
                    "Failed to create deserialize_duration metric: {}",
                    e
                ))
            })?,
        })
    }
}

impl HighPerformanceQueue {
    pub fn new<P: Into<PathBuf>>(disk_dir: P, max_memory_batches: usize) -> KairosResult<Self> {
        let disk_dir = disk_dir.into();

        // Ensure directory exists
        if !disk_dir.exists() {
            fs::create_dir_all(&disk_dir).map_err(|e| {
                KairosError::validation(format!("Failed to create queue directory: {}", e))
            })?;
        }

        let metrics = QueueMetrics::new()?;

        let inner = QueueInner {
            memory_queue: RwLock::new(VecDeque::new()),
            max_memory_batches,
            disk_dir,
            metrics,
            disk_file_counter: AtomicU64::new(0),
            memory_size: AtomicUsize::new(0),
            total_enqueued: AtomicU64::new(0),
            total_dequeued: AtomicU64::new(0),
        };

        let queue = Self {
            inner: Arc::new(inner),
        };

        // Recover any existing disk files on startup
        queue.recover_from_disk()?;

        info!(
            "HighPerformanceQueue initialized with max_memory_batches={}",
            max_memory_batches
        );
        Ok(queue)
    }

    /// Enqueue a batch - always succeeds by using memory first, then disk spillover
    pub fn enqueue(&self, batch: DataPointBatch) -> KairosResult<()> {
        let _timer = self.inner.metrics.enqueue_duration.start_timer();
        let start = Instant::now();

        trace!("Enqueueing batch of {} data points", batch.points.len());

        // Try to add to memory queue first
        {
            let mut memory_queue = self.inner.memory_queue.write();

            if memory_queue.len() < self.inner.max_memory_batches {
                // Fast path: add to memory
                let batch_size = estimate_batch_size(&batch);
                memory_queue.push_back(batch);

                self.inner
                    .memory_size
                    .fetch_add(batch_size, Ordering::Relaxed);
                self.inner.total_enqueued.fetch_add(1, Ordering::Relaxed);

                // Update metrics
                self.inner.metrics.enqueue_ops.inc();
                self.inner.metrics.queue_size.inc();
                self.inner.metrics.memory_queue_size.inc();
                self.inner.metrics.memory_usage_bytes.add(batch_size as f64);

                trace!(
                    "Successfully enqueued batch to memory in {:?}",
                    start.elapsed()
                );
                return Ok(());
            }
        }

        // Memory queue is full - spill to disk
        self.spill_to_disk(batch)?;

        trace!(
            "Successfully enqueued batch with disk spillover in {:?}",
            start.elapsed()
        );
        Ok(())
    }

    /// Dequeue a batch - returns None if queue is empty
    pub fn dequeue(&self) -> KairosResult<Option<DataPointBatch>> {
        let _timer = self.inner.metrics.dequeue_duration.start_timer();

        // Try memory queue first (fast path)
        {
            let mut memory_queue = self.inner.memory_queue.write();
            if let Some(batch) = memory_queue.pop_front() {
                let batch_size = estimate_batch_size(&batch);

                self.inner
                    .memory_size
                    .fetch_sub(batch_size, Ordering::Relaxed);
                self.inner.total_dequeued.fetch_add(1, Ordering::Relaxed);

                // Update metrics
                self.inner.metrics.dequeue_ops.inc();
                self.inner.metrics.queue_size.dec();
                self.inner.metrics.memory_queue_size.dec();
                self.inner.metrics.memory_usage_bytes.sub(batch_size as f64);

                trace!(
                    "Dequeued batch of {} points from memory",
                    batch.points.len()
                );
                return Ok(Some(batch));
            }
        }

        // Memory queue is empty - check disk
        self.recover_from_disk_single()
    }

    /// Dequeue multiple batches up to limit - optimized for batch processing
    pub fn dequeue_batch(&self, _max_batches: usize) -> KairosResult<Option<DataPointBatch>> {
        // For now, just dequeue single batches to maintain compatibility
        // TODO: Could optimize this later to merge multiple small batches
        self.dequeue()
    }

    /// Get current queue size (memory + disk)
    pub fn size(&self) -> u64 {
        let _memory_size = self.inner.memory_queue.read().len() as u64;
        let total_enqueued = self.inner.total_enqueued.load(Ordering::Relaxed);
        let total_dequeued = self.inner.total_dequeued.load(Ordering::Relaxed);

        total_enqueued - total_dequeued
    }

    /// Get disk usage in bytes
    pub fn get_disk_usage_bytes(&self) -> KairosResult<u64> {
        let mut total_size = 0u64;

        for entry in fs::read_dir(&self.inner.disk_dir)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_file() {
                total_size += metadata.len();
            }
        }

        self.inner.metrics.disk_usage_bytes.set(total_size as f64);
        Ok(total_size)
    }

    /// Get queue metrics
    pub fn metrics(&self) -> &QueueMetrics {
        &self.inner.metrics
    }

    /// Spill oldest batch from memory to disk
    fn spill_to_disk(&self, new_batch: DataPointBatch) -> KairosResult<()> {
        let spill_start = Instant::now();

        // Get the batch to spill (could be the new one or oldest from memory)
        let batch_to_spill = {
            let mut memory_queue = self.inner.memory_queue.write();
            if let Some(oldest_batch) = memory_queue.pop_front() {
                // Spill oldest, add new batch to memory
                let batch_size = estimate_batch_size(&new_batch);
                memory_queue.push_back(new_batch);

                // Update memory metrics
                let old_size = estimate_batch_size(&oldest_batch);
                self.inner
                    .memory_size
                    .fetch_add(batch_size, Ordering::Relaxed);
                self.inner
                    .memory_size
                    .fetch_sub(old_size, Ordering::Relaxed);
                self.inner.metrics.memory_usage_bytes.add(batch_size as f64);
                self.inner.metrics.memory_usage_bytes.sub(old_size as f64);

                oldest_batch
            } else {
                // Empty memory queue - spill the new batch directly
                new_batch
            }
        };

        // Write to disk file
        let file_id = self.inner.disk_file_counter.fetch_add(1, Ordering::Relaxed);
        let file_path = self
            .inner
            .disk_dir
            .join(format!("batch_{:010}.dat", file_id));

        let serialize_start = Instant::now();
        let serialized =
            bincode::serde::encode_to_vec(&batch_to_spill, bincode::config::standard()).map_err(
                |e| KairosError::parse(format!("Failed to serialize batch for disk: {}", e)),
            )?;
        self.inner
            .metrics
            .serialize_duration
            .observe(serialize_start.elapsed().as_secs_f64());

        fs::write(&file_path, serialized).map_err(|e| {
            KairosError::validation(format!("Failed to write batch to disk: {}", e))
        })?;

        // Update metrics
        self.inner.total_enqueued.fetch_add(1, Ordering::Relaxed);
        self.inner.metrics.enqueue_ops.inc();
        self.inner.metrics.queue_size.inc();
        self.inner.metrics.disk_queue_size.inc();
        self.inner.metrics.disk_spillovers.inc();

        debug!(
            "Spilled batch of {} points to disk in {:?}",
            batch_to_spill.points.len(),
            spill_start.elapsed()
        );
        Ok(())
    }

    /// Recover batches from disk files
    fn recover_from_disk(&self) -> KairosResult<()> {
        let mut recovered_count = 0;

        if !self.inner.disk_dir.exists() {
            return Ok(());
        }

        // Get all batch files sorted by name (which includes sequence number)
        let mut entries: Vec<_> = fs::read_dir(&self.inner.disk_dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .map(|name| name.starts_with("batch_") && name.ends_with(".dat"))
                    .unwrap_or(false)
            })
            .collect();

        entries.sort_by_key(|entry| entry.file_name());

        for entry in entries {
            match self.recover_single_file(&entry.path()) {
                Ok(_) => {
                    recovered_count += 1;
                    // Delete the file after successful recovery
                    if let Err(e) = fs::remove_file(entry.path()) {
                        warn!("Failed to delete recovered file {:?}: {}", entry.path(), e);
                    }
                }
                Err(e) => {
                    error!("Failed to recover file {:?}: {}", entry.path(), e);
                    // Don't fail startup, just log the error
                }
            }
        }

        if recovered_count > 0 {
            info!("Recovered {} batches from disk", recovered_count);
        }

        Ok(())
    }

    /// Recover a single batch from disk (used during dequeue when memory is empty)
    fn recover_from_disk_single(&self) -> KairosResult<Option<DataPointBatch>> {
        if !self.inner.disk_dir.exists() {
            return Ok(None);
        }

        // Find oldest disk file
        let mut entries: Vec<_> = fs::read_dir(&self.inner.disk_dir)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .map(|name| name.starts_with("batch_") && name.ends_with(".dat"))
                    .unwrap_or(false)
            })
            .collect();

        if entries.is_empty() {
            return Ok(None);
        }

        entries.sort_by_key(|entry| entry.file_name());
        let oldest_file = &entries[0];

        // Recover the batch
        match self.recover_single_file(&oldest_file.path()) {
            Ok(batch) => {
                // Delete the file
                if let Err(e) = fs::remove_file(oldest_file.path()) {
                    warn!(
                        "Failed to delete recovered file {:?}: {}",
                        oldest_file.path(),
                        e
                    );
                }

                // Update metrics
                self.inner.total_dequeued.fetch_add(1, Ordering::Relaxed);
                self.inner.metrics.dequeue_ops.inc();
                self.inner.metrics.queue_size.dec();
                self.inner.metrics.disk_queue_size.dec();
                self.inner.metrics.disk_recoveries.inc();

                trace!("Recovered batch of {} points from disk", batch.points.len());
                Ok(Some(batch))
            }
            Err(e) => {
                error!("Failed to recover file {:?}: {}", oldest_file.path(), e);
                self.inner.metrics.dequeue_errors.inc();
                Err(e)
            }
        }
    }

    /// Recover a single file from disk
    fn recover_single_file(&self, file_path: &PathBuf) -> KairosResult<DataPointBatch> {
        let deserialize_start = Instant::now();

        let data = fs::read(file_path)
            .map_err(|e| KairosError::validation(format!("Failed to read batch file: {}", e)))?;

        let (batch, _): (DataPointBatch, _) =
            bincode::serde::decode_from_slice(&data, bincode::config::standard()).map_err(|e| {
                KairosError::parse(format!("Failed to deserialize batch from disk: {}", e))
            })?;

        self.inner
            .metrics
            .deserialize_duration
            .observe(deserialize_start.elapsed().as_secs_f64());
        Ok(batch)
    }
}

/// Estimate memory size of a batch (rough approximation)
fn estimate_batch_size(batch: &DataPointBatch) -> usize {
    // Rough estimation: each data point is ~100 bytes on average
    // (metric name, tags, timestamp, value, etc.)
    batch.points.len() * 100
}
