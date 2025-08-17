//! High-performance ingestion service with async batch processing
//!
//! This module provides the core ingestion service that handles:
//! - Async batch processing with configurable limits
//! - Backpressure handling when memory limits are reached
//! - Connection pooling and error recovery
//! - Metrics collection and monitoring

use anyhow::{Context, Result};
use dashmap::DashMap;
use futures::stream::{self, StreamExt};
use kairosdb_core::{
    datapoint::{DataPoint, DataPointBatch},
    error::{KairosError, KairosResult},
};
use parking_lot::RwLock;
use prometheus::{
    register_counter, register_gauge, register_histogram, Counter, Gauge, Histogram,
};
use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use sysinfo::{System, SystemExt};
use tokio::{
    sync::{
        mpsc::{self, Receiver, Sender},
        Semaphore,
    },
    task::JoinHandle,
    time::{interval, sleep, timeout},
};
use tracing::{debug, error, info, warn};

use crate::{
    cassandra_client::CassandraClient,
    config::IngestConfig,
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
    /// Current queue size
    pub queue_size: Arc<AtomicUsize>,
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
    pub queue_size_gauge: Gauge,
    pub memory_usage_gauge: Gauge,
    pub batch_duration_histogram: Histogram,
}

/// Main ingestion service that handles data point processing
pub struct IngestionService {
    /// Configuration
    config: Arc<IngestConfig>,
    
    /// Cassandra client for data persistence
    cassandra_client: Arc<CassandraClient>,
    
    /// Metrics collection
    metrics: IngestionMetrics,
    
    /// Batch processing queue
    batch_sender: Sender<DataPointBatch>,
    
    /// Semaphore for controlling concurrent processing
    processing_semaphore: Arc<Semaphore>,
    
    /// System monitor for memory tracking
    system_monitor: Arc<RwLock<System>>,
    
    /// Worker handles for graceful shutdown
    worker_handles: Vec<JoinHandle<()>>,
    
    /// Backpressure handler
    backpressure_active: Arc<AtomicU64>,
}

impl IngestionService {
    /// Create a new ingestion service
    pub async fn new(config: Arc<IngestConfig>) -> Result<Self> {
        config.validate().context("Invalid configuration")?;
        
        info!("Initializing ingestion service");
        
        // Initialize Cassandra client
        let cassandra_client = Arc::new(
            CassandraClient::new(config.cassandra.clone())
                .await
                .context("Failed to initialize Cassandra client")?
        );
        
        // Initialize Prometheus metrics
        let prometheus_metrics = PrometheusMetrics::new()?;
        
        // Initialize ingestion metrics
        let metrics = IngestionMetrics {
            datapoints_ingested: Arc::new(AtomicU64::new(0)),
            batches_processed: Arc::new(AtomicU64::new(0)),
            ingestion_errors: Arc::new(AtomicU64::new(0)),
            validation_errors: Arc::new(AtomicU64::new(0)),
            cassandra_errors: Arc::new(AtomicU64::new(0)),
            queue_size: Arc::new(AtomicUsize::new(0)),
            memory_usage: Arc::new(AtomicU64::new(0)),
            avg_batch_time_ms: Arc::new(AtomicU64::new(0)),
            last_batch_time: Arc::new(RwLock::new(None)),
            prometheus_metrics,
        };
        
        // Create batch processing channel with bounded capacity for backpressure
        let (batch_sender, batch_receiver) = mpsc::channel(config.ingestion.max_queue_size);
        
        // Create semaphore for controlling concurrent processing
        let processing_semaphore = Arc::new(Semaphore::new(config.ingestion.worker_threads));
        
        // Initialize system monitor for memory tracking
        let mut system = System::new_all();
        system.refresh_all();
        let system_monitor = Arc::new(RwLock::new(system));
        
        // Initialize backpressure tracking
        let backpressure_active = Arc::new(AtomicU64::new(0));
        
        let mut service = Self {
            config: config.clone(),
            cassandra_client,
            metrics,
            batch_sender,
            processing_semaphore,
            system_monitor,
            worker_handles: Vec::new(),
            backpressure_active,
        };
        
        // Start worker threads
        service.start_workers(batch_receiver).await?;
        
        // Start monitoring tasks
        service.start_monitoring_tasks().await?;
        
        info!("Ingestion service initialized with {} worker threads", config.ingestion.worker_threads);
        Ok(service)
    }
    
    /// Submit a batch for ingestion with backpressure handling
    pub async fn ingest_batch(&self, batch: DataPointBatch) -> KairosResult<()> {
        let start = Instant::now();
        
        // Check memory usage and activate backpressure if needed
        let current_memory = self.get_current_memory_usage().await;
        let max_memory = self.config.max_memory_bytes();
        
        if current_memory > max_memory {
            self.backpressure_active.store(1, Ordering::Relaxed);
            warn!("Memory limit exceeded: {} > {}, activating backpressure", current_memory, max_memory);
            return Err(KairosError::rate_limit("Memory limit exceeded".to_string()));
        }
        
        // Validate batch if validation is enabled
        if self.config.ingestion.enable_validation {
            batch.validate_self().map_err(|e| {
                self.metrics.validation_errors.fetch_add(1, Ordering::Relaxed);
                e
            })?;
        }
        
        // Update queue size metric
        let queue_size = self.metrics.queue_size.fetch_add(1, Ordering::Relaxed);
        self.metrics.prometheus_metrics.queue_size_gauge.set(queue_size as f64);
        
        // Send batch to processing queue with timeout
        let send_result = timeout(
            Duration::from_millis(100), // Short timeout for backpressure
            self.batch_sender.send(batch)
        ).await;
        
        match send_result {
            Ok(Ok(_)) => {
                debug!("Batch queued successfully in {:?}", start.elapsed());
                Ok(())
            }
            Ok(Err(_)) => {
                self.metrics.queue_size.fetch_sub(1, Ordering::Relaxed);
                Err(KairosError::internal("Failed to queue batch for processing"))
            }
            Err(_) => {
                self.metrics.queue_size.fetch_sub(1, Ordering::Relaxed);
                self.backpressure_active.store(1, Ordering::Relaxed);
                Err(KairosError::rate_limit("Queue is full, backpressure activated".to_string()))
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
            queue_size: self.metrics.queue_size.load(Ordering::Relaxed),
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
        let queue_size = self.metrics.queue_size.load(Ordering::Relaxed);
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
        let datapoints_total = register_counter!(
            "kairosdb_datapoints_ingested_total",
            "Total number of data points ingested"
        )?;
        
        let batches_total = register_counter!(
            "kairosdb_batches_processed_total", 
            "Total number of batches processed"
        )?;
        
        let errors_total = register_counter!(
            "kairosdb_ingestion_errors_total",
            "Total number of ingestion errors"
        )?;
        
        let queue_size_gauge = register_gauge!(
            "kairosdb_queue_size",
            "Current queue size"
        )?;
        
        let memory_usage_gauge = register_gauge!(
            "kairosdb_memory_usage_bytes",
            "Current memory usage in bytes"
        )?;
        
        let batch_duration_histogram = register_histogram!(
            "kairosdb_batch_duration_seconds",
            "Batch processing duration"
        )?;
        
        Ok(Self {
            datapoints_total,
            batches_total,
            errors_total,
            queue_size_gauge,
            memory_usage_gauge,
            batch_duration_histogram,
        })
    }
}

impl Default for IngestionMetrics {
    fn default() -> Self {
        Self {
            datapoints_ingested: Arc::new(AtomicU64::new(0)),
            batches_processed: Arc::new(AtomicU64::new(0)),
            ingestion_errors: Arc::new(AtomicU64::new(0)),
            validation_errors: Arc::new(AtomicU64::new(0)),
            cassandra_errors: Arc::new(AtomicU64::new(0)),
            queue_size: Arc::new(AtomicUsize::new(0)),
            memory_usage: Arc::new(AtomicU64::new(0)),
            avg_batch_time_ms: Arc::new(AtomicU64::new(0)),
            last_batch_time: Arc::new(RwLock::new(None)),
            prometheus_metrics: PrometheusMetrics::new().unwrap(),
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
    use kairosdb_core::time::Timestamp;
    
    #[tokio::test]
    #[ignore] // Requires Cassandra
    async fn test_ingestion_service_creation() {
        let config = Arc::new(IngestConfig::default());
        let service = IngestionService::new(config).await;
        assert!(service.is_ok());
    }
    
    #[tokio::test]
    #[ignore] // Requires Cassandra
    async fn test_batch_ingestion() {
        let config = Arc::new(IngestConfig::default());
        let service = IngestionService::new(config).await.unwrap();
        
        let mut batch = DataPointBatch::new();
        batch.add_point(DataPoint::new_long("test.metric", Timestamp::now(), 42)).unwrap();
        
        let result = service.ingest_batch(batch).await;
        assert!(result.is_ok());
        
        // Give some time for background processing
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        let metrics = service.get_metrics_snapshot();
        assert!(metrics.batches_processed >= 1);
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