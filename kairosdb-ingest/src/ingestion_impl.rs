// Implementation methods continuation for IngestionService

impl IngestionService {
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

    /// Start worker threads for batch processing
    async fn start_workers(&mut self, mut batch_receiver: Receiver<DataPointBatch>) -> Result<()> {
        info!("Starting {} worker threads", self.config.ingestion.worker_threads);
        
        for worker_id in 0..self.config.ingestion.worker_threads {
            let cassandra_client = self.cassandra_client.clone();
            let config = self.config.clone();
            let metrics = self.metrics.clone();
            let semaphore = self.processing_semaphore.clone();
            let backpressure = self.backpressure_active.clone();
            
            let handle = tokio::spawn(async move {
                Self::worker_loop(
                    worker_id,
                    cassandra_client,
                    config,
                    metrics,
                    semaphore,
                    backpressure,
                    &mut batch_receiver,
                ).await;
            });
            
            self.worker_handles.push(handle);
        }
        
        Ok(())
    }
    
    /// Worker loop for processing batches
    async fn worker_loop(
        worker_id: usize,
        cassandra_client: Arc<CassandraClient>,
        config: Arc<IngestConfig>,
        metrics: IngestionMetrics,
        semaphore: Arc<Semaphore>,
        backpressure: Arc<AtomicU64>,
        batch_receiver: &mut Receiver<DataPointBatch>,
    ) {
        info!("Worker {} started", worker_id);
        
        while let Some(batch) = batch_receiver.recv().await {
            // Acquire semaphore permit for processing
            let _permit = semaphore.acquire().await.unwrap();
            
            let start_time = Instant::now();
            let batch_size = batch.len();
            
            // Update queue size
            metrics.queue_size.fetch_sub(1, Ordering::Relaxed);
            metrics.prometheus_metrics.queue_size_gauge.dec();
            
            match Self::process_batch(&cassandra_client, &batch, &config).await {
                Ok(_) => {
                    // Update success metrics
                    metrics.datapoints_ingested.fetch_add(batch_size as u64, Ordering::Relaxed);
                    metrics.batches_processed.fetch_add(1, Ordering::Relaxed);
                    metrics.prometheus_metrics.datapoints_total.inc_by(batch_size as f64);
                    metrics.prometheus_metrics.batches_total.inc();
                    
                    // Update timing metrics
                    let processing_time = start_time.elapsed();
                    let processing_ms = processing_time.as_millis() as u64;
                    metrics.avg_batch_time_ms.store(processing_ms, Ordering::Relaxed);
                    metrics.prometheus_metrics.batch_duration_histogram.observe(processing_time.as_secs_f64());
                    
                    // Update last batch time
                    *metrics.last_batch_time.write() = Some(start_time);
                    
                    // Clear backpressure if it was active
                    backpressure.store(0, Ordering::Relaxed);
                    
                    debug!("Worker {} processed batch of {} points in {:?}", 
                           worker_id, batch_size, processing_time);
                }
                Err(e) => {
                    error!("Worker {} failed to process batch: {}", worker_id, e);
                    
                    // Update error metrics
                    metrics.ingestion_errors.fetch_add(1, Ordering::Relaxed);
                    metrics.prometheus_metrics.errors_total.inc();
                    
                    match e.category() {
                        "cassandra" => {
                            metrics.cassandra_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        "validation" => {
                            metrics.validation_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {}
                    }
                }
            }
        }
        
        warn!("Worker {} stopped", worker_id);
    }
    
    /// Process a single batch of data points
    async fn process_batch(
        cassandra_client: &CassandraClient,
        batch: &DataPointBatch,
        config: &IngestConfig,
    ) -> KairosResult<()> {
        let start = Instant::now();
        
        // Create batch writer with configured settings
        let batch_writer = cassandra_client.batch_writer(
            config.ingestion.max_batch_size,
            config.batch_timeout(),
        );
        
        // Write batch to Cassandra
        batch_writer.write_batch(batch).await?;
        
        debug!("Batch processed successfully in {:?}", start.elapsed());
        Ok(())
    }
    
    /// Start monitoring tasks for system metrics
    async fn start_monitoring_tasks(&mut self) -> Result<()> {
        let system_monitor = self.system_monitor.clone();
        let memory_gauge = self.metrics.prometheus_metrics.memory_usage_gauge.clone();
        let memory_atomic = self.metrics.memory_usage.clone();
        
        // Memory monitoring task
        let handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(10));
            
            loop {
                interval.tick().await;
                
                let mut system = system_monitor.write();
                system.refresh_memory();
                let memory_used = system.used_memory();
                
                memory_gauge.set(memory_used as f64);
                memory_atomic.store(memory_used, Ordering::Relaxed);
            }
        });
        
        self.worker_handles.push(handle);
        Ok(())
    }
    
    /// Get current memory usage
    async fn get_current_memory_usage(&self) -> usize {
        let system = self.system_monitor.read();
        system.used_memory() as usize
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
    
    /// Graceful shutdown
    pub async fn shutdown(&mut self) -> Result<()> {
        info!("Shutting down ingestion service");
        
        // Cancel all worker tasks
        for handle in &self.worker_handles {
            handle.abort();
        }
        
        // Wait for tasks to complete (with timeout)
        let shutdown_timeout = Duration::from_secs(30);
        let futures: Vec<_> = self.worker_handles.drain(..).collect();
        
        match timeout(shutdown_timeout, async {
            for handle in futures {
                let _ = handle.await;
            }
        }).await {
            Ok(_) => info!("All workers shut down cleanly"),
            Err(_) => warn!("Some workers did not shut down within timeout"),
        }
        
        info!("Ingestion service shutdown complete");
        Ok(())
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