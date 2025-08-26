//! Multi-Worker Cassandra Client with MPMC Channels
//!
//! This implementation uses Flume MPMC channels with dedicated worker tasks
//! for highly concurrent Cassandra writes with proper status tracking.

use async_trait::async_trait;
use flume::{Receiver, Sender};
use kairosdb_core::{
    cassandra::{CassandraValue, ColumnName, RowKey},
    datapoint::{DataPointBatch, DataPointValue},
    error::{KairosError, KairosResult},
    schema::StringIndexEntry,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace};

// ScyllaDB Rust driver imports
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::client::PoolSize;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::Consistency;

use crate::bloom_manager::BloomManager;
use crate::cassandra::{CassandraClient, CassandraStats};
use crate::config::CassandraConfig;

/// Work item sent to worker tasks via MPMC channel
#[derive(Debug)]
pub struct WorkItem {
    pub batch: DataPointBatch,
    pub queue_key: String, // Key to identify the queue item for completion
}

/// Response sent back from Cassandra workers to queue processor
#[derive(Debug, Clone)]
pub struct WorkResponse {
    pub queue_key: String,
    pub result: Result<usize, String>, // Ok(batch_size) or Err(error_message)
    pub processing_duration_ns: u64,
}

/// Shared session and prepared statements for all workers
#[derive(Clone)]
#[allow(dead_code)]
struct SharedCassandraResources {
    session: Arc<Session>,
    config: Arc<CassandraConfig>,
    insert_data_point: PreparedStatement,
    insert_row_key_time_index: PreparedStatement,
    insert_row_keys: PreparedStatement,
    insert_string_index: PreparedStatement,
}

/// Statistics for the multi-worker client
#[derive(Debug, Default)]
struct MultiWorkerStats {
    total_queries: AtomicU64,
    failed_queries: AtomicU64,
    total_datapoints: AtomicU64,
    connection_errors: AtomicU64,
    batches_processed: AtomicU64,

    // Detailed operation metrics
    datapoint_writes: AtomicU64,
    datapoint_write_errors: AtomicU64,
    index_writes: AtomicU64,
    index_write_errors: AtomicU64,

    // Worker metrics
    active_workers: AtomicU64,
    total_work_items_processed: AtomicU64,

    // Timing metrics (in nanoseconds)
    total_datapoint_write_time_ns: AtomicU64,
    total_index_write_time_ns: AtomicU64,
    total_batch_write_time_ns: AtomicU64,
}

/// Multi-worker Cassandra client with MPMC channels
#[allow(dead_code)]
pub struct MultiWorkerCassandraClient {
    work_tx: Sender<WorkItem>,
    response_rx: Receiver<WorkResponse>, // For backward compatibility with write_batch
    stats: Arc<MultiWorkerStats>,
    shared_resources: SharedCassandraResources, // Keep for schema operations
    shared_bloom_manager: Arc<BloomManager>, // Shared across all workers
    _worker_handles: Vec<tokio::task::JoinHandle<()>>, // Keep handles alive
}

impl MultiWorkerCassandraClient {
    /// Create a new multi-worker Cassandra client with external channels
    pub async fn new_with_channels(
        config: CassandraConfig,
        num_workers: Option<usize>,
        work_tx: Sender<WorkItem>,
        work_rx: Receiver<WorkItem>,
        response_tx: Sender<WorkResponse>,
        response_rx: Receiver<WorkResponse>,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> KairosResult<Self> {
        let config = Arc::new(config);
        let num_workers = num_workers.unwrap_or(200); // Default to 200 workers for high concurrency

        info!(
            "Initializing Multi-Worker Cassandra Client with {} workers",
            num_workers
        );

        // Build session with contact points (ONE session for all workers)
        let session = Self::create_session(&config).await?;
        let session = Arc::new(session);

        // Create schema BEFORE creating workers
        info!("Creating KairosDB schema if needed");
        Self::ensure_schema_with_session(&session, &config).await?;
        info!("KairosDB schema creation completed");

        // Prepare shared statements
        let shared_resources = Self::prepare_shared_resources(session, config).await?;

        // Create shared stats
        let stats = Arc::new(MultiWorkerStats::default());

        // Create shared bloom manager for all workers
        let shared_bloom_manager = Arc::new(BloomManager::new());

        // Spawn worker tasks
        let mut worker_handles = Vec::with_capacity(num_workers);
        for worker_id in 0..num_workers {
            let worker_handle = Self::spawn_worker(
                worker_id,
                work_rx.clone(),
                response_tx.clone(),
                shared_resources.clone(),
                stats.clone(),
                shared_bloom_manager.clone(),
                shutdown_token.clone(),
            );
            worker_handles.push(worker_handle);
        }

        stats
            .active_workers
            .store(num_workers as u64, Ordering::Relaxed);

        info!(
            "Multi-worker Cassandra client started with {} workers",
            num_workers
        );

        Ok(Self {
            work_tx,
            response_rx,
            stats,
            shared_resources: shared_resources.clone(),
            shared_bloom_manager,
            _worker_handles: worker_handles,
        })
    }

    /// Create schema using a specific session and config
    async fn ensure_schema_with_session(
        session: &Arc<Session>,
        config: &Arc<CassandraConfig>,
    ) -> KairosResult<()> {
        use kairosdb_core::schema::KairosSchema;

        let schema = KairosSchema::new(config.keyspace.clone(), 1);

        // Execute keyspace creation
        session
            .query_unpaged(schema.create_keyspace_cql(), ())
            .await
            .map_err(|e| KairosError::cassandra(format!("Failed to create keyspace: {}", e)))?;

        // Execute table creation statements
        let statements = vec![
            schema.create_data_points_table_cql(),
            schema.create_row_key_time_index_table_cql(),
            schema.create_string_index_table_cql(),
        ];

        for statement in statements {
            session.query_unpaged(statement, ()).await.map_err(|e| {
                KairosError::cassandra(format!("Failed to execute schema statement: {}", e))
            })?;
        }

        Ok(())
    }

    /// Create a new multi-worker Cassandra client (creates its own channels)
    pub async fn new(config: CassandraConfig, num_workers: Option<usize>) -> KairosResult<Self> {
        // Create MPMC channels for work distribution and responses
        let (work_tx, work_rx) = flume::unbounded::<WorkItem>();
        let (response_tx, response_rx) = flume::unbounded::<WorkResponse>();
        // Create a cancellation token for this instance
        let shutdown_token = CancellationToken::new();
        Self::new_with_channels(
            config,
            num_workers,
            work_tx,
            work_rx,
            response_tx,
            response_rx,
            shutdown_token,
        )
        .await
    }

    /// Create ScyllaDB session
    async fn create_session(config: &CassandraConfig) -> KairosResult<Session> {
        let mut session_builder = SessionBuilder::new();

        for contact_point in &config.contact_points {
            session_builder = session_builder.known_node(contact_point);
        }

        // Add authentication if configured
        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            session_builder = session_builder.user(username, password);
        }

        // Configure connection pool and settings
        session_builder = session_builder
            .connection_timeout(std::time::Duration::from_millis(
                config.connection_timeout_ms,
            ))
            .pool_size(PoolSize::PerHost(
                std::num::NonZero::new(config.max_connections)
                    .unwrap_or_else(|| std::num::NonZero::new(10).unwrap()),
            ))
            .default_execution_profile_handle(
                ExecutionProfile::builder()
                    .consistency(Consistency::LocalQuorum)
                    .request_timeout(Some(std::time::Duration::from_millis(
                        config.query_timeout_ms,
                    )))
                    .build()
                    .into_handle(),
            );

        let session = session_builder.build().await.map_err(|e| {
            KairosError::cassandra(format!("Failed to create ScyllaDB session: {}", e))
        })?;

        info!("ScyllaDB session established successfully");
        Ok(session)
    }

    /// Prepare shared statements for all workers
    async fn prepare_shared_resources(
        session: Arc<Session>,
        config: Arc<CassandraConfig>,
    ) -> KairosResult<SharedCassandraResources> {
        debug!("Preparing shared CQL statements for workers");

        // Prepare data point insert
        let data_point_query = format!(
            "INSERT INTO {}.data_points (key, column1, value) VALUES (?, ?, ?)",
            config.keyspace
        );
        let insert_data_point = session.prepare(data_point_query).await.map_err(|e| {
            KairosError::cassandra(format!("Failed to prepare data point statement: {}", e))
        })?;

        // Prepare row_key_time_index insert
        let row_key_time_index_query = format!(
            "INSERT INTO {}.row_key_time_index (metric, table_name, row_time, value) VALUES (?, ?, ?, ?)",
            config.keyspace
        );
        let insert_row_key_time_index =
            session
                .prepare(row_key_time_index_query)
                .await
                .map_err(|e| {
                    KairosError::cassandra(format!(
                        "Failed to prepare row_key_time_index statement: {}",
                        e
                    ))
                })?;

        // Prepare row_keys insert
        let row_keys_query = format!(
            "INSERT INTO {}.row_keys (metric, table_name, row_time, data_type, tags, mtime, value) VALUES (?, ?, ?, ?, ?, ?, ?)",
            config.keyspace
        );
        let insert_row_keys = session.prepare(row_keys_query).await.map_err(|e| {
            KairosError::cassandra(format!("Failed to prepare row_keys statement: {}", e))
        })?;

        // Prepare string index insert
        let string_index_query = format!(
            "INSERT INTO {}.string_index (key, column1, value) VALUES (?, ?, ?)",
            config.keyspace
        );
        let insert_string_index = session.prepare(string_index_query).await.map_err(|e| {
            KairosError::cassandra(format!("Failed to prepare string index statement: {}", e))
        })?;

        info!("All shared CQL statements prepared successfully");

        Ok(SharedCassandraResources {
            session,
            config,
            insert_data_point,
            insert_row_key_time_index,
            insert_row_keys,
            insert_string_index,
        })
    }

    /// Spawn a worker task that processes batches sequentially
    fn spawn_worker(
        worker_id: usize,
        work_rx: Receiver<WorkItem>,
        response_tx: Sender<WorkResponse>,
        shared_resources: SharedCassandraResources,
        stats: Arc<MultiWorkerStats>,
        shared_bloom_manager: Arc<BloomManager>,
        shutdown_token: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            // info!("Worker {} started", worker_id);
            // Use shared bloom manager instead of individual ones

            loop {
                let work_item = tokio::select! {
                    result = work_rx.recv_async() => {
                        match result {
                            Ok(item) => item,
                            Err(_) => {
                                // Channel closed, worker should exit
                                break;
                            }
                        }
                    }
                    _ = shutdown_token.cancelled() => {
                        // Shutdown requested
                        trace!("Worker {} received shutdown signal", worker_id);
                        break;
                    }
                };
                let batch_start = std::time::Instant::now();
                let batch_size = work_item.batch.points.len();

                // info!("Worker {} processing batch of {} points", worker_id, batch_size);

                // Process the batch sequentially in this worker
                let result = Self::process_batch_sequentially(
                    &shared_resources,
                    &work_item.batch,
                    &shared_bloom_manager,
                    &stats,
                )
                .await;

                let batch_duration = batch_start.elapsed();
                stats
                    .total_batch_write_time_ns
                    .fetch_add(batch_duration.as_nanos() as u64, Ordering::Relaxed);
                stats
                    .total_work_items_processed
                    .fetch_add(1, Ordering::Relaxed);

                // Create response for queue processor
                let response = match &result {
                    Ok(_) => {
                        trace!(
                            "Worker {} completed batch of {} points successfully in {:?}",
                            worker_id,
                            batch_size,
                            batch_duration
                        );
                        stats.batches_processed.fetch_add(1, Ordering::Relaxed);

                        WorkResponse {
                            queue_key: work_item.queue_key.clone(),
                            result: Ok(batch_size),
                            processing_duration_ns: batch_duration.as_nanos() as u64,
                        }
                    }
                    Err(e) => {
                        error!(
                            "Worker {} failed to process batch of {} points after {:?}: {}",
                            worker_id, batch_size, batch_duration, e
                        );
                        stats.failed_queries.fetch_add(1, Ordering::Relaxed);

                        WorkResponse {
                            queue_key: work_item.queue_key.clone(),
                            result: Err(e.to_string()),
                            processing_duration_ns: batch_duration.as_nanos() as u64,
                        }
                    }
                };

                // Send response back via MPMC channel (BLOCKING - must not drop responses)
                // info!("Worker {} sending response for batch {}", worker_id, work_item.queue_key);
                if let Err(e) = response_tx.send(response) {
                    error!(
                        "Worker {} failed to send response (channel disconnected): {}",
                        worker_id, e
                    );
                    break; // Channel disconnected, worker should exit
                }
                // info!("Worker {} successfully sent response for {}", worker_id, work_item.queue_key);
            }

            info!("Worker {} shutting down", worker_id);
        })
    }

    /// Process a batch sequentially (called by individual workers)
    async fn process_batch_sequentially(
        resources: &SharedCassandraResources,
        batch: &DataPointBatch,
        bloom_manager: &Arc<BloomManager>,
        stats: &Arc<MultiWorkerStats>,
    ) -> KairosResult<()> {
        if batch.points.is_empty() {
            trace!("Empty batch, nothing to write");
            return Ok(());
        }

        let internal_start = std::time::Instant::now();
        trace!(
            "Sequential worker processing batch of {} data points",
            batch.points.len()
        );

        stats
            .total_datapoints
            .fetch_add(batch.points.len() as u64, Ordering::Relaxed);

        // Write all data points sequentially
        let datapoints_start = std::time::Instant::now();
        for data_point in &batch.points {
            let row_key = RowKey::from_data_point(data_point);
            let column_name = ColumnName::from_timestamp(data_point.timestamp);
            let cassandra_value = CassandraValue::from_data_point_value(&data_point.value, None);

            let row_key_bytes = row_key.to_bytes();
            let column_key_bytes = column_name.to_bytes();
            let value_bytes = &cassandra_value.bytes;

            // Write to data_points table
            match resources
                .session
                .execute_unpaged(
                    &resources.insert_data_point,
                    (row_key_bytes.clone(), column_key_bytes, value_bytes.clone()),
                )
                .await
            {
                Ok(_) => {
                    stats.total_queries.fetch_add(1, Ordering::Relaxed);
                    stats.datapoint_writes.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    stats.failed_queries.fetch_add(1, Ordering::Relaxed);
                    stats.datapoint_write_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(KairosError::cassandra(format!(
                        "Failed to write data point: {}",
                        e
                    )));
                }
            }

            // Write to row_key_time_index table for time-based queries
            Self::write_row_key_time_index_entry(resources, &row_key, stats).await?;
        }

        let datapoints_duration = datapoints_start.elapsed();
        stats
            .total_datapoint_write_time_ns
            .fetch_add(datapoints_duration.as_nanos() as u64, Ordering::Relaxed);
        trace!(
            "Successfully wrote {} data points to Cassandra in {:?}",
            batch.points.len(),
            datapoints_duration
        );

        // Write indexes sequentially with bloom filter deduplication
        let indexes_start = std::time::Instant::now();
        let index_count =
            Self::write_indexes_sequentially(resources, batch, bloom_manager, stats).await?;
        let indexes_duration = indexes_start.elapsed();

        stats
            .total_index_write_time_ns
            .fetch_add(indexes_duration.as_nanos() as u64, Ordering::Relaxed);
        trace!(
            "Completed writing {} indexes in {:?}",
            index_count,
            indexes_duration
        );

        let total_duration = internal_start.elapsed();
        trace!(
            "Sequential worker batch completed successfully in {:?} total",
            total_duration
        );
        Ok(())
    }

    /// Write indexes sequentially with bloom filter deduplication
    async fn write_indexes_sequentially(
        resources: &SharedCassandraResources,
        batch: &DataPointBatch,
        bloom_manager: &Arc<BloomManager>,
        stats: &Arc<MultiWorkerStats>,
    ) -> KairosResult<u64> {
        let mut metric_names = HashSet::new();
        let mut tag_names = HashSet::new();
        let mut tag_values = HashSet::new();
        let mut row_keys_entries = HashMap::new();

        // Collect all unique metric names, tags, and row_keys entries
        for data_point in &batch.points {
            metric_names.insert(data_point.metric.as_str());

            for (tag_key, tag_value) in data_point.tags.iter() {
                tag_names.insert(tag_key.as_str());
                tag_values.insert(tag_value.as_str());
            }

            // Collect unique row_keys entries (one per unique combination of metric, row_time, data_type, tags)
            let row_key = RowKey::from_data_point(data_point);
            let data_type = match &data_point.value {
                DataPointValue::Long(_) => "kairos_long",
                DataPointValue::Double(_) => "kairos_double",
                DataPointValue::Text(_) => "kairos_string",
                DataPointValue::Complex { .. } => "kairos_complex",
                DataPointValue::Binary(_) => "kairos_binary",
                DataPointValue::Histogram(_) => "kairos_histogram_v2",
            };

            // Create a unique string key for this row_keys entry (since TagSet doesn't implement Hash/Eq)
            let tags_string: String = {
                let mut tag_pairs: Vec<_> = data_point
                    .tags
                    .iter()
                    .map(|(k, v)| format!("{}={}", k.as_str(), v.as_str()))
                    .collect();
                tag_pairs.sort(); // Ensure consistent ordering
                tag_pairs.join(",")
            };

            let row_keys_entry_key = format!(
                "{}:data_points:{}:{}:{}",
                data_point.metric.as_str(),
                row_key.row_time.timestamp_millis(),
                data_type,
                tags_string
            );
            row_keys_entries.insert(row_keys_entry_key, data_point.clone());
        }

        trace!(
            "Writing indexes for {} metrics, {} tag keys, {} tag values, {} row_keys entries",
            metric_names.len(),
            tag_names.len(),
            tag_values.len(),
            row_keys_entries.len()
        );

        let mut indexes_written = 0u64;

        // Write metric name indexes (with bloom filter deduplication)
        for metric_name in metric_names {
            let bloom_key = format!("metric_name:{}", metric_name);
            if bloom_manager.should_write_index(&bloom_key) {
                let entry = StringIndexEntry::metric_name(metric_name);
                Self::write_string_index_sequentially(resources, &entry, stats).await?;
                indexes_written += 1;
            }
        }

        // Write row_keys entries (with bloom filter deduplication)
        for (unique_key, data_point) in &row_keys_entries {
            // Use the unique key for bloom filtering
            let bloom_key = format!("row_keys:{}", unique_key);
            if bloom_manager.should_write_index(&bloom_key) {
                let row_key = RowKey::from_data_point(data_point);
                let metric_name = data_point.metric.as_str();
                let table_name = "data_points";
                let row_time = scylla::value::CqlTimestamp(row_key.row_time.timestamp_millis());

                let data_type = match &data_point.value {
                    DataPointValue::Long(_) => "kairos_long",
                    DataPointValue::Double(_) => "kairos_double",
                    DataPointValue::Text(_) => "kairos_string",
                    DataPointValue::Complex { .. } => "kairos_complex",
                    DataPointValue::Binary(_) => "kairos_binary",
                    DataPointValue::Histogram(_) => "kairos_histogram_v2",
                };

                let tags_map: HashMap<String, String> = data_point
                    .tags
                    .iter()
                    .map(|(k, v)| (k.as_str().to_string(), v.as_str().to_string()))
                    .collect();

                // Generate proper UUID v1 timeuuid for mtime
                let uuid = uuid::Uuid::now_v1(&[1, 2, 3, 4, 5, 6]);
                let mtime = scylla::value::CqlTimeuuid::from_bytes(uuid.into_bytes());
                let value: Option<String> = None;

                match resources
                    .session
                    .execute_unpaged(
                        &resources.insert_row_keys,
                        (
                            metric_name,
                            table_name,
                            row_time,
                            data_type,
                            tags_map,
                            mtime,
                            value,
                        ),
                    )
                    .await
                {
                    Ok(_) => {
                        stats.total_queries.fetch_add(1, Ordering::Relaxed);
                        stats.index_writes.fetch_add(1, Ordering::Relaxed);
                        indexes_written += 1;
                    }
                    Err(e) => {
                        stats.failed_queries.fetch_add(1, Ordering::Relaxed);
                        stats.index_write_errors.fetch_add(1, Ordering::Relaxed);
                        return Err(KairosError::cassandra(format!(
                            "Failed to write row_keys: {}",
                            e
                        )));
                    }
                }
            }
        }

        // NOTE: Java KairosDB does NOT write tag_names or tag_values during ingestion
        // Only metric_names are written to string_index during ingestion
        // Commenting out tag index writes to match Java KairosDB behavior exactly

        // // Write tag name indexes (with bloom filter deduplication)
        // for tag_name in tag_names {
        //     let bloom_key = format!("tag_name:{}", tag_name);
        //     if bloom_manager.should_write_index(&bloom_key) {
        //         let entry = StringIndexEntry::tag_name(tag_name);
        //         Self::write_string_index_sequentially(resources, &entry, stats).await?;
        //         indexes_written += 1;
        //     }
        // }

        // // Write tag value indexes (with bloom filter deduplication)
        // for tag_value in tag_values {
        //     let bloom_key = format!("tag_value:{}", tag_value);
        //     if bloom_manager.should_write_index(&bloom_key) {
        //         let entry = StringIndexEntry::tag_value(tag_value);
        //         Self::write_string_index_sequentially(resources, &entry, stats).await?;
        //         indexes_written += 1;
        //     }
        // }

        trace!("All {} indexes written successfully", indexes_written);
        Ok(indexes_written)
    }

    /// Write a string index entry sequentially
    async fn write_string_index_sequentially(
        resources: &SharedCassandraResources,
        entry: &StringIndexEntry,
        stats: &Arc<MultiWorkerStats>,
    ) -> KairosResult<()> {
        let key_bytes = entry.key().to_bytes();
        let column_name = entry.index_column();
        let value_bytes = vec![0u8]; // Empty value for string index

        match resources
            .session
            .execute_unpaged(
                &resources.insert_string_index,
                (key_bytes, column_name, value_bytes),
            )
            .await
        {
            Ok(_) => {
                stats.total_queries.fetch_add(1, Ordering::Relaxed);
                stats.index_writes.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                stats.failed_queries.fetch_add(1, Ordering::Relaxed);
                stats.index_write_errors.fetch_add(1, Ordering::Relaxed);
                Err(KairosError::cassandra(format!(
                    "Failed to write string index: {}",
                    e
                )))
            }
        }
    }

    /// Write a row key time index entry for time-based queries
    async fn write_row_key_time_index_entry(
        resources: &SharedCassandraResources,
        row_key: &RowKey,
        stats: &Arc<MultiWorkerStats>,
    ) -> KairosResult<()> {
        // Extract values for row_key_time_index table
        let metric_name = row_key.metric.as_str();
        let table_name = "data_points"; // Always data_points table
        let row_time = scylla::value::CqlTimestamp(row_key.row_time.timestamp_millis());

        // Java KairosDB stores null in the value column - the presence of the row is what matters
        let value: Option<String> = None;

        match resources
            .session
            .execute_unpaged(
                &resources.insert_row_key_time_index,
                (metric_name, table_name, row_time, value),
            )
            .await
        {
            Ok(_) => {
                stats.total_queries.fetch_add(1, Ordering::Relaxed);
                stats.index_writes.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                stats.failed_queries.fetch_add(1, Ordering::Relaxed);
                stats.index_write_errors.fetch_add(1, Ordering::Relaxed);
                Err(KairosError::cassandra(format!(
                    "Failed to write row_key_time_index: {}",
                    e
                )))
            }
        }
    }
}

#[async_trait]
impl CassandraClient for MultiWorkerCassandraClient {
    async fn write_batch(&self, batch: &DataPointBatch) -> KairosResult<()> {
        let start = std::time::Instant::now();

        // Create work item with a unique queue key for response tracking
        let queue_key = format!("direct_{}", uuid::Uuid::new_v4());
        let work_item = WorkItem {
            batch: batch.clone(),
            queue_key: queue_key.clone(),
        };

        trace!(
            "MultiWorker: Sending batch of {} points to worker pool",
            batch.points.len()
        );

        // Send work item to worker pool via MPMC channel
        self.work_tx
            .send(work_item)
            .map_err(|_| KairosError::cassandra("Worker pool unavailable"))?;

        // Wait for response from worker via response channel
        loop {
            match self.response_rx.recv_async().await {
                Ok(response) if response.queue_key == queue_key => {
                    // Found our response
                    match response.result {
                        Ok(_) => break Ok(()),
                        Err(e) => break Err(KairosError::cassandra(e)),
                    }
                }
                Ok(_) => {
                    // Not our response, continue waiting
                    continue;
                }
                Err(_) => {
                    break Err(KairosError::cassandra("Response channel closed"));
                }
            }
        }?;

        let duration = start.elapsed();
        trace!("MultiWorker: Batch write completed in {:?}", duration);
        Ok(())
    }

    async fn health_check(&self) -> KairosResult<bool> {
        // Check if worker pool is available and active
        let active_workers = self.stats.active_workers.load(Ordering::Relaxed);
        Ok(active_workers > 0 && !self.work_tx.is_disconnected())
    }

    fn get_stats(&self) -> CassandraStats {
        self.get_stats_internal(false)
    }

    fn get_detailed_stats(&self) -> CassandraStats {
        self.get_stats_internal(true)
    }

    async fn ensure_schema(&self) -> KairosResult<()> {
        // Schema already created during initialization
        info!("Schema already ensured during client initialization");
        Ok(())
    }
}

impl MultiWorkerCassandraClient {
    fn get_stats_internal(&self, include_ones_count: bool) -> CassandraStats {
        let bloom_stats = self.shared_bloom_manager.get_stats_with_options(include_ones_count);

        // Calculate averages
        let datapoint_writes = self.stats.datapoint_writes.load(Ordering::Relaxed);
        let index_writes = self.stats.index_writes.load(Ordering::Relaxed);
        let total_batches = self.stats.batches_processed.load(Ordering::Relaxed).max(1);

        let avg_datapoint_write_time_ms = if datapoint_writes > 0 {
            (self
                .stats
                .total_datapoint_write_time_ns
                .load(Ordering::Relaxed) as f64)
                / (datapoint_writes as f64)
                / 1_000_000.0
        } else {
            0.0
        };

        let avg_index_write_time_ms = if index_writes > 0 {
            (self.stats.total_index_write_time_ns.load(Ordering::Relaxed) as f64)
                / (index_writes as f64)
                / 1_000_000.0
        } else {
            0.0
        };

        let avg_batch_write_time_ms = if total_batches > 0 {
            (self.stats.total_batch_write_time_ns.load(Ordering::Relaxed) as f64)
                / (total_batches as f64)
                / 1_000_000.0
        } else {
            0.0
        };

        CassandraStats {
            total_queries: self.stats.total_queries.load(Ordering::Relaxed),
            failed_queries: self.stats.failed_queries.load(Ordering::Relaxed),
            total_datapoints_written: self.stats.total_datapoints.load(Ordering::Relaxed),
            avg_batch_size: if total_batches > 0 {
                self.stats.total_datapoints.load(Ordering::Relaxed) as f64 / total_batches as f64
            } else {
                0.0
            },
            connection_errors: self.stats.connection_errors.load(Ordering::Relaxed),
            bloom_filter_in_overlap_period: bloom_stats.in_overlap_period,
            bloom_filter_primary_age_seconds: bloom_stats.primary_age_seconds,
            bloom_filter_expected_items: bloom_stats.expected_items,
            bloom_filter_false_positive_rate: bloom_stats.false_positive_rate,
            bloom_filter_primary_memory_bytes: bloom_stats.primary_memory_bytes,
            bloom_filter_secondary_memory_bytes: bloom_stats.secondary_memory_bytes,
            bloom_filter_total_memory_bytes: bloom_stats.total_memory_bytes,
            bloom_filter_primary_ones_count: bloom_stats.primary_ones_count,
            bloom_filter_secondary_ones_count: bloom_stats.secondary_ones_count,

            // Detailed operation metrics
            datapoint_writes: self.stats.datapoint_writes.load(Ordering::Relaxed),
            datapoint_write_errors: self.stats.datapoint_write_errors.load(Ordering::Relaxed),
            index_writes: self.stats.index_writes.load(Ordering::Relaxed),
            index_write_errors: self.stats.index_write_errors.load(Ordering::Relaxed),
            prepared_statement_cache_hits: 0, // Not applicable to multi-worker model
            prepared_statement_cache_misses: 0,

            // Worker-specific metrics instead of concurrency
            current_concurrent_requests: self.stats.active_workers.load(Ordering::Relaxed),
            max_concurrent_requests_reached: self.stats.active_workers.load(Ordering::Relaxed),
            avg_semaphore_wait_time_ms: 0.0, // Not applicable - workers consume sequentially

            // Timing metrics
            avg_datapoint_write_time_ms,
            avg_index_write_time_ms,
            avg_batch_write_time_ms,
        }
    }
}
