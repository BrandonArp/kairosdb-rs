//! Single Writer Cassandra Client - Eliminates Arc cloning and task spawning overhead
//!
//! This implementation uses a single writer task with producer/consumer model
//! to eliminate the CPU hotspots identified in the flame graph analysis.

use async_trait::async_trait;
use futures::{future, stream::FuturesUnordered, StreamExt};
use tokio::sync::{mpsc, oneshot, Semaphore};
use kairosdb_core::{
    cassandra::{CassandraValue, ColumnName, RowKey},
    datapoint::DataPointBatch,
    error::{KairosError, KairosResult},
    schema::{StringIndexEntry},
};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
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

/// Write command for the single writer task
#[derive(Debug)]
enum WriteCommand {
    DataPoint {
        data_point: kairosdb_core::datapoint::DataPoint,
    },
    Batch {
        batch: DataPointBatch,
        response_tx: oneshot::Sender<KairosResult<()>>,
    },
    Shutdown,
}

/// Production Cassandra client implementation using single writer task
#[derive(Clone)]
pub struct SingleWriterCassandraClient {
    write_tx: mpsc::UnboundedSender<WriteCommand>,
    stats: Arc<CassandraClientStats>,
}

/// Internal writer task state (shared across concurrent operations)
struct WriterTaskState {
    session: Arc<Session>,  // Arc needed for sharing across concurrent tasks
    config: Arc<CassandraConfig>,
    bloom_manager: BloomManager,
    stats: Arc<CassandraClientStats>,
    // Concurrency control
    semaphore: Arc<Semaphore>,
    // Prepared statements for performance
    insert_data_point: Option<PreparedStatement>,
    insert_row_key_index: Option<PreparedStatement>,
    insert_string_index: Option<PreparedStatement>,
    insert_row_keys: Option<PreparedStatement>,
    insert_row_key_time_index: Option<PreparedStatement>,
}

/// Internal statistics tracking
struct CassandraClientStats {
    total_queries: AtomicU64,
    failed_queries: AtomicU64,
    total_datapoints: AtomicU64,
    connection_errors: AtomicU64,
    batches_processed: AtomicU64,
    
    // Detailed Cassandra operation metrics
    datapoint_writes: AtomicU64,
    datapoint_write_errors: AtomicU64,
    index_writes: AtomicU64,
    index_write_errors: AtomicU64,
    prepared_statement_cache_hits: AtomicU64,
    prepared_statement_cache_misses: AtomicU64,
    
    // Concurrency metrics
    concurrent_requests: AtomicU64,
    max_concurrent_requests: AtomicU64,
    semaphore_wait_time_ns: AtomicU64,
    
    // Timing metrics (in nanoseconds for precision)
    total_datapoint_write_time_ns: AtomicU64,
    total_index_write_time_ns: AtomicU64,
    total_batch_write_time_ns: AtomicU64,
}

impl Default for CassandraClientStats {
    fn default() -> Self {
        Self {
            total_queries: AtomicU64::new(0),
            failed_queries: AtomicU64::new(0),
            total_datapoints: AtomicU64::new(0),
            connection_errors: AtomicU64::new(0),
            batches_processed: AtomicU64::new(0),
            
            // Detailed Cassandra operation metrics
            datapoint_writes: AtomicU64::new(0),
            datapoint_write_errors: AtomicU64::new(0),
            index_writes: AtomicU64::new(0),
            index_write_errors: AtomicU64::new(0),
            prepared_statement_cache_hits: AtomicU64::new(0),
            prepared_statement_cache_misses: AtomicU64::new(0),
            
            // Concurrency metrics
            concurrent_requests: AtomicU64::new(0),
            max_concurrent_requests: AtomicU64::new(0),
            semaphore_wait_time_ns: AtomicU64::new(0),
            
            // Timing metrics (in nanoseconds for precision)
            total_datapoint_write_time_ns: AtomicU64::new(0),
            total_index_write_time_ns: AtomicU64::new(0),
            total_batch_write_time_ns: AtomicU64::new(0),
        }
    }
}

impl SingleWriterCassandraClient {
    /// Create a new Cassandra client with single writer task
    pub async fn new(config: CassandraConfig) -> KairosResult<Self> {
        let config = Arc::new(config);

        info!("Initializing Single Writer Cassandra Client");

        // Build session with contact points
        let mut session_builder = SessionBuilder::new();

        for contact_point in &config.contact_points {
            session_builder = session_builder.known_node(contact_point);
        }

        // Add authentication if configured
        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            session_builder = session_builder.user(username, password);
        }

        // Configure connection pool and concurrency settings
        session_builder = session_builder
            .connection_timeout(std::time::Duration::from_millis(config.connection_timeout_ms))
            .pool_size(PoolSize::PerHost(
                std::num::NonZero::new(config.max_connections)
                    .unwrap_or_else(|| std::num::NonZero::new(10).unwrap())
            ))
            .default_execution_profile_handle(
                ExecutionProfile::builder()
                    .consistency(Consistency::LocalQuorum)
                    .request_timeout(Some(std::time::Duration::from_millis(config.query_timeout_ms)))
                    .build()
                    .into_handle()
            );

        let session = session_builder.build().await.map_err(|e| {
            KairosError::cassandra(format!("Failed to create ScyllaDB session: {}", e))
        })?;

        info!("ScyllaDB session established successfully");

        // Create shared stats
        let stats = Arc::new(CassandraClientStats::default());

        // Create semaphore for concurrency control (default 50 concurrent requests)
        let max_concurrent = config.max_concurrent_requests.unwrap_or(50);
        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        info!("Configured with {} max concurrent requests", max_concurrent);

        // Create writer task state (shared across concurrent operations)
        let mut writer_state = WriterTaskState {
            session: Arc::new(session),
            config: config.clone(),
            bloom_manager: BloomManager::new(),
            stats: stats.clone(),
            semaphore,
            insert_data_point: None,
            insert_row_key_index: None,
            insert_string_index: None,
            insert_row_keys: None,
            insert_row_key_time_index: None,
        };

        // Prepare statements
        writer_state.prepare_statements().await?;

        // Create channel for write commands
        let (write_tx, write_rx) = mpsc::unbounded_channel();

        // Spawn single writer task
        tokio::spawn(Self::writer_task(writer_state, write_rx));

        info!("Single writer task started successfully");

        Ok(Self {
            write_tx,
            stats,
        })
    }

    /// The high-concurrency writer task - handles multiple concurrent Cassandra writes
    async fn writer_task(state: WriterTaskState, mut write_rx: mpsc::UnboundedReceiver<WriteCommand>) {
        info!("High-concurrency writer task started");

        // Use FuturesUnordered to track outstanding requests efficiently
        let mut outstanding_requests = FuturesUnordered::new();
        let mut shutting_down = false;

        loop {
            tokio::select! {
                // Handle incoming commands
                command = write_rx.recv(), if !shutting_down => {
                    match command {
                        Some(WriteCommand::DataPoint { data_point }) => {
                            // Process individual data point (for future use)
                            let mut batch = DataPointBatch::new();
                            if let Err(e) = batch.add_point(data_point) {
                                error!("Failed to create batch from single data point: {}", e);
                                continue;
                            }
                            
                            // Spawn concurrent processing
                            let future = Self::process_batch_concurrently(state.clone_for_concurrent(), batch, None);
                            outstanding_requests.push(future);
                        }
                        Some(WriteCommand::Batch { batch, response_tx }) => {
                            trace!("Writer task: Queuing batch of {} points for concurrent processing", batch.points.len());
                            
                            // Spawn concurrent processing
                            let future = Self::process_batch_concurrently(state.clone_for_concurrent(), batch, Some(response_tx));
                            outstanding_requests.push(future);
                        }
                        Some(WriteCommand::Shutdown) => {
                            info!("Writer task received shutdown signal");
                            shutting_down = true;
                        }
                        None => {
                            info!("Writer task command channel closed");
                            shutting_down = true;
                        }
                    }
                }
                
                // Handle completed requests
                _ = outstanding_requests.next(), if !outstanding_requests.is_empty() => {
                    // Completed request is automatically removed from FuturesUnordered
                }
                
                // Exit when shutting down and no outstanding requests
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)), if shutting_down && outstanding_requests.is_empty() => {
                    break;
                }
            }
        }

        // Wait for any remaining outstanding requests to complete
        while let Some(_) = outstanding_requests.next().await {
            // Just wait for completion
        }

        info!("High-concurrency writer task stopped");
    }
    
    /// Process a batch concurrently with semaphore-controlled concurrency
    async fn process_batch_concurrently(
        state: WriterTaskState, 
        batch: DataPointBatch, 
        response_tx: Option<oneshot::Sender<KairosResult<()>>>
    ) {
        let batch_start = std::time::Instant::now();
        let batch_size = batch.points.len();
        
        // Acquire semaphore permit with timing
        let semaphore_start = std::time::Instant::now();
        let semaphore = Arc::clone(&state.semaphore);
        let _permit = match semaphore.acquire().await {
            Ok(permit) => {
                let wait_time = semaphore_start.elapsed();
                state.stats.semaphore_wait_time_ns.fetch_add(wait_time.as_nanos() as u64, Ordering::Relaxed);
                
                // Update concurrency metrics
                let current_concurrent = state.stats.concurrent_requests.fetch_add(1, Ordering::Relaxed) + 1;
                let max_concurrent = state.stats.max_concurrent_requests.load(Ordering::Relaxed);
                if current_concurrent > max_concurrent {
                    state.stats.max_concurrent_requests.store(current_concurrent, Ordering::Relaxed);
                }
                
                trace!("Acquired semaphore permit after {:?} wait (concurrent requests: {})", wait_time, current_concurrent);
                permit
            }
            Err(e) => {
                error!("Failed to acquire semaphore permit: {}", e);
                if let Some(tx) = response_tx {
                    let _ = tx.send(Err(KairosError::cassandra("Concurrency limit acquisition failed")));
                }
                return;
            }
        };
        
        // Process the batch - need to make this method work without mutable self
        let result = Self::write_batch_internal_static(state, &batch).await;
        let batch_duration = batch_start.elapsed();
        
        match &result {
            Ok(_) => trace!("Concurrent batch of {} points completed successfully in {:?}", batch_size, batch_duration),
            Err(e) => error!("Concurrent batch of {} points failed after {:?}: {}", batch_size, batch_duration, e),
        }
        
        // Send response back if requested
        if let Some(tx) = response_tx {
            let _ = tx.send(result);
        }
        
        // Permit is automatically dropped here, releasing the semaphore
    }
    
    /// Static version of write_batch_internal for concurrent processing
    async fn write_batch_internal_static(state: WriterTaskState, batch: &DataPointBatch) -> KairosResult<()> {
        if batch.points.is_empty() {
            trace!("Empty batch, nothing to write");
            return Ok(());
        }

        let internal_start = std::time::Instant::now();
        trace!("Concurrent writer processing batch of {} data points", batch.points.len());
        state.stats
            .total_datapoints
            .fetch_add(batch.points.len() as u64, Ordering::Relaxed);

        // Prepare all data point writes concurrently
        let prepared_stmt = state.insert_data_point.as_ref()
            .ok_or_else(|| KairosError::cassandra("Data point prepared statement not available"))?;
        
        let write_futures: Vec<_> = batch.points.iter().map(|data_point| {
            let row_key = RowKey::from_data_point(data_point);
            let column_name = ColumnName::from_timestamp(data_point.timestamp);
            let cassandra_value = CassandraValue::from_data_point_value(&data_point.value, None);
            
            let row_key_bytes = row_key.to_bytes();
            let column_key_bytes = column_name.to_bytes();
            let value_bytes = &cassandra_value.bytes;
            
            state.session.execute_unpaged(prepared_stmt, (row_key_bytes, column_key_bytes, value_bytes.clone()))
        }).collect();
        
        // Execute all data point writes concurrently
        let datapoints_start = std::time::Instant::now();
        trace!("Executing {} concurrent data point writes to Cassandra", batch.points.len());
        let results = future::try_join_all(write_futures).await;
        let datapoints_duration = datapoints_start.elapsed();
        
        // Update detailed stats based on results
        let datapoint_count = batch.points.len() as u64;
        match results {
            Ok(_) => {
                trace!("Successfully wrote {} data points to Cassandra in {:?}", batch.points.len(), datapoints_duration);
                state.stats.total_queries.fetch_add(datapoint_count, Ordering::Relaxed);
                state.stats.datapoint_writes.fetch_add(datapoint_count, Ordering::Relaxed);
                state.stats.prepared_statement_cache_hits.fetch_add(datapoint_count, Ordering::Relaxed);
                state.stats.total_datapoint_write_time_ns.fetch_add(datapoints_duration.as_nanos() as u64, Ordering::Relaxed);
                
                // Update concurrency metrics after successful completion
                state.stats.concurrent_requests.fetch_sub(1, Ordering::Relaxed);
            }
            Err(e) => {
                error!("Failed to write data points to Cassandra after {:?}: {}", datapoints_duration, e);
                state.stats.failed_queries.fetch_add(1, Ordering::Relaxed);
                state.stats.datapoint_write_errors.fetch_add(datapoint_count, Ordering::Relaxed);
                state.stats.total_datapoint_write_time_ns.fetch_add(datapoints_duration.as_nanos() as u64, Ordering::Relaxed);
                
                // Update concurrency metrics even on failure
                state.stats.concurrent_requests.fetch_sub(1, Ordering::Relaxed);
                return Err(KairosError::cassandra(format!("Failed to write data points: {}", e)));
            }
        }

        // Write indexes with bloom filter deduplication
        let indexes_start = std::time::Instant::now();
        trace!("Writing indexes for batch");
        match Self::write_indexes_static(&state, batch).await {
            Ok(index_count) => {
                let indexes_duration = indexes_start.elapsed();
                trace!("Completed writing {} indexes in {:?}", index_count, indexes_duration);
                state.stats.index_writes.fetch_add(index_count, Ordering::Relaxed);
                state.stats.total_index_write_time_ns.fetch_add(indexes_duration.as_nanos() as u64, Ordering::Relaxed);
            }
            Err(e) => {
                let indexes_duration = indexes_start.elapsed();
                error!("Failed to write indexes after {:?}: {}", indexes_duration, e);
                state.stats.index_write_errors.fetch_add(1, Ordering::Relaxed);
                state.stats.total_index_write_time_ns.fetch_add(indexes_duration.as_nanos() as u64, Ordering::Relaxed);
                return Err(e);
            }
        }

        let total_duration = internal_start.elapsed();
        state.stats.total_batch_write_time_ns.fetch_add(total_duration.as_nanos() as u64, Ordering::Relaxed);
        state.stats.batches_processed.fetch_add(1, Ordering::Relaxed);
        trace!("Concurrent writer batch completed successfully in {:?} total", total_duration);
        Ok(())
    }
    
    /// Static version of write_indexes for concurrent processing
    async fn write_indexes_static(state: &WriterTaskState, batch: &DataPointBatch) -> KairosResult<u64> {
        let mut metric_names = HashSet::new();
        let mut tag_names = HashSet::new();
        let mut tag_values = HashSet::new();

        // Collect all unique metric names and tags
        for data_point in &batch.points {
            metric_names.insert(data_point.metric.as_str());

            for (tag_key, tag_value) in data_point.tags.iter() {
                tag_names.insert(tag_key.as_str());
                tag_values.insert(tag_value.as_str());
            }
        }

        trace!(
            "Writing indexes for {} metrics, {} tag keys, {} tag values",
            metric_names.len(),
            tag_names.len(),
            tag_values.len()
        );

        let mut indexes_written = 0u64;
        let mut bloom_manager = state.bloom_manager.clone(); // Each concurrent task gets its own

        // Write metric name indexes (with bloom filter deduplication)
        for metric_name in metric_names {
            let bloom_key = format!("metric_name:{}", metric_name);
            if bloom_manager.should_write_index(&bloom_key) {
                let entry = StringIndexEntry::metric_name(metric_name);
                Self::write_string_index_static(&state, &entry).await?;
                indexes_written += 1;
            }
        }

        // Write tag name indexes (with bloom filter deduplication)
        for tag_name in tag_names {
            let bloom_key = format!("tag_name:{}", tag_name);
            if bloom_manager.should_write_index(&bloom_key) {
                let entry = StringIndexEntry::tag_name(tag_name);
                Self::write_string_index_static(&state, &entry).await?;
                indexes_written += 1;
            }
        }

        // Write tag value indexes (with bloom filter deduplication)
        for tag_value in tag_values {
            let bloom_key = format!("tag_value:{}", tag_value);
            if bloom_manager.should_write_index(&bloom_key) {
                let entry = StringIndexEntry::tag_value(tag_value);
                Self::write_string_index_static(&state, &entry).await?;
                indexes_written += 1;
            }
        }

        trace!("All {} indexes written successfully", indexes_written);
        Ok(indexes_written)
    }
    
    /// Static version of write_string_index for concurrent processing
    async fn write_string_index_static(state: &WriterTaskState, entry: &StringIndexEntry) -> KairosResult<()> {
        let key_bytes = entry.key().to_bytes();
        let column_name = entry.index_column();
        let value_bytes = vec![0u8]; // Empty value for string index

        if let Some(ref prepared) = state.insert_string_index {
            state.session.execute_unpaged(prepared, (key_bytes, column_name, value_bytes))
                .await
                .map_err(|e| {
                    state.stats.failed_queries.fetch_add(1, Ordering::Relaxed);
                    KairosError::cassandra(format!("Failed to write string index: {}", e))
                })?;
        } else {
            return Err(KairosError::cassandra(
                "String index prepared statement not available",
            ));
        }

        state.stats.total_queries.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

impl WriterTaskState {
    /// Clone the state for concurrent processing (shares session and prepared statements)
    fn clone_for_concurrent(&self) -> Self {
        Self {
            session: Arc::clone(&self.session),  // Share the Arc<Session>
            config: Arc::clone(&self.config),
            bloom_manager: BloomManager::new(), // Each concurrent task gets its own bloom filter
            stats: Arc::clone(&self.stats),
            semaphore: Arc::clone(&self.semaphore),
            insert_data_point: self.insert_data_point.clone(),
            insert_row_key_index: self.insert_row_key_index.clone(),
            insert_string_index: self.insert_string_index.clone(),
            insert_row_keys: self.insert_row_keys.clone(),
            insert_row_key_time_index: self.insert_row_key_time_index.clone(),
        }
    }
    /// Prepare frequently used statements for better performance
    async fn prepare_statements(&mut self) -> KairosResult<()> {
        debug!("Preparing frequently used CQL statements");

        // Prepare data point insert
        let data_point_query = format!(
            "INSERT INTO {}.data_points (key, column1, value) VALUES (?, ?, ?)",
            self.config.keyspace
        );
        self.insert_data_point = Some(
            self.session.prepare(data_point_query).await.map_err(|e| {
                KairosError::cassandra(format!("Failed to prepare data point statement: {}", e))
            })?
        );

        // Prepare string index insert
        let string_index_query = format!(
            "INSERT INTO {}.string_index (key, column1, value) VALUES (?, ?, ?)",
            self.config.keyspace
        );
        self.insert_string_index = Some(
            self.session.prepare(string_index_query).await.map_err(|e| {
                KairosError::cassandra(format!("Failed to prepare string index statement: {}", e))
            })?
        );

        // Prepare row key index insert
        let row_key_index_query = format!(
            "INSERT INTO {}.row_key_index (key, column1, value) VALUES (?, ?, ?)",
            self.config.keyspace
        );
        self.insert_row_key_index = Some(
            self.session.prepare(row_key_index_query).await.map_err(|e| {
                KairosError::cassandra(format!("Failed to prepare row key index statement: {}", e))
            })?
        );

        info!("All CQL statements prepared successfully");
        Ok(())
    }

    /// Write a batch of data points (internal implementation)
    async fn write_batch_internal(&mut self, batch: &DataPointBatch) -> KairosResult<()> {
        if batch.points.is_empty() {
            trace!("Empty batch, nothing to write");
            return Ok(());
        }

        let internal_start = std::time::Instant::now();
        trace!("Single writer processing batch of {} data points", batch.points.len());
        self.stats
            .total_datapoints
            .fetch_add(batch.points.len() as u64, Ordering::Relaxed);

        // Prepare all data point writes concurrently
        let prepared_stmt = self.insert_data_point.as_ref()
            .ok_or_else(|| KairosError::cassandra("Data point prepared statement not available"))?;
        
        let write_futures: Vec<_> = batch.points.iter().map(|data_point| {
            let row_key = RowKey::from_data_point(data_point);
            let column_name = ColumnName::from_timestamp(data_point.timestamp);
            let cassandra_value = CassandraValue::from_data_point_value(&data_point.value, None);
            
            let row_key_bytes = row_key.to_bytes();
            let column_key_bytes = column_name.to_bytes();
            let value_bytes = &cassandra_value.bytes;
            
            self.session.execute_unpaged(prepared_stmt, (row_key_bytes, column_key_bytes, value_bytes.clone()))
        }).collect();
        
        // Execute all data point writes concurrently
        let datapoints_start = std::time::Instant::now();
        trace!("Executing {} concurrent data point writes to Cassandra", batch.points.len());
        let results = future::try_join_all(write_futures).await;
        let datapoints_duration = datapoints_start.elapsed();
        
        // Update detailed stats based on results
        let datapoint_count = batch.points.len() as u64;
        match results {
            Ok(_) => {
                trace!("Successfully wrote {} data points to Cassandra in {:?}", batch.points.len(), datapoints_duration);
                self.stats.total_queries.fetch_add(datapoint_count, Ordering::Relaxed);
                self.stats.datapoint_writes.fetch_add(datapoint_count, Ordering::Relaxed);
                self.stats.prepared_statement_cache_hits.fetch_add(datapoint_count, Ordering::Relaxed);
                self.stats.total_datapoint_write_time_ns.fetch_add(datapoints_duration.as_nanos() as u64, Ordering::Relaxed);
            }
            Err(e) => {
                error!("Failed to write data points to Cassandra after {:?}: {}", datapoints_duration, e);
                self.stats.failed_queries.fetch_add(1, Ordering::Relaxed);
                self.stats.datapoint_write_errors.fetch_add(datapoint_count, Ordering::Relaxed);
                self.stats.total_datapoint_write_time_ns.fetch_add(datapoints_duration.as_nanos() as u64, Ordering::Relaxed);
                return Err(KairosError::cassandra(format!("Failed to write data points: {}", e)));
            }
        }

        // Write indexes with bloom filter deduplication (no locking needed!)
        let indexes_start = std::time::Instant::now();
        trace!("Writing indexes for batch");
        match self.write_indexes(batch).await {
            Ok(index_count) => {
                let indexes_duration = indexes_start.elapsed();
                trace!("Completed writing {} indexes in {:?}", index_count, indexes_duration);
                self.stats.index_writes.fetch_add(index_count, Ordering::Relaxed);
                self.stats.total_index_write_time_ns.fetch_add(indexes_duration.as_nanos() as u64, Ordering::Relaxed);
            }
            Err(e) => {
                let indexes_duration = indexes_start.elapsed();
                error!("Failed to write indexes after {:?}: {}", indexes_duration, e);
                self.stats.index_write_errors.fetch_add(1, Ordering::Relaxed);
                self.stats.total_index_write_time_ns.fetch_add(indexes_duration.as_nanos() as u64, Ordering::Relaxed);
                return Err(e);
            }
        }

        let total_duration = internal_start.elapsed();
        self.stats.total_batch_write_time_ns.fetch_add(total_duration.as_nanos() as u64, Ordering::Relaxed);
        self.stats.batches_processed.fetch_add(1, Ordering::Relaxed);
        trace!("Single writer batch completed successfully in {:?} total (datapoints: {:?}, indexes: {:?})", 
              total_duration, datapoints_duration, indexes_start.elapsed());
        Ok(())
    }

    /// Write a single data point to Cassandra
    async fn write_data_point(
        &mut self,
        row_key: &RowKey,
        column_name: &ColumnName,
        value: &CassandraValue,
    ) -> KairosResult<()> {
        let row_key_bytes = row_key.to_bytes();
        let column_key_bytes = column_name.to_bytes();
        let value_bytes = &value.bytes;

        if let Some(ref prepared) = self.insert_data_point {
            self.session.execute_unpaged(prepared, (row_key_bytes, column_key_bytes, value_bytes))
                .await
                .map_err(|e| {
                    self.stats.failed_queries.fetch_add(1, Ordering::Relaxed);
                    KairosError::cassandra(format!("Failed to write data point: {}", e))
                })?;
        } else {
            return Err(KairosError::cassandra(
                "Data point prepared statement not available",
            ));
        }

        self.stats.total_queries.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Write all indexing data for a batch of data points (no locking needed!)
    /// Returns the number of index entries written
    async fn write_indexes(&mut self, batch: &DataPointBatch) -> KairosResult<u64> {
        let mut metric_names = HashSet::new();
        let mut tag_names = HashSet::new();
        let mut tag_values = HashSet::new();

        // Collect all unique metric names and tags
        for data_point in &batch.points {
            metric_names.insert(data_point.metric.as_str());

            for (tag_key, tag_value) in data_point.tags.iter() {
                tag_names.insert(tag_key.as_str());
                tag_values.insert(tag_value.as_str());
            }
        }

        trace!(
            "Writing indexes for {} metrics, {} tag keys, {} tag values",
            metric_names.len(),
            tag_names.len(),
            tag_values.len()
        );

        let mut indexes_written = 0u64;

        // Write metric name indexes (with bloom filter deduplication - no locking!)
        for metric_name in metric_names {
            let bloom_key = format!("metric_name:{}", metric_name);
            if self.bloom_manager.should_write_index(&bloom_key) {
                let entry = StringIndexEntry::metric_name(metric_name);
                self.write_string_index(&entry).await?;
                indexes_written += 1;
            }
        }

        // Write tag name indexes (with bloom filter deduplication - no locking!)
        for tag_name in tag_names {
            let bloom_key = format!("tag_name:{}", tag_name);
            if self.bloom_manager.should_write_index(&bloom_key) {
                let entry = StringIndexEntry::tag_name(tag_name);
                self.write_string_index(&entry).await?;
                indexes_written += 1;
            }
        }

        // Write tag value indexes (with bloom filter deduplication - no locking!)
        for tag_value in tag_values {
            let bloom_key = format!("tag_value:{}", tag_value);
            if self.bloom_manager.should_write_index(&bloom_key) {
                let entry = StringIndexEntry::tag_value(tag_value);
                self.write_string_index(&entry).await?;
                indexes_written += 1;
            }
        }

        trace!("All {} indexes written successfully", indexes_written);
        Ok(indexes_written)
    }

    /// Write a string index entry
    async fn write_string_index(&mut self, entry: &StringIndexEntry) -> KairosResult<()> {
        let key_bytes = entry.key().to_bytes();
        let column_name = entry.index_column();
        let value_bytes = vec![0u8]; // Empty value for string index

        if let Some(ref prepared) = self.insert_string_index {
            self.session.execute_unpaged(prepared, (key_bytes, column_name, value_bytes))
                .await
                .map_err(|e| {
                    self.stats.failed_queries.fetch_add(1, Ordering::Relaxed);
                    KairosError::cassandra(format!("Failed to write string index: {}", e))
                })?;
        } else {
            return Err(KairosError::cassandra(
                "String index prepared statement not available",
            ));
        }

        self.stats.total_queries.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[async_trait]
impl CassandraClient for SingleWriterCassandraClient {
    async fn write_batch(&self, batch: &DataPointBatch) -> KairosResult<()> {
        let start = std::time::Instant::now();
        
        // Create oneshot channel for response
        let (response_tx, response_rx) = oneshot::channel();

        // Send write command to single writer task
        let command = WriteCommand::Batch {
            batch: batch.clone(),
            response_tx,
        };

        trace!("SingleWriter: Sending batch of {} points to writer task", batch.points.len());
        self.write_tx.send(command).map_err(|_| {
            KairosError::cassandra("Writer task unavailable")
        })?;

        // Wait for response from writer task
        let result = response_rx.await.map_err(|_| {
            KairosError::cassandra("Writer task response lost")
        })?;
        
        let duration = start.elapsed();
        trace!("SingleWriter: Batch write completed in {:?}", duration);
        result
    }

    async fn health_check(&self) -> KairosResult<bool> {
        // For simplicity, always return healthy if writer task is running
        // (Could implement a ping command to writer task if needed)
        Ok(!self.write_tx.is_closed())
    }

    fn get_stats(&self) -> CassandraStats {
        let bloom_stats = BloomManager::new().get_stats(); // Placeholder - would need writer task stats
        
        // Calculate average timing metrics
        let datapoint_writes = self.stats.datapoint_writes.load(Ordering::Relaxed);
        let index_writes = self.stats.index_writes.load(Ordering::Relaxed);
        let total_batches = self.stats.batches_processed.load(Ordering::Relaxed).max(1); // Avoid division by zero
        
        let avg_datapoint_write_time_ms = if datapoint_writes > 0 {
            (self.stats.total_datapoint_write_time_ns.load(Ordering::Relaxed) as f64) / (datapoint_writes as f64) / 1_000_000.0
        } else {
            0.0
        };
        
        let avg_index_write_time_ms = if index_writes > 0 {
            (self.stats.total_index_write_time_ns.load(Ordering::Relaxed) as f64) / (index_writes as f64) / 1_000_000.0
        } else {
            0.0
        };
        
        let avg_batch_write_time_ms = if total_batches > 0 {
            (self.stats.total_batch_write_time_ns.load(Ordering::Relaxed) as f64) / (total_batches as f64) / 1_000_000.0
        } else {
            0.0
        };
        
        // Calculate semaphore wait time average
        let total_batches_completed = self.stats.batches_processed.load(Ordering::Relaxed);
        let avg_semaphore_wait_time_ms = if total_batches_completed > 0 {
            (self.stats.semaphore_wait_time_ns.load(Ordering::Relaxed) as f64) / (total_batches_completed as f64) / 1_000_000.0
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
            
            // Detailed Cassandra operation metrics
            datapoint_writes: self.stats.datapoint_writes.load(Ordering::Relaxed),
            datapoint_write_errors: self.stats.datapoint_write_errors.load(Ordering::Relaxed),
            index_writes: self.stats.index_writes.load(Ordering::Relaxed),
            index_write_errors: self.stats.index_write_errors.load(Ordering::Relaxed),
            prepared_statement_cache_hits: self.stats.prepared_statement_cache_hits.load(Ordering::Relaxed),
            prepared_statement_cache_misses: self.stats.prepared_statement_cache_misses.load(Ordering::Relaxed),
            
            // Concurrency metrics
            current_concurrent_requests: self.stats.concurrent_requests.load(Ordering::Relaxed),
            max_concurrent_requests_reached: self.stats.max_concurrent_requests.load(Ordering::Relaxed),
            avg_semaphore_wait_time_ms,
            
            // Timing metrics (averaged per operation in milliseconds)
            avg_datapoint_write_time_ms,
            avg_index_write_time_ms,
            avg_batch_write_time_ms,
        }
    }

    async fn ensure_schema(&self) -> KairosResult<()> {
        // Schema operations could be sent to writer task or handled separately
        // For now, assume schema is already created
        info!("Schema assumed to be present");
        Ok(())
    }
}