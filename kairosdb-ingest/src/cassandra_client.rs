//! Production-grade Cassandra client for KairosDB using ScyllaDB Rust driver
//!
//! This client provides robust, high-performance data persistence using the ScyllaDB Rust driver
//! with proper error handling, connection management, and KairosDB schema compatibility.

use async_trait::async_trait;
use futures::future::join_all;
use kairosdb_core::{
    cassandra::{CassandraValue, ColumnName, RowKey},
    datapoint::DataPointBatch,
    error::{KairosError, KairosResult},
    schema::{RowKeyIndexEntry, StringIndexEntry},
};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tracing::{debug, error, info, trace, warn};

// ScyllaDB Rust driver imports
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::client::PoolSize;
use scylla::response::query_result::QueryResult;
use scylla::serialize::row::SerializeRow;
// Removed batch imports - using async concurrency instead
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlTimestamp;

use crate::bloom_manager::BloomManager;
use crate::cassandra::{CassandraClient, CassandraStats};
use crate::config::CassandraConfig;

/// Production Cassandra client implementation using ScyllaDB Rust driver
#[derive(Clone)]
pub struct CassandraClientImpl {
    session: Arc<Session>,
    config: Arc<CassandraConfig>,
    stats: Arc<CassandraClientStats>,
    bloom_manager: Arc<BloomManager>,
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
}

impl Default for CassandraClientStats {
    fn default() -> Self {
        Self {
            total_queries: AtomicU64::new(0),
            failed_queries: AtomicU64::new(0),
            total_datapoints: AtomicU64::new(0),
            connection_errors: AtomicU64::new(0),
        }
    }
}

impl CassandraClientImpl {
    /// Create a new Cassandra client instance
    pub async fn new(config: CassandraConfig) -> KairosResult<Self> {
        let config = Arc::new(config);

        info!(
            "Initializing ScyllaDB client with contact points: {:?}",
            config.contact_points
        );

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
                    .unwrap_or_else(|| std::num::NonZero::new(1).unwrap())
            ));

        // Don't set default keyspace here - we'll create it in ensure_schema()

        let session = session_builder.build().await.map_err(|e| {
            KairosError::cassandra(format!("Failed to create ScyllaDB session: {}", e))
        })?;

        info!("ScyllaDB session established successfully");

        Ok(Self {
            session: Arc::new(session),
            config: config.clone(),
            stats: Arc::new(CassandraClientStats::default()),
            bloom_manager: Arc::new(BloomManager::new()),
            insert_data_point: None,
            insert_row_key_index: None,
            insert_string_index: None,
            insert_row_keys: None,
            insert_row_key_time_index: None,
        })
    }

    /// Prepare frequently used statements for better performance
    pub async fn prepare_statements(&mut self) -> KairosResult<()> {
        debug!("Preparing frequently used CQL statements");

        // Prepare data point insertion statement
        self.insert_data_point = Some(
            self.session
                .prepare("INSERT INTO data_points (key, column1, value) VALUES (?, ?, ?)")
                .await
                .map_err(|e| {
                    KairosError::cassandra(format!(
                        "Failed to prepare data_points statement: {}",
                        e
                    ))
                })?,
        );

        // Prepare row key index insertion statement
        self.insert_row_key_index = Some(
            self.session
                .prepare("INSERT INTO row_key_index (key, column1, value) VALUES (?, ?, ?)")
                .await
                .map_err(|e| {
                    KairosError::cassandra(format!(
                        "Failed to prepare row_key_index statement: {}",
                        e
                    ))
                })?,
        );

        // Prepare string index insertion statement
        self.insert_string_index = Some(
            self.session
                .prepare("INSERT INTO string_index (key, column1, value) VALUES (?, ?, ?)")
                .await
                .map_err(|e| {
                    KairosError::cassandra(format!(
                        "Failed to prepare string_index statement: {}",
                        e
                    ))
                })?,
        );

        // Prepare row_keys insertion statement (new format)
        self.insert_row_keys = Some(
            self.session
                .prepare("INSERT INTO row_keys (metric, table_name, row_time, data_type, tags, mtime) VALUES (?, 'data_points', ?, ?, ?, now()) USING TTL ?")
                .await
                .map_err(|e| {
                    KairosError::cassandra(format!(
                        "Failed to prepare row_keys statement: {}",
                        e
                    ))
                })?,
        );

        // Prepare row_key_time_index insertion statement (new format)
        self.insert_row_key_time_index = Some(
            self.session
                .prepare("INSERT INTO row_key_time_index (metric, table_name, row_time) VALUES (?, 'data_points', ?) USING TTL ?")
                .await
                .map_err(|e| {
                    KairosError::cassandra(format!(
                        "Failed to prepare row_key_time_index statement: {}",
                        e
                    ))
                })?,
        );

        debug!("All CQL statements prepared successfully");
        Ok(())
    }

    /// Execute a simple query without parameters
    async fn execute_query(&self, query: &str) -> KairosResult<QueryResult> {
        self.stats.total_queries.fetch_add(1, Ordering::Relaxed);

        trace!("Executing query: {}", query);

        match self.session.query_unpaged(query, &[]).await {
            Ok(result) => {
                trace!("Query executed successfully");
                Ok(result)
            }
            Err(e) => {
                self.stats.failed_queries.fetch_add(1, Ordering::Relaxed);
                error!("Query execution failed: {}", e);
                Err(KairosError::cassandra(format!("Query failed: {}", e)))
            }
        }
    }

    /// Execute a prepared statement with values
    async fn execute_prepared<T>(
        &self,
        prepared: &PreparedStatement,
        values: T,
    ) -> KairosResult<QueryResult>
    where
        T: SerializeRow,
    {
        self.stats.total_queries.fetch_add(1, Ordering::Relaxed);

        match self.session.execute_unpaged(prepared, values).await {
            Ok(result) => {
                trace!("Prepared statement executed successfully");
                Ok(result)
            }
            Err(e) => {
                self.stats.failed_queries.fetch_add(1, Ordering::Relaxed);
                error!("Prepared statement execution failed: {}", e);
                Err(KairosError::cassandra(format!(
                    "Prepared statement failed: {}",
                    e
                )))
            }
        }
    }

    /// Write a single data point to Cassandra using prepared statement
    async fn write_data_point(
        &self,
        row_key: &RowKey,
        column_name: &ColumnName,
        value: &CassandraValue,
    ) -> KairosResult<()> {
        let row_key_bytes = row_key.to_bytes();
        let column_key_bytes = column_name.to_bytes();
        let value_bytes = &value.bytes;

        trace!(
            "Writing data point: row_key_len={}, column_key_len={}, value_len={}, row_key_hex={}",
            row_key_bytes.len(),
            column_key_bytes.len(),
            value_bytes.len(),
            hex::encode(&row_key_bytes)
        );

        if let Some(ref prepared) = self.insert_data_point {
            self.execute_prepared(prepared, (row_key_bytes, column_key_bytes, value_bytes))
                .await?;
        } else {
            return Err(KairosError::cassandra(
                "Data point prepared statement not available",
            ));
        }

        trace!("Data point written successfully");
        Ok(())
    }

    /// Write an entry to the row_key_index table using prepared statement
    async fn write_row_key_index(&self, entry: &RowKeyIndexEntry) -> KairosResult<()> {
        let key_bytes = entry.key().to_bytes();
        let column_bytes = entry.column().to_bytes();
        let value_bytes = entry.value().to_bytes();

        trace!("Writing row key index entry");

        if let Some(ref prepared) = self.insert_row_key_index {
            self.execute_prepared(prepared, (key_bytes, column_bytes, value_bytes))
                .await?;
        } else {
            return Err(KairosError::cassandra(
                "Row key index prepared statement not available",
            ));
        }

        trace!("Row key index entry written successfully");
        Ok(())
    }

    /// Write an entry to the row_keys table (new format)
    async fn write_row_keys(&self, row_key: &RowKey, ttl: Option<u32>) -> KairosResult<()> {
        trace!("Writing row_keys entry");

        if let Some(ref prepared) = self.insert_row_keys {
            // Convert tags to a map format expected by Cassandra
            let tags_map: std::collections::HashMap<String, String> = row_key
                .tags
                .split(':')
                .filter_map(|pair| {
                    if pair.is_empty() {
                        return None;
                    }
                    let mut parts = pair.split('=');
                    let key = parts.next()?.to_string();
                    let value = parts.next()?.to_string();
                    Some((key, value))
                })
                .collect();

            // Convert to Cassandra timestamp type
            let row_time_timestamp = CqlTimestamp(row_key.row_time.timestamp_millis());
            let ttl_seconds = ttl.unwrap_or(0);

            self.execute_prepared(
                prepared,
                (
                    &row_key.metric.as_str(), // metric
                    row_time_timestamp,       // row_time as timestamp
                    &row_key.data_type,       // data_type
                    tags_map,                 // tags as map
                    ttl_seconds as i32,       // TTL
                ),
            )
            .await?;
        } else {
            return Err(KairosError::cassandra(
                "Row keys prepared statement not available",
            ));
        }

        trace!("Row keys entry written successfully");
        Ok(())
    }

    /// Write an entry to the row_key_time_index table (new format)
    async fn write_row_key_time_index(
        &self,
        row_key: &RowKey,
        ttl: Option<u32>,
    ) -> KairosResult<()> {
        trace!("Writing row_key_time_index entry");

        if let Some(ref prepared) = self.insert_row_key_time_index {
            // Convert to Cassandra timestamp type
            let row_time_timestamp = CqlTimestamp(row_key.row_time.timestamp_millis());
            let ttl_seconds = ttl.unwrap_or(0);

            self.execute_prepared(
                prepared,
                (
                    &row_key.metric.as_str(), // metric
                    row_time_timestamp,       // row_time as timestamp
                    ttl_seconds as i32,       // TTL
                ),
            )
            .await?;
        } else {
            return Err(KairosError::cassandra(
                "Row key time index prepared statement not available",
            ));
        }

        trace!("Row key time index entry written successfully");
        Ok(())
    }

    /// Write an entry to the string_index table using prepared statement
    async fn write_string_index(&self, entry: &StringIndexEntry) -> KairosResult<()> {
        let key_bytes = entry.key().to_bytes();
        let column_name = entry.index_column();
        let value_bytes = vec![0u8]; // Empty value for string index

        trace!("Writing string index entry: {}", column_name);

        if let Some(ref prepared) = self.insert_string_index {
            self.execute_prepared(prepared, (key_bytes, column_name, value_bytes))
                .await?;
        } else {
            return Err(KairosError::cassandra(
                "String index prepared statement not available",
            ));
        }

        trace!("String index entry written successfully");
        Ok(())
    }

    /// Write all indexing data for a batch of data points
    async fn write_indexes(&self, batch: &DataPointBatch) -> KairosResult<()> {
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

        // Write metric name indexes (with bloom filter deduplication)
        for metric_name in metric_names {
            let bloom_key = format!("metric_name:{}", metric_name);
            if self.bloom_manager.should_write_index(&bloom_key) {
                let entry = StringIndexEntry::metric_name(metric_name);
                self.write_string_index(&entry).await?;
            }
        }

        // Write tag name indexes (with bloom filter deduplication)
        for tag_name in tag_names {
            let bloom_key = format!("tag_name:{}", tag_name);
            if self.bloom_manager.should_write_index(&bloom_key) {
                let entry = StringIndexEntry::tag_name(tag_name);
                self.write_string_index(&entry).await?;
            }
        }

        // Write tag value indexes (with bloom filter deduplication)
        for tag_value in tag_values {
            let bloom_key = format!("tag_value:{}", tag_value);
            if self.bloom_manager.should_write_index(&bloom_key) {
                let entry = StringIndexEntry::tag_value(tag_value);
                self.write_string_index(&entry).await?;
            }
        }

        // Write row key indexes (legacy and new format) with bloom filter deduplication
        for data_point in &batch.points {
            let row_key = RowKey::from_data_point(data_point);
            
            // Use a composite key that includes metric name and time bucket for row keys
            // This ensures we don't skip legitimate row key updates for different time periods  
            let time_bucket = data_point.timestamp.timestamp_millis() / (3600 * 1000); // Hour bucket
            let row_key_bloom_key = format!("row_key:{}:{}", data_point.metric, time_bucket);
            
            if self.bloom_manager.should_write_index(&row_key_bloom_key) {
                // Write to legacy row_key_index for backward compatibility
                let entry = RowKeyIndexEntry::from_row_key(&row_key);
                self.write_row_key_index(&entry).await?;

                // Write to new row_keys and row_key_time_index tables used by Java KairosDB
                let ttl = if data_point.ttl == 0 {
                    None
                } else {
                    Some(data_point.ttl)
                };
                self.write_row_keys(&row_key, ttl).await?;
                self.write_row_key_time_index(&row_key, ttl).await?;
            }
        }

        trace!("All indexes written successfully");
        Ok(())
    }
}

#[async_trait]
impl CassandraClient for CassandraClientImpl {
    async fn write_batch(&self, batch: &DataPointBatch) -> KairosResult<()> {
        if batch.points.is_empty() {
            trace!("Empty batch, nothing to write");
            return Ok(());
        }

        trace!("Writing batch of {} data points with async concurrency", batch.points.len());
        self.stats
            .total_datapoints
            .fetch_add(batch.points.len() as u64, Ordering::Relaxed);

        // Write all data points concurrently using async writes
        let data_point_futures: Vec<_> = batch.points.iter().map(|data_point| {
            let row_key = RowKey::from_data_point(data_point);
            let column_name = ColumnName::from_timestamp(data_point.timestamp);
            let cassandra_value = CassandraValue::from_data_point_value(&data_point.value, None);
            
            // Clone self and move values into the async block
            let client = self.clone();
            async move {
                client.write_data_point(&row_key, &column_name, &cassandra_value).await
            }
        }).collect();

        // Execute all data point writes concurrently
        let results = join_all(data_point_futures).await;
        
        // Check for any errors
        for result in results {
            result?;
        }

        // Write indexes with bloom filter deduplication
        self.write_indexes(batch).await?;

        trace!("Async batch written successfully");
        Ok(())
    }

    async fn health_check(&self) -> KairosResult<bool> {
        debug!("Performing health check");

        match self.execute_query("SELECT now() FROM system.local").await {
            Ok(_) => {
                debug!("Health check passed");
                Ok(true)
            }
            Err(e) => {
                warn!("Health check failed: {}", e);
                self.stats.connection_errors.fetch_add(1, Ordering::Relaxed);
                Ok(false)
            }
        }
    }

    fn get_stats(&self) -> CassandraStats {
        let bloom_stats = self.bloom_manager.get_stats();
        
        CassandraStats {
            total_queries: self.stats.total_queries.load(Ordering::Relaxed),
            failed_queries: self.stats.failed_queries.load(Ordering::Relaxed),
            total_datapoints_written: self.stats.total_datapoints.load(Ordering::Relaxed),
            avg_batch_size: 0.0, // TODO: Calculate actual average batch size
            connection_errors: self.stats.connection_errors.load(Ordering::Relaxed),
            bloom_filter_in_overlap_period: bloom_stats.in_overlap_period,
            bloom_filter_primary_age_seconds: bloom_stats.primary_age_seconds,
            bloom_filter_expected_items: bloom_stats.expected_items,
            bloom_filter_false_positive_rate: bloom_stats.false_positive_rate,
        }
    }

    async fn ensure_schema(&self) -> KairosResult<()> {
        info!("Initializing KairosDB schema");

        let keyspace = &self.config.keyspace;

        // Create keyspace
        let create_keyspace = format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}",
            keyspace
        );
        self.execute_query(&create_keyspace).await?;

        // Use keyspace
        let use_keyspace = format!("USE {}", keyspace);
        self.execute_query(&use_keyspace).await?;

        // Create data_points table
        let create_data_points = "
            CREATE TABLE IF NOT EXISTS data_points (
                key blob,
                column1 blob,
                value blob,
                PRIMARY KEY (key, column1)
            ) WITH COMPACT STORAGE AND CLUSTERING ORDER BY (column1 ASC)";
        self.execute_query(create_data_points).await?;

        // Create row_keys table (Java KairosDB new format)
        let create_row_keys = "
            CREATE TABLE IF NOT EXISTS row_keys (
                metric text,
                table_name text,
                row_time timestamp,
                data_type text,
                tags frozen<map<text, text>>,
                mtime timeuuid static,
                value text,
                PRIMARY KEY ((metric, table_name, row_time), data_type, tags)
            ) WITH CLUSTERING ORDER BY (data_type ASC, tags ASC)";
        self.execute_query(create_row_keys).await?;

        // Create row_key_time_index table (Java KairosDB new format)
        let create_row_key_time_index = "
            CREATE TABLE IF NOT EXISTS row_key_time_index (
                metric text,
                table_name text,
                row_time timestamp,
                value text,
                PRIMARY KEY (metric, table_name, row_time)
            ) WITH CLUSTERING ORDER BY (table_name ASC, row_time ASC)";
        self.execute_query(create_row_key_time_index).await?;

        // Create row_key_index table
        let create_row_key_index = "
            CREATE TABLE IF NOT EXISTS row_key_index (
                key blob,
                column1 blob,
                value blob,
                PRIMARY KEY (key, column1)
            ) WITH COMPACT STORAGE AND CLUSTERING ORDER BY (column1 ASC)";
        self.execute_query(create_row_key_index).await?;

        // Create string_index table
        let create_string_index = "
            CREATE TABLE IF NOT EXISTS string_index (
                key blob,
                column1 text,
                value blob,
                PRIMARY KEY (key, column1)
            ) WITH COMPACT STORAGE";
        self.execute_query(create_string_index).await?;

        info!("KairosDB schema initialized successfully");
        Ok(())
    }
}
