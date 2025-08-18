//! Production-grade Cassandra client for KairosDB using ScyllaDB Rust driver
//!
//! This client provides robust, high-performance data persistence using the ScyllaDB Rust driver
//! with proper error handling, connection management, and KairosDB schema compatibility.

use async_trait::async_trait;
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
use tracing::{debug, error, info, warn};

// ScyllaDB Rust driver imports
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::response::query_result::QueryResult;
use scylla::serialize::row::SerializeRow;
use scylla::statement::prepared::PreparedStatement;

use crate::cassandra::{CassandraClient, CassandraStats};
use crate::config::CassandraConfig;

/// Production Cassandra client implementation using ScyllaDB Rust driver
pub struct CassandraClientImpl {
    session: Session,
    config: Arc<CassandraConfig>,
    stats: CassandraClientStats,
    // Prepared statements for performance
    insert_data_point: Option<PreparedStatement>,
    insert_row_key_index: Option<PreparedStatement>,
    insert_string_index: Option<PreparedStatement>,
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

        // Don't set default keyspace here - we'll create it in ensure_schema()

        let session = session_builder.build().await.map_err(|e| {
            KairosError::cassandra(format!("Failed to create ScyllaDB session: {}", e))
        })?;

        info!("ScyllaDB session established successfully");

        Ok(Self {
            session,
            config: config.clone(),
            stats: CassandraClientStats::default(),
            insert_data_point: None,
            insert_row_key_index: None,
            insert_string_index: None,
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

        debug!("All CQL statements prepared successfully");
        Ok(())
    }

    /// Execute a simple query without parameters
    async fn execute_query(&self, query: &str) -> KairosResult<QueryResult> {
        self.stats.total_queries.fetch_add(1, Ordering::Relaxed);

        debug!("Executing query: {}", query);

        match self.session.query_unpaged(query, &[]).await {
            Ok(result) => {
                debug!("Query executed successfully");
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
                debug!("Prepared statement executed successfully");
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

        debug!(
            "Writing data point: row_key_len={}, column_key_len={}, value_len={}",
            row_key_bytes.len(),
            column_key_bytes.len(),
            value_bytes.len()
        );

        if let Some(ref prepared) = self.insert_data_point {
            self.execute_prepared(prepared, (row_key_bytes, column_key_bytes, value_bytes))
                .await?;
        } else {
            return Err(KairosError::cassandra(
                "Data point prepared statement not available",
            ));
        }

        debug!("Data point written successfully");
        Ok(())
    }

    /// Write an entry to the row_key_index table using prepared statement
    async fn write_row_key_index(&self, entry: &RowKeyIndexEntry) -> KairosResult<()> {
        let key_bytes = entry.key().to_bytes();
        let column_bytes = entry.column().to_bytes();
        let value_bytes = entry.value().to_bytes();

        debug!("Writing row key index entry");

        if let Some(ref prepared) = self.insert_row_key_index {
            self.execute_prepared(prepared, (key_bytes, column_bytes, value_bytes))
                .await?;
        } else {
            return Err(KairosError::cassandra(
                "Row key index prepared statement not available",
            ));
        }

        debug!("Row key index entry written successfully");
        Ok(())
    }

    /// Write an entry to the string_index table using prepared statement
    async fn write_string_index(&self, entry: &StringIndexEntry) -> KairosResult<()> {
        let key_bytes = entry.key().to_bytes();
        let column_name = entry.index_column();
        let value_bytes = vec![0u8]; // Empty value for string index

        debug!("Writing string index entry: {}", column_name);

        if let Some(ref prepared) = self.insert_string_index {
            self.execute_prepared(prepared, (key_bytes, column_name, value_bytes))
                .await?;
        } else {
            return Err(KairosError::cassandra(
                "String index prepared statement not available",
            ));
        }

        debug!("String index entry written successfully");
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

        debug!(
            "Writing indexes for {} metrics, {} tag keys, {} tag values",
            metric_names.len(),
            tag_names.len(),
            tag_values.len()
        );

        // Write metric name indexes
        for metric_name in metric_names {
            let entry = StringIndexEntry::metric_name(metric_name);
            self.write_string_index(&entry).await?;
        }

        // Write tag name indexes
        for tag_name in tag_names {
            let entry = StringIndexEntry::tag_name(tag_name);
            self.write_string_index(&entry).await?;
        }

        // Write tag value indexes
        for tag_value in tag_values {
            let entry = StringIndexEntry::tag_value(tag_value);
            self.write_string_index(&entry).await?;
        }

        // Write row key indexes
        for data_point in &batch.points {
            let row_key = RowKey::from_data_point(data_point);
            let entry = RowKeyIndexEntry::from_row_key(&row_key);
            self.write_row_key_index(&entry).await?;
        }

        debug!("All indexes written successfully");
        Ok(())
    }
}

#[async_trait]
impl CassandraClient for CassandraClientImpl {
    async fn write_batch(&self, batch: &DataPointBatch) -> KairosResult<()> {
        if batch.points.is_empty() {
            debug!("Empty batch, nothing to write");
            return Ok(());
        }

        info!("Writing batch of {} data points", batch.points.len());
        self.stats
            .total_datapoints
            .fetch_add(batch.points.len() as u64, Ordering::Relaxed);

        // Write data points
        for data_point in &batch.points {
            let row_key = RowKey::from_data_point(data_point);
            let column_name = ColumnName::from_timestamp(data_point.timestamp);
            let cassandra_value = CassandraValue::from_data_point_value(&data_point.value, None);

            self.write_data_point(&row_key, &column_name, &cassandra_value)
                .await?;
        }

        // Write indexes
        self.write_indexes(batch).await?;

        info!("Batch written successfully");
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
        CassandraStats {
            total_queries: self.stats.total_queries.load(Ordering::Relaxed),
            failed_queries: self.stats.failed_queries.load(Ordering::Relaxed),
            total_datapoints_written: self.stats.total_datapoints.load(Ordering::Relaxed),
            avg_batch_size: 0.0, // TODO: Calculate actual average batch size
            connection_errors: self.stats.connection_errors.load(Ordering::Relaxed),
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

        // Create row_keys table
        let create_row_keys = "
            CREATE TABLE IF NOT EXISTS row_keys (
                key blob,
                column1 blob,
                value blob,
                PRIMARY KEY (key, column1)
            ) WITH COMPACT STORAGE AND CLUSTERING ORDER BY (column1 ASC)";
        self.execute_query(create_row_keys).await?;

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
