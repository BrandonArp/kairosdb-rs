//! Cassandra implementation using the legacy KairosDB schema
//!
//! This implementation works with the existing KairosDB tables:
//! - data_points (COMPACT STORAGE with blob keys)
//! - row_key_index (legacy blob-based index)
//! - string_index (metric/tag discovery)
//!
//! This allows us to validate the datastore abstraction with production data
//! before migrating to a new schema.

use crate::datapoint::{DataPoint, DataPointValue};
use crate::datastore::{
    TagFilter, TagSet as DataStoreTagSet, TagValue as DataStoreTagValue, TimeSeriesStore,
    WriteResult,
};
use crate::error::{KairosError, KairosResult};
use crate::metrics::MetricName;
use crate::tags::TagSet;
use crate::time::{TimeRange, Timestamp};
use async_trait::async_trait;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::value::CqlTimestamp;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// Simple data structure to represent a data points row key
/// This matches the legacy KairosDB row key format
#[derive(Debug, Clone, PartialEq)]
pub struct DataPointsRowKey {
    pub metric_name: String,
    pub cluster_name: String,
    pub timestamp: i64, // Row time in milliseconds
    pub data_type: String,
    pub tags: BTreeMap<String, String>,
}

impl DataPointsRowKey {
    /// Convert to bytes using the legacy format
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Add metric name
        bytes.extend_from_slice(self.metric_name.as_bytes());
        bytes.push(0x00); // null separator

        // Add timestamp (8 bytes, big endian)
        bytes.extend_from_slice(&self.timestamp.to_be_bytes());

        // Add data type
        bytes.push(0x00); // separator
        bytes.push(self.data_type.len() as u8);
        bytes.extend_from_slice(self.data_type.as_bytes());

        // Add tags (simplified format for now)
        let tags_str = format_tags(&self.tags);
        bytes.extend_from_slice(tags_str.as_bytes());

        bytes
    }

    /// Parse from bytes (simplified version)
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        // This is a simplified parser - in reality this would need to match
        // the exact Java KairosDB format
        if bytes.len() < 16 {
            return None;
        }

        // For now, return a dummy parsed result
        // In a real implementation, this would parse the exact format
        Some(Self {
            metric_name: "parsed_metric".to_string(),
            cluster_name: "default".to_string(),
            timestamp: 0,
            data_type: "kairos_long".to_string(),
            tags: BTreeMap::new(),
        })
    }
}

/// Convert TagSet to BTreeMap for legacy compatibility
fn tagset_to_btreemap(_tags: &TagSet) -> BTreeMap<String, String> {
    // This is a simplified conversion - in reality we'd need proper TagSet methods
    // For now, return empty map
    BTreeMap::new()
}

/// Convert BTreeMap to TagSet
fn btreemap_to_tagset(_tags: &BTreeMap<String, String>) -> TagSet {
    // This is a simplified conversion - in reality we'd need proper TagSet construction
    // For now, return empty TagSet
    TagSet::new()
}

/// Format tags as a string (simplified version of KairosDB format)
pub fn format_tags(tags: &BTreeMap<String, String>) -> String {
    // Use Java KairosDB format: key1=value1:key2=value2: (with trailing colon)
    let mut parts: Vec<String> = Vec::new();
    for (key, value) in tags {
        parts.push(format!("{}={}", key, value));
    }
    if parts.is_empty() {
        String::new()
    } else {
        format!("{}:", parts.join(":"))
    }
}

/// Format TagSet as a string
pub fn format_tagset(tags: &TagSet) -> String {
    // Convert to BTreeMap first, then format
    let btree = tagset_to_btreemap(tags);
    format_tags(&btree)
}

/// Parse tags from string format (Java KairosDB format with colons)
pub fn parse_tags(tags_str: &str) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();
    if tags_str.is_empty() {
        return tags;
    }

    // Split on colons and filter out empty parts (handles trailing colon)
    for part in tags_str.split(':').filter(|p| !p.is_empty()) {
        if let Some((key, value)) = part.split_once('=') {
            tags.insert(key.to_string(), value.to_string());
        }
    }
    tags
}

/// Cassandra implementation using legacy KairosDB schema
#[allow(dead_code)]
pub struct CassandraLegacyStore {
    session: Arc<Session>,
    keyspace: String,
}

impl CassandraLegacyStore {
    /// Create a new legacy store instance  
    pub async fn new(keyspace: String) -> KairosResult<Self> {
        // Get Cassandra contact points from environment
        let contact_points = std::env::var("KAIROSDB_CASSANDRA_CONTACT_POINTS")
            .unwrap_or_else(|_| "cassandra:9042".to_string());

        // Build the session
        let mut builder = SessionBuilder::new();
        for contact_point in contact_points.split(',') {
            builder = builder.known_node(contact_point);
        }
        builder = builder.use_keyspace(&keyspace, false);

        // Add authentication if provided
        if let (Ok(username), Ok(password)) = (
            std::env::var("KAIROSDB_CASSANDRA_USERNAME"),
            std::env::var("KAIROSDB_CASSANDRA_PASSWORD"),
        ) {
            builder = builder.user(&username, &password);
        }

        let session = builder.build().await.map_err(|e| {
            KairosError::Connection(format!("Failed to connect to Cassandra: {}", e))
        })?;
        let session = Arc::new(session);

        Ok(Self { session, keyspace })
    }

    /// Calculate the row time for a timestamp (3-week boundaries)
    fn calculate_row_time(timestamp: Timestamp) -> Timestamp {
        // KairosDB uses 3-week row boundaries
        const THREE_WEEKS_MS: i64 = 3 * 7 * 24 * 60 * 60 * 1000;
        let millis = timestamp.timestamp_millis();
        let row_time_millis = (millis / THREE_WEEKS_MS) * THREE_WEEKS_MS;
        Timestamp::from_millis(row_time_millis).unwrap_or(timestamp)
    }

    /// Get data type string for a value
    fn get_data_type(value: &DataPointValue) -> &'static str {
        match value {
            DataPointValue::Long(_) => "kairos_long",
            DataPointValue::Double(_) => "kairos_double",
            DataPointValue::Text(_) => "kairos_string",
            DataPointValue::Histogram(_) => "kairos_histogram_v2",
            DataPointValue::Binary(_) => "kairos_bytes",
            _ => "kairos_mixed",
        }
    }

    /// Encode column name for data_points table (matches ingest service format)
    fn encode_column_name(timestamp: Timestamp) -> Vec<u8> {
        // Use the same encoding as ColumnName::to_bytes() in the ingest service
        // Java KairosDB format: 4-byte integer with offset left-shifted by 1
        let offset = timestamp.row_offset();
        let column_name = (offset as u32) << 1;
        (column_name as i32).to_be_bytes().to_vec()
    }

    /// Decode value bytes to DataPointValue based on data type
    fn decode_value(value_bytes: &[u8], data_type: &str) -> KairosResult<DataPointValue> {
        // Use the proper CassandraValue implementation for consistency
        let cassandra_value = crate::cassandra::CassandraValue {
            bytes: value_bytes.to_vec(),
            ttl: None,
        };
        cassandra_value.to_data_point_value(data_type)
    }

    /// Encode DataPointValue to bytes for storage
    fn encode_value(value: &DataPointValue) -> Vec<u8> {
        // Use the proper CassandraValue implementation for consistency
        let cassandra_value = crate::cassandra::CassandraValue::from_data_point_value(value, None);
        cassandra_value.bytes
    }

    /// Query the row_keys table to find all series for a metric in the time range
    async fn query_row_keys_for_metric(
        &self,
        metric: &MetricName,
        _time_range: TimeRange,
    ) -> KairosResult<Vec<DataPointsRowKey>> {
        // Query row_keys table to find all row keys for this metric
        // The row_keys table schema:
        // CREATE TABLE row_keys (
        //   metric text,
        //   table_name text,
        //   row_time timestamp,
        //   data_type text,
        //   tags frozen<map<text, text>>,
        //   mtime timeuuid static,
        //   value text,
        //   PRIMARY KEY ((metric, table_name, row_time), data_type, tags)
        // )

        // Since row_time is part of the partition key, we need to query each row_time separately
        // For now, let's get all row_times and filter in memory
        let query = "SELECT row_time, data_type, tags FROM row_keys WHERE metric = ? AND table_name = ? ALLOW FILTERING";

        let query_result = self
            .session
            .query_unpaged(
                query,
                (
                    metric.as_str(),
                    "data_points", // table_name is always "data_points" for time series data
                ),
            )
            .await
            .map_err(|e| KairosError::Cassandra(format!("Failed to query row_keys: {}", e)))?;

        let rows_result = query_result
            .into_rows_result()
            .map_err(|e| KairosError::Cassandra(format!("Failed to parse rows result: {:?}", e)))?;

        let mut row_keys = Vec::new();

        let rows_iter = rows_result
            .rows::<(CqlTimestamp, String, HashMap<String, String>)>()
            .map_err(|e| KairosError::Cassandra(format!("Failed to deserialize rows: {:?}", e)))?;

        for row_result in rows_iter {
            let (row_time_ts, data_type, tags_map) = row_result.map_err(|e| {
                KairosError::Cassandra(format!("Failed to deserialize row: {:?}", e))
            })?;

            // Note: We don't filter by time range here because row_time represents
            // the 3-week boundary start time, not the actual data point timestamps.
            // Time filtering happens later when querying actual data points.
            let row_time = row_time_ts.0;

            // Convert HashMap to BTreeMap for consistent ordering
            let tags: BTreeMap<String, String> = tags_map.into_iter().collect();

            row_keys.push(DataPointsRowKey {
                metric_name: metric.as_str().to_string(),
                cluster_name: "default".to_string(),
                timestamp: row_time,
                data_type,
                tags,
            });
        }

        Ok(row_keys)
    }
}

#[async_trait]
impl TimeSeriesStore for CassandraLegacyStore {
    async fn write_points(&self, points: Vec<DataPoint>) -> KairosResult<WriteResult> {
        if points.is_empty() {
            return Ok(WriteResult::success(0));
        }

        // Group points by series (metric + tags + data type)
        let mut series_map: HashMap<(String, String, String), Vec<DataPoint>> = HashMap::new();

        for point in points {
            let data_type = Self::get_data_type(&point.value);
            let tags_string = format_tagset(&point.tags);
            let key = (
                point.metric.as_str().to_string(),
                tags_string,
                data_type.to_string(),
            );
            series_map.entry(key).or_default().push(point);
        }

        let mut total_written = 0;
        let errors = Vec::new();

        // Process each series
        for ((metric_str, tags_str, data_type), series_points) in series_map {
            let metric = MetricName::from(metric_str.as_str());

            // Parse tags back from string format
            let tags = parse_tags(&tags_str);

            // Group by row time (3-week boundaries)
            let mut row_groups: HashMap<Timestamp, Vec<&DataPoint>> = HashMap::new();
            for point in &series_points {
                let row_time = Self::calculate_row_time(point.timestamp);
                row_groups.entry(row_time).or_default().push(point);
            }

            // Write each row group
            for (row_time, row_points) in row_groups {
                let row_key = DataPointsRowKey {
                    metric_name: metric.as_str().to_string(),
                    cluster_name: "default".to_string(),
                    timestamp: row_time.timestamp_millis(),
                    data_type: data_type.clone(),
                    tags: tags.clone(),
                };

                let key_bytes = row_key.to_bytes();

                // Write data points
                for point in row_points {
                    let column = Self::encode_column_name(point.timestamp);
                    let value = Self::encode_value(&point.value);

                    // Insert into data_points table
                    let insert_query =
                        "INSERT INTO data_points (key, column1, value) VALUES (?, ?, ?)";
                    self.session
                        .query_unpaged(insert_query, (key_bytes.clone(), column, value))
                        .await
                        .map_err(|e| {
                            KairosError::Cassandra(format!("Failed to insert data point: {}", e))
                        })?;

                    total_written += 1;
                }

                // Update row_keys table
                let index_query = "INSERT INTO row_keys (metric, table_name, row_time, data_type, tags) VALUES (?, ?, ?, ?, ?)";
                self.session
                    .query_unpaged(
                        index_query,
                        (
                            metric.as_str(),
                            "data_points",
                            CqlTimestamp(row_time.timestamp_millis()),
                            data_type.clone(),
                            tags.clone(), // Pass as HashMap/Map
                        ),
                    )
                    .await
                    .map_err(|e| {
                        KairosError::Cassandra(format!("Failed to update row_keys: {}", e))
                    })?;

                // Update string_index for metric name
                let metric_index_query =
                    "INSERT INTO string_index (key, column1, value) VALUES (?, ?, ?)";
                self.session
                    .query_unpaged(
                        metric_index_query,
                        (
                            "metric_names".as_bytes(),
                            metric.as_str(),
                            vec![0u8], // Empty value
                        ),
                    )
                    .await
                    .map_err(|e| {
                        KairosError::Cassandra(format!("Failed to update string_index: {}", e))
                    })?;
            }
        }

        Ok(WriteResult::success(total_written).with_errors(errors))
    }

    async fn query_points(
        &self,
        metric: &MetricName,
        tags: &TagFilter,
        time_range: TimeRange,
    ) -> KairosResult<Vec<DataPoint>> {
        // Phase 1: Query row_keys table to find all series for this metric
        let row_keys = self.query_row_keys_for_metric(metric, time_range.clone()).await?;

        // Phase 2: Filter row keys by tag criteria
        let filtered_keys: Vec<_> = row_keys
            .into_iter()
            .filter(|key| tags.matches(&key.tags))
            .collect();

        if filtered_keys.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 3: Query data points for each matching row key
        let mut all_points = Vec::new();

        for row_key in filtered_keys {
            // Build the row key bytes for the data_points table
            let key_bytes = row_key.to_bytes();

            // Query the data_points table
            // The data_points table schema (COMPACT STORAGE):
            // CREATE TABLE data_points (
            //   key blob,
            //   column1 blob,
            //   value blob,
            //   PRIMARY KEY (key, column1)
            // )

            // Build column range for the time range
            let start_column = Self::encode_column_name(time_range.start);
            let end_column = Self::encode_column_name(time_range.end);

            let query = "SELECT column1, value FROM data_points WHERE key = ? AND column1 >= ? AND column1 <= ?";

            let query_result = self
                .session
                .query_unpaged(
                    query,
                    (key_bytes.clone(), start_column.clone(), end_column.clone()),
                )
                .await
                .map_err(|e| {
                    KairosError::Cassandra(format!("Failed to query data_points: {}", e))
                })?;

            let rows_result = query_result.into_rows_result().map_err(|e| {
                KairosError::Cassandra(format!("Failed to parse data_points rows result: {:?}", e))
            })?;

            let rows_iter = rows_result.rows::<(Vec<u8>, Vec<u8>)>().map_err(|e| {
                KairosError::Cassandra(format!("Failed to deserialize data_points rows: {:?}", e))
            })?;

            for row_result in rows_iter {
                let (column_bytes, value_bytes) = row_result.map_err(|e| {
                    KairosError::Cassandra(format!(
                        "Failed to deserialize data_points row: {:?}",
                        e
                    ))
                })?;

                // Decode timestamp from 4-byte column name (Java KairosDB format)
                if column_bytes.len() >= 4 {
                    // Use existing ColumnName::from_bytes method for consistency
                    let column_name = crate::cassandra::ColumnName::from_bytes(&column_bytes)?;
                    let offset = column_name.offset;

                    // Reconstruct full timestamp = row_time + offset
                    let timestamp_millis = row_key.timestamp + offset;
                    let timestamp = Timestamp::from_millis(timestamp_millis)?;

                    // Decode value based on data type
                    let value = Self::decode_value(&value_bytes, &row_key.data_type)?;

                    let point = DataPoint {
                        metric: metric.clone(),
                        timestamp,
                        value,
                        tags: btreemap_to_tagset(&row_key.tags),
                        ttl: 0,
                    };
                    all_points.push(point);
                }
            }
        }

        // Sort by timestamp
        all_points.sort_by_key(|p| p.timestamp);

        Ok(all_points)
    }

    async fn list_metrics(&self, _time_range: Option<TimeRange>) -> KairosResult<Vec<MetricName>> {
        // Mock implementation - would query string_index for all metric names
        Ok(vec![
            MetricName::from("cpu.usage"),
            MetricName::from("memory.used"),
            MetricName::from("disk.free"),
        ])
    }

    async fn list_tags(
        &self,
        _metric: &MetricName,
        _time_range: Option<TimeRange>,
    ) -> KairosResult<DataStoreTagSet> {
        // Mock implementation - would query string_index and actual data
        let mut tag_set = DataStoreTagSet::new();

        tag_set.insert(
            "host".to_string(),
            vec![
                DataStoreTagValue::new("server1".to_string()).with_count(10),
                DataStoreTagValue::new("server2".to_string()).with_count(8),
            ],
        );

        tag_set.insert(
            "env".to_string(),
            vec![
                DataStoreTagValue::new("prod".to_string()).with_count(15),
                DataStoreTagValue::new("dev".to_string()).with_count(3),
            ],
        );

        Ok(tag_set)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_row_time() {
        // Test 3-week boundary calculation
        let timestamp = Timestamp::from_millis(1234567890000).unwrap();
        let row_time = CassandraLegacyStore::calculate_row_time(timestamp);

        // Should align to 3-week boundary
        let three_weeks_ms = 3 * 7 * 24 * 60 * 60 * 1000;
        assert_eq!(row_time.timestamp_millis() % three_weeks_ms, 0);
        assert!(row_time.timestamp_millis() <= timestamp.timestamp_millis());
    }

    #[test]
    fn test_format_parse_tags() {
        let tags: BTreeMap<String, String> = [
            ("host".to_string(), "server1".to_string()),
            ("env".to_string(), "prod".to_string()),
        ]
        .into_iter()
        .collect();

        let formatted = format_tags(&tags);
        let parsed = parse_tags(&formatted);

        assert_eq!(tags, parsed);
    }

    #[test]
    fn test_encode_column_name() {
        let timestamp = Timestamp::from_millis(1234567890000).unwrap();
        let encoded = CassandraLegacyStore::encode_column_name(timestamp);

        // Should be 4 bytes: Java KairosDB format
        assert_eq!(encoded.len(), 4);

        // Test that we can decode it back using ColumnName::from_bytes
        let column_name = crate::cassandra::ColumnName::from_bytes(&encoded).unwrap();
        let expected_offset = timestamp.row_offset();
        assert_eq!(column_name.offset, expected_offset);
    }

    #[tokio::test]
    #[ignore] // Requires Cassandra instance
    async fn test_basic_operations() {
        let store = CassandraLegacyStore::new("kairosdb".to_string())
            .await
            .unwrap();

        // Test write
        let points = vec![DataPoint {
            metric: MetricName::from("test.metric"),
            timestamp: Timestamp::from_millis(1234567890000).unwrap(),
            value: DataPointValue::Long(42),
            tags: TagSet::new(),
            ttl: 0,
        }];

        let result = store.write_points(points).await.unwrap();
        assert_eq!(result.points_written, 1);

        // Test query
        let query_result = store
            .query_points(
                &MetricName::from("test.metric"),
                &TagFilter::All,
                TimeRange::new(
                    Timestamp::from_millis(1234567890000).unwrap(),
                    Timestamp::from_millis(1234567900000).unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        assert!(!query_result.is_empty());

        // Test list operations
        let metrics = store.list_metrics(None).await.unwrap();
        assert!(!metrics.is_empty());

        let tags = store
            .list_tags(&MetricName::from("test.metric"), None)
            .await
            .unwrap();
        assert!(!tags.tags.is_empty());
    }
}
