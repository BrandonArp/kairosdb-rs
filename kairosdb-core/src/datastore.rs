//! Data store abstraction layer for KairosDB-rs
//!
//! This module provides a flexible abstraction over different storage backends
//! (Cassandra, ClickHouse, TimescaleDB, etc.) while maintaining efficient
//! time series operations with proper indexing.

pub mod cassandra_legacy;

use async_trait::async_trait;
use std::collections::BTreeMap;
use std::time::Duration;

use crate::datapoint::DataPoint;
use crate::error::{KairosError, KairosResult};
use crate::metrics::MetricName;
use crate::time::TimeRange;

/// Core trait with minimal required functionality for time series storage.
/// All storage backends MUST implement these 4 operations.
///
/// # Design Philosophy
///
/// This trait follows the "minimal core, optional extensions" pattern:
/// - Only 4 methods are required to implement
/// - Implementations MUST provide efficient time, metric, and tag indexing
/// - Advanced features are available through extension traits with default implementations
/// - Storage backends can override defaults for optimization
///
/// # Implementation Requirements
///
/// All implementations MUST:
/// - Maintain time-based indexing for efficient range queries
/// - Provide tag-based filtering without full table scans
/// - Support metric name discovery scoped by time ranges
/// - Handle concurrent reads and writes safely
#[async_trait]
pub trait TimeSeriesStore: Send + Sync {
    /// Write data points with automatic indexing.
    ///
    /// Implementation MUST maintain:
    /// - Time-based partitioning for efficient range queries
    /// - Tag-based indexes for filtering without full scans
    /// - Metric name indexes for discovery
    ///
    /// # Arguments
    /// * `points` - Vector of data points to write
    ///
    /// # Returns
    /// * `WriteResult` - Information about the write operation
    ///
    /// # Example
    /// ```rust
    /// let points = vec![
    ///     DataPoint::new("cpu.usage", timestamp, 75.2, tags),
    ///     DataPoint::new("memory.used", timestamp, 1024, tags),
    /// ];
    /// let result = store.write_points(points).await?;
    /// println!("Wrote {} points", result.points_written);
    /// ```
    async fn write_points(&self, points: Vec<DataPoint>) -> KairosResult<WriteResult>;

    /// Query time series data using efficient indexing.
    ///
    /// Implementation MUST use:
    /// - Time-range filtering to minimize data scanned
    /// - Tag indexes to avoid full metric scans
    /// - Parallel queries when beneficial
    ///
    /// # Arguments
    /// * `metric` - Metric name to query
    /// * `tags` - Tag filter criteria
    /// * `time_range` - Time range to query
    ///
    /// # Returns
    /// * Vector of matching data points sorted by timestamp
    ///
    /// # Example
    /// ```rust
    /// let points = store.query_points(
    ///     &MetricName::try_from("cpu.usage")?,
    ///     &TagFilter::exact([("host", "server1")]),
    ///     TimeRange::from_duration(Duration::hours(1))
    /// ).await?;
    /// ```
    async fn query_points(
        &self,
        metric: &MetricName,
        tags: &TagFilter,
        time_range: TimeRange,
    ) -> KairosResult<Vec<DataPoint>>;

    /// List available metrics using efficient indexing.
    ///
    /// Implementation MUST:
    /// - Use metric name indexes, not full data scans
    /// - Support time-range scoping to show only active metrics
    /// - Handle large metric counts efficiently
    ///
    /// # Arguments
    /// * `time_range` - Optional time range to scope results
    ///
    /// # Returns
    /// * Vector of metric names
    ///
    /// # Example
    /// ```rust
    /// // List all metrics active in the last hour
    /// let metrics = store.list_metrics(
    ///     Some(TimeRange::from_duration(Duration::hours(1)))
    /// ).await?;
    /// ```
    async fn list_metrics(&self, time_range: Option<TimeRange>) -> KairosResult<Vec<MetricName>>;

    /// List tags for a metric using efficient indexing.
    ///
    /// Implementation MUST:
    /// - Use tag indexes scoped by metric and time
    /// - Return both tag keys and possible values
    /// - Handle high cardinality tags efficiently
    ///
    /// # Arguments
    /// * `metric` - Metric to get tags for
    /// * `time_range` - Optional time range to scope results
    ///
    /// # Returns
    /// * `TagSet` containing available tags
    ///
    /// # Example
    /// ```rust
    /// let tags = store.list_tags(
    ///     &MetricName::try_from("cpu.usage")?,
    ///     Some(TimeRange::from_duration(Duration::hours(1)))
    /// ).await?;
    /// ```
    async fn list_tags(
        &self,
        metric: &MetricName,
        time_range: Option<TimeRange>,
    ) -> KairosResult<TagSet>;
}

/// Result of a write operation
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// Number of data points successfully written
    pub points_written: usize,
    /// Number of data points rejected (validation failures, etc.)
    pub points_rejected: usize,
    /// Error messages for rejected points
    pub errors: Vec<String>,
    /// Write latency in milliseconds
    pub write_latency_ms: u64,
    /// Storage compression ratio achieved
    pub compression_ratio: f64,
}

impl WriteResult {
    pub fn success(points_written: usize) -> Self {
        Self {
            points_written,
            points_rejected: 0,
            errors: Vec::new(),
            write_latency_ms: 0,
            compression_ratio: 1.0,
        }
    }

    pub fn with_errors(mut self, errors: Vec<String>) -> Self {
        self.points_rejected = errors.len();
        self.errors = errors;
        self
    }
}

/// Tag filtering criteria for queries
#[derive(Debug, Clone)]
pub enum TagFilter {
    /// No tag filtering - return all series for the metric
    All,
    /// Exact tag matches - all specified tags must match exactly
    Exact(BTreeMap<String, String>),
    /// Match any of the provided tag sets
    Any(Vec<BTreeMap<String, String>>),
    /// Advanced filtering with multiple criteria
    Advanced(AdvancedTagFilter),
}

impl TagFilter {
    /// Create an exact tag filter from key-value pairs
    pub fn exact<I, K, V>(tags: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let tag_map = tags
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        TagFilter::Exact(tag_map)
    }

    /// Create a filter that matches any of the provided tag sets
    pub fn any<I>(tag_sets: I) -> Self
    where
        I: IntoIterator<Item = BTreeMap<String, String>>,
    {
        TagFilter::Any(tag_sets.into_iter().collect())
    }

    /// Check if this filter would match the given tags
    pub fn matches(&self, tags: &BTreeMap<String, String>) -> bool {
        match self {
            TagFilter::All => true,
            TagFilter::Exact(required) => required
                .iter()
                .all(|(key, value)| tags.get(key).map(|v| v == value).unwrap_or(false)),
            TagFilter::Any(tag_sets) => tag_sets.iter().any(|required| {
                required
                    .iter()
                    .all(|(key, value)| tags.get(key).map(|v| v == value).unwrap_or(false))
            }),
            TagFilter::Advanced(filter) => filter.matches(tags),
        }
    }
}

/// Advanced tag filtering with regex, existence checks, etc.
#[derive(Debug, Clone)]
pub struct AdvancedTagFilter {
    pub criteria: Vec<TagFilterCriterion>,
    pub logic: FilterLogic,
}

impl AdvancedTagFilter {
    pub fn matches(&self, tags: &BTreeMap<String, String>) -> bool {
        match self.logic {
            FilterLogic::And => self.criteria.iter().all(|c| c.matches(tags)),
            FilterLogic::Or => self.criteria.iter().any(|c| c.matches(tags)),
        }
    }
}

/// Individual tag filter criterion
#[derive(Debug, Clone)]
pub enum TagFilterCriterion {
    /// Exact key-value match
    Exact(String, String),
    /// Key exists with any value
    Exists(String),
    /// Key does not exist
    NotExists(String),
    /// Value matches regex pattern
    Regex(String, String),
    /// Value starts with prefix
    Prefix(String, String),
    /// Value is in a set of options
    In(String, Vec<String>),
}

impl TagFilterCriterion {
    pub fn matches(&self, tags: &BTreeMap<String, String>) -> bool {
        match self {
            TagFilterCriterion::Exact(key, value) => {
                tags.get(key).map(|v| v == value).unwrap_or(false)
            }
            TagFilterCriterion::Exists(key) => tags.contains_key(key),
            TagFilterCriterion::NotExists(key) => !tags.contains_key(key),
            TagFilterCriterion::Regex(key, pattern) => {
                if let Some(value) = tags.get(key) {
                    // In a real implementation, use regex crate
                    value.contains(pattern) // Simplified
                } else {
                    false
                }
            }
            TagFilterCriterion::Prefix(key, prefix) => tags
                .get(key)
                .map(|v| v.starts_with(prefix))
                .unwrap_or(false),
            TagFilterCriterion::In(key, values) => {
                tags.get(key).map(|v| values.contains(v)).unwrap_or(false)
            }
        }
    }
}

/// Logical operator for combining tag filter criteria
#[derive(Debug, Clone)]
pub enum FilterLogic {
    And,
    Or,
}

/// Set of tags with their possible values
#[derive(Debug, Clone)]
pub struct TagSet {
    /// Map of tag key to possible values
    pub tags: BTreeMap<String, Vec<TagValue>>,
}

impl TagSet {
    pub fn new() -> Self {
        Self {
            tags: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: String, values: Vec<TagValue>) {
        self.tags.insert(key, values);
    }

    pub fn get(&self, key: &str) -> Option<&Vec<TagValue>> {
        self.tags.get(key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.tags.keys()
    }
}

impl Default for TagSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about a tag value
#[derive(Debug, Clone)]
pub struct TagValue {
    /// The tag value
    pub value: String,
    /// Number of series with this tag value in the queried time range
    pub series_count: u64,
    /// Estimated cardinality for this tag value
    pub estimated_cardinality: u64,
}

impl TagValue {
    pub fn new(value: String) -> Self {
        Self {
            value,
            series_count: 1,
            estimated_cardinality: 1,
        }
    }

    pub fn with_count(mut self, series_count: u64) -> Self {
        self.series_count = series_count;
        self
    }
}

// Optional extension traits with default implementations

/// Optional: Advanced write operations
///
/// Storage backends can implement this for optimized batch writing.
/// Default implementations use the core `write_points` method.
#[async_trait]
pub trait BatchedTimeSeriesStore: TimeSeriesStore {
    /// Write a batch of data points with batch-specific options
    async fn write_batch(&self, batch: WriteBatch) -> KairosResult<WriteResult>;

    /// Write multiple series in an optimized batch
    async fn write_multi_series(&self, batch: MultiSeriesBatch) -> KairosResult<WriteResult>;

    /// Stream write for real-time ingestion
    async fn write_stream(&self, stream: DataPointStream) -> KairosResult<WriteResult>;
}

/// Default implementation for any TimeSeriesStore
#[async_trait]
impl<T: TimeSeriesStore> BatchedTimeSeriesStore for T {
    async fn write_batch(&self, batch: WriteBatch) -> KairosResult<WriteResult> {
        // Simple implementation: just call write_points
        self.write_points(batch.points).await
    }

    async fn write_multi_series(&self, batch: MultiSeriesBatch) -> KairosResult<WriteResult> {
        // Naive implementation: write each series sequentially
        let mut total_written = 0;
        let mut total_rejected = 0;
        let mut all_errors = Vec::new();

        for series_batch in batch.series_batches {
            match self.write_points(series_batch.points).await {
                Ok(result) => {
                    total_written += result.points_written;
                    total_rejected += result.points_rejected;
                    all_errors.extend(result.errors);
                }
                Err(e) => {
                    all_errors.push(e.to_string());
                }
            }
        }

        Ok(WriteResult {
            points_written: total_written,
            points_rejected: total_rejected,
            errors: all_errors,
            write_latency_ms: 0, // Not tracked in default impl
            compression_ratio: 1.0,
        })
    }

    async fn write_stream(&self, mut stream: DataPointStream) -> KairosResult<WriteResult> {
        use futures::StreamExt;

        let mut batch = Vec::new();
        let mut total_result = WriteResult::success(0);

        while let Some(point) = stream.next().await {
            batch.push(point);

            // Simple batching: flush every 1000 points
            if batch.len() >= 1000 {
                let result = self.write_points(std::mem::take(&mut batch)).await?;
                total_result.points_written += result.points_written;
                total_result.points_rejected += result.points_rejected;
                total_result.errors.extend(result.errors);
            }
        }

        // Flush remaining points
        if !batch.is_empty() {
            let result = self.write_points(batch).await?;
            total_result.points_written += result.points_written;
            total_result.points_rejected += result.points_rejected;
            total_result.errors.extend(result.errors);
        }

        Ok(total_result)
    }
}

/// Optional: Advanced query operations
#[async_trait]
pub trait AdvancedTimeSeriesStore: TimeSeriesStore {
    /// Query multiple metrics in parallel
    async fn query_multi_series(&self, query: MultiSeriesQuery) -> KairosResult<MultiSeriesResult>;

    /// Advanced query with aggregation and downsampling
    async fn query_aggregated(&self, query: AggregatedQuery) -> KairosResult<AggregatedResult>;

    /// Scan partitions for large-scale analytics
    async fn scan_partitions(&self, scan: PartitionScan) -> KairosResult<ScanResult>;
}

/// Default implementation for any TimeSeriesStore
#[async_trait]
impl<T: TimeSeriesStore> AdvancedTimeSeriesStore for T {
    async fn query_multi_series(&self, query: MultiSeriesQuery) -> KairosResult<MultiSeriesResult> {
        // Naive implementation: query each metric separately
        let mut all_series = Vec::new();
        let mut total_points = 0;

        for metric_query in &query.metrics {
            let points = self
                .query_points(
                    &metric_query.metric,
                    &metric_query.tag_filter,
                    query.time_range.clone(),
                )
                .await?;

            total_points += points.len();

            // Group points by series (simplified)
            let series = TimeSeries {
                metric: metric_query.metric.clone(),
                tags: BTreeMap::new(), // Simplified
                data_points: points,
            };
            all_series.push(series);
        }

        let series_count = all_series.len();
        Ok(MultiSeriesResult {
            series: all_series,
            query_stats: QueryStatistics {
                total_series: series_count,
                total_points,
                partitions_scanned: 1, // Unknown in default impl
                query_latency_ms: 0,   // Not tracked in default impl
            },
        })
    }

    async fn query_aggregated(&self, query: AggregatedQuery) -> KairosResult<AggregatedResult> {
        // Simple implementation: get raw data and aggregate in memory
        let points = self
            .query_points(&query.metric, &query.tag_filter, query.time_range.clone())
            .await?;

        // Basic aggregation (sum as example)
        let sum = points.iter().filter_map(|p| p.value.as_f64()).sum::<f64>();

        Ok(AggregatedResult {
            aggregated_points: vec![DataPoint::new_double(
                query.metric,
                query.time_range.end,
                sum,
            )],
            aggregation_info: AggregationInfo {
                aggregator_type: "sum".to_string(),
                sample_size: points.len(),
                time_range: query.time_range,
            },
        })
    }

    async fn scan_partitions(&self, _scan: PartitionScan) -> KairosResult<ScanResult> {
        Err(KairosError::not_supported(
            "Partition scanning not supported by this backend",
        ))
    }
}

/// Optional: Maintenance and management operations
#[async_trait]
pub trait MaintainableTimeSeriesStore: TimeSeriesStore {
    /// Compact old data for better performance
    async fn compact_data(&self, time_range: TimeRange) -> KairosResult<CompactionResult>;

    /// Delete data matching criteria
    async fn delete_data(&self, criteria: DeleteCriteria) -> KairosResult<DeleteResult>;

    /// Estimate storage requirements
    async fn estimate_storage(&self, query: StorageQuery) -> KairosResult<StorageEstimate>;

    /// Get health and statistics
    async fn get_health(&self) -> KairosResult<HealthStatus>;
}

// Supporting data structures for extension traits

/// Batch write configuration
#[derive(Debug)]
pub struct WriteBatch {
    pub points: Vec<DataPoint>,
    pub options: WriteOptions,
}

impl WriteBatch {
    pub fn new(points: Vec<DataPoint>) -> Self {
        Self {
            points,
            options: WriteOptions::default(),
        }
    }

    pub fn with_options(mut self, options: WriteOptions) -> Self {
        self.options = options;
        self
    }
}

/// Write options for batch operations
#[derive(Debug, Clone)]
pub struct WriteOptions {
    pub consistency_level: ConsistencyLevel,
    pub ttl_seconds: Option<u32>,
    pub compression: CompressionType,
    pub deduplication: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            consistency_level: ConsistencyLevel::LocalQuorum,
            ttl_seconds: None,
            compression: CompressionType::LZ4,
            deduplication: true,
        }
    }
}

/// Consistency level for writes
#[derive(Debug, Clone)]
pub enum ConsistencyLevel {
    One,
    LocalQuorum,
    Quorum,
    All,
}

/// Compression types
#[derive(Debug, Clone)]
pub enum CompressionType {
    None,
    LZ4,
    Snappy,
    Zstd,
}

/// Multi-series batch for optimized writes
#[derive(Debug)]
pub struct MultiSeriesBatch {
    pub series_batches: Vec<SeriesBatch>,
    pub options: WriteOptions,
}

/// Batch for a single series
#[derive(Debug)]
pub struct SeriesBatch {
    pub metric: MetricName,
    pub tags: BTreeMap<String, String>,
    pub points: Vec<DataPoint>,
}

/// Stream of data points for real-time writing
pub type DataPointStream = futures::stream::BoxStream<'static, DataPoint>;

/// Multi-series query configuration
#[derive(Debug)]
pub struct MultiSeriesQuery {
    pub metrics: Vec<MetricQuery>,
    pub time_range: TimeRange,
    pub options: QueryOptions,
}

/// Query for a single metric
#[derive(Debug)]
pub struct MetricQuery {
    pub metric: MetricName,
    pub tag_filter: TagFilter,
    pub series_limit: Option<usize>,
}

/// Query options
#[derive(Debug, Clone)]
pub struct QueryOptions {
    pub limit: Option<usize>,
    pub order: SortOrder,
    pub timeout_ms: u64,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            limit: None,
            order: SortOrder::Ascending,
            timeout_ms: 30_000, // 30 seconds
        }
    }
}

/// Sort order for query results
#[derive(Debug, Clone)]
pub enum SortOrder {
    Ascending,
    Descending,
}

/// Result of multi-series query
#[derive(Debug)]
pub struct MultiSeriesResult {
    pub series: Vec<TimeSeries>,
    pub query_stats: QueryStatistics,
}

/// Single time series data
#[derive(Debug)]
pub struct TimeSeries {
    pub metric: MetricName,
    pub tags: BTreeMap<String, String>,
    pub data_points: Vec<DataPoint>,
}

/// Query execution statistics
#[derive(Debug)]
pub struct QueryStatistics {
    pub total_series: usize,
    pub total_points: usize,
    pub partitions_scanned: usize,
    pub query_latency_ms: u64,
}

/// Aggregated query configuration
#[derive(Debug)]
pub struct AggregatedQuery {
    pub metric: MetricName,
    pub tag_filter: TagFilter,
    pub time_range: TimeRange,
    pub aggregator: AggregatorType,
    pub downsample: Option<DownsampleConfig>,
}

/// Types of aggregation
#[derive(Debug, Clone)]
pub enum AggregatorType {
    Sum,
    Mean,
    Max,
    Min,
    Count,
    StdDev,
    Percentile(f64),
}

/// Downsampling configuration
#[derive(Debug, Clone)]
pub struct DownsampleConfig {
    pub interval: Duration,
    pub aggregator: AggregatorType,
    pub fill_policy: FillPolicy,
}

/// How to handle missing values in downsampling
#[derive(Debug, Clone)]
pub enum FillPolicy {
    None,
    Zero,
    Previous,
    Linear,
    Null,
}

/// Result of aggregated query
#[derive(Debug)]
pub struct AggregatedResult {
    pub aggregated_points: Vec<DataPoint>,
    pub aggregation_info: AggregationInfo,
}

/// Information about performed aggregation
#[derive(Debug)]
pub struct AggregationInfo {
    pub aggregator_type: String,
    pub sample_size: usize,
    pub time_range: TimeRange,
}

/// Partition scan configuration
#[derive(Debug)]
pub struct PartitionScan {
    pub time_range: TimeRange,
    pub metrics: Vec<MetricName>,
    pub projection: Vec<String>,
    pub filter: ScanFilter,
}

/// Filtering for partition scans
#[derive(Debug)]
pub struct ScanFilter {
    pub tag_filters: Vec<TagFilter>,
    pub value_filter: Option<ValueFilter>,
}

/// Filter based on data point values
#[derive(Debug)]
pub enum ValueFilter {
    GreaterThan(f64),
    LessThan(f64),
    Between(f64, f64),
    NotNull,
}

/// Result of partition scan
#[derive(Debug)]
pub struct ScanResult {
    pub data_points: Vec<DataPoint>,
    pub scan_stats: ScanStatistics,
}

/// Statistics from partition scan
#[derive(Debug)]
pub struct ScanStatistics {
    pub partitions_scanned: usize,
    pub rows_examined: usize,
    pub rows_returned: usize,
    pub scan_latency_ms: u64,
}

/// Compaction operation result
#[derive(Debug)]
pub struct CompactionResult {
    pub partitions_compacted: usize,
    pub space_reclaimed_bytes: u64,
    pub compaction_time_ms: u64,
}

/// Data deletion criteria
#[derive(Debug)]
pub struct DeleteCriteria {
    pub time_range: TimeRange,
    pub metrics: Vec<MetricName>,
    pub tag_filter: Option<TagFilter>,
}

/// Result of delete operation
#[derive(Debug)]
pub struct DeleteResult {
    pub points_deleted: u64,
    pub series_deleted: usize,
    pub deletion_time_ms: u64,
}

/// Storage estimation query
#[derive(Debug)]
pub struct StorageQuery {
    pub time_range: TimeRange,
    pub metrics: Vec<MetricName>,
    pub include_indexes: bool,
}

/// Storage space estimate
#[derive(Debug)]
pub struct StorageEstimate {
    pub total_bytes: u64,
    pub data_bytes: u64,
    pub index_bytes: u64,
    pub compression_ratio: f64,
}

/// Health status of the data store
#[derive(Debug)]
pub struct HealthStatus {
    pub is_healthy: bool,
    pub connection_status: ConnectionStatus,
    pub disk_usage: DiskUsage,
    pub performance_metrics: PerformanceMetrics,
}

/// Connection health
#[derive(Debug)]
pub enum ConnectionStatus {
    Connected,
    Degraded,
    Disconnected,
}

/// Disk usage information
#[derive(Debug)]
pub struct DiskUsage {
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
}

/// Performance metrics
#[derive(Debug)]
pub struct PerformanceMetrics {
    pub avg_write_latency_ms: f64,
    pub avg_read_latency_ms: f64,
    pub write_throughput_per_sec: f64,
    pub read_throughput_per_sec: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_tag_filter_exact() {
        let filter = TagFilter::exact([("host", "server1"), ("env", "prod")]);

        let matching_tags: BTreeMap<String, String> = [
            ("host".into(), "server1".into()),
            ("env".into(), "prod".into()),
            ("region".into(), "us-east".into()),
        ]
        .into_iter()
        .collect();

        let non_matching_tags: BTreeMap<String, String> = [
            ("host".into(), "server2".into()),
            ("env".into(), "prod".into()),
        ]
        .into_iter()
        .collect();

        assert!(filter.matches(&matching_tags));
        assert!(!filter.matches(&non_matching_tags));
    }

    #[test]
    fn test_tag_filter_any() {
        let tag_set1: BTreeMap<String, String> =
            [("host".into(), "server1".into())].into_iter().collect();

        let tag_set2: BTreeMap<String, String> =
            [("host".into(), "server2".into())].into_iter().collect();

        let filter = TagFilter::any([tag_set1, tag_set2]);

        let matching_tags1: BTreeMap<String, String> = [
            ("host".into(), "server1".into()),
            ("env".into(), "prod".into()),
        ]
        .into_iter()
        .collect();

        let matching_tags2: BTreeMap<String, String> = [
            ("host".into(), "server2".into()),
            ("env".into(), "dev".into()),
        ]
        .into_iter()
        .collect();

        let non_matching_tags: BTreeMap<String, String> = [
            ("host".into(), "server3".into()),
            ("env".into(), "prod".into()),
        ]
        .into_iter()
        .collect();

        assert!(filter.matches(&matching_tags1));
        assert!(filter.matches(&matching_tags2));
        assert!(!filter.matches(&non_matching_tags));
    }

    #[test]
    fn test_write_result() {
        let result = WriteResult::success(100).with_errors(vec!["Invalid metric name".to_string()]);

        assert_eq!(result.points_written, 100);
        assert_eq!(result.points_rejected, 1);
        assert_eq!(result.errors.len(), 1);
    }

    #[test]
    fn test_tag_set() {
        let mut tag_set = TagSet::new();
        tag_set.insert(
            "host".to_string(),
            vec![
                TagValue::new("server1".to_string()).with_count(5),
                TagValue::new("server2".to_string()).with_count(3),
            ],
        );

        let hosts = tag_set.get("host").unwrap();
        assert_eq!(hosts.len(), 2);
        assert_eq!(hosts[0].value, "server1");
        assert_eq!(hosts[0].series_count, 5);
    }
}
