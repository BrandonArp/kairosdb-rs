# Data Store Abstraction Layer Design

## Overview

This document outlines a new data store abstraction layer for KairosDB-rs that enables flexible storage backend implementations while maintaining efficient time series operations. The design addresses time-based indexing, tag cardinality management, and batch operations.

## Core Design Principles

1. **Storage Agnostic**: Operations should be generic enough to implement on various backends (Cassandra, ClickHouse, TimescaleDB, S3+Parquet, etc.)
2. **Time-Optimized**: Leverage time-based partitioning for efficient range queries
3. **Tag Cardinality Aware**: Handle high cardinality tags without performance degradation
4. **Batch-First**: Optimize for batch writes and reads
5. **Schema Evolution**: Support data type changes and schema versioning

## Architecture Components

### 1. Core Trait: Minimal TimeSeriesStore

```rust
use async_trait::async_trait;
use kairosdb_core::{
    datapoint::DataPoint,
    error::KairosResult,
    metrics::MetricName,
    time::{TimeRange, Timestamp},
    tags::TagSet,
};
use std::collections::HashMap;

/// Core trait with minimal required functionality
/// All storage backends MUST implement these 4 operations
/// NOTE: Implementations MUST provide efficient indexing by time, metric, and tags
#[async_trait]
pub trait TimeSeriesStore: Send + Sync {
    /// Write data points with automatic indexing (core requirement)
    /// Implementation MUST maintain time-based and tag-based indexes
    async fn write_points(&self, points: Vec<DataPoint>) -> KairosResult<WriteResult>;
    
    /// Query time series data using indexes (core requirement)
    /// Implementation MUST use efficient time-range and tag filtering
    async fn query_points(
        &self,
        metric: &MetricName,
        tags: &TagFilter,
        time_range: TimeRange,
    ) -> KairosResult<Vec<DataPoint>>;
    
    /// List available metrics using indexes (core requirement for discovery)
    /// Implementation MUST use efficient metric name indexes scoped by time
    async fn list_metrics(&self, time_range: Option<TimeRange>) -> KairosResult<Vec<MetricName>>;
    
    /// List tags for a metric using indexes (core requirement for discovery)  
    /// Implementation MUST use efficient tag indexes scoped by time and metric
    async fn list_tags(&self, metric: &MetricName, time_range: Option<TimeRange>) -> KairosResult<TagSet>;
}

/// Simple structures for core operations
#[derive(Debug)]
pub struct WriteResult {
    pub points_written: usize,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum TagFilter {
    All,                           // No filtering
    Exact(HashMap<String, String>), // Exact tag matches
    Any(Vec<HashMap<String, String>>), // Match any of these tag sets
}
```

### 1a. Optional Extensions via Additional Traits

```rust
/// Optional: Advanced write operations
#[async_trait]
pub trait BatchedTimeSeriesStore: TimeSeriesStore {
    async fn write_batch(&self, batch: WriteBatch) -> KairosResult<WriteResult>;
    async fn write_multi_series(&self, batch: MultiSeriesBatch) -> KairosResult<WriteResult>;
}

/// Optional: Advanced query operations  
#[async_trait]
pub trait AdvancedTimeSeriesStore: TimeSeriesStore {
    async fn query_multi_series(&self, query: MultiSeriesQuery) -> KairosResult<MultiSeriesResult>;
    async fn scan_partitions(&self, scan: PartitionScan) -> KairosResult<ScanResult>;
}

/// Optional: Advanced index management and maintenance
#[async_trait] 
pub trait IndexManagementStore: TimeSeriesStore {
    /// Rebuild indexes for better performance (optional optimization)
    async fn rebuild_indexes(&self, time_range: Option<TimeRange>) -> KairosResult<()>;
    
    /// Get index statistics and health (optional monitoring)
    async fn get_index_stats(&self, metric: Option<&MetricName>) -> KairosResult<IndexStats>;
    
    /// Optimize index storage (optional maintenance)
    async fn optimize_indexes(&self, criteria: OptimizationCriteria) -> KairosResult<()>;
}

/// Optional: Maintenance operations
#[async_trait]
pub trait MaintainableTimeSeriesStore: TimeSeriesStore {
    async fn compact_partitions(&self, range: TimeRange) -> KairosResult<CompactionResult>;
    async fn delete_data(&self, criteria: DeleteCriteria) -> KairosResult<DeleteResult>;
    async fn estimate_storage(&self, query: StorageQuery) -> KairosResult<StorageEstimate>;
}

/// Optional: Streaming operations
#[async_trait]
pub trait StreamingTimeSeriesStore: TimeSeriesStore {
    async fn write_stream(&self, stream: DataPointStream) -> KairosResult<WriteResult>;
    async fn subscribe_changes(&self, filter: ChangeFilter) -> KairosResult<ChangeStream>;
}
```

### 1b. Default Implementations for Optional Traits

```rust
/// Provides naive implementations of advanced features using core operations
/// Storage backends can override for optimized implementations
impl<T: TimeSeriesStore> BatchedTimeSeriesStore for T {
    /// Default: Just call write_points() for each batch
    async fn write_batch(&self, batch: WriteBatch) -> KairosResult<WriteResult> {
        self.write_points(batch.points).await
    }
    
    /// Default: Call write_points() for each series sequentially  
    async fn write_multi_series(&self, batch: MultiSeriesBatch) -> KairosResult<WriteResult> {
        let mut total_written = 0;
        let mut errors = Vec::new();
        
        for series_batch in batch.series_batches {
            match self.write_points(series_batch.points).await {
                Ok(result) => total_written += result.points_written,
                Err(e) => errors.push(e.to_string()),
            }
        }
        
        Ok(WriteResult { points_written: total_written, errors })
    }
}

impl<T: TimeSeriesStore> AdvancedTimeSeriesStore for T {
    /// Default: Query each metric separately and combine results
    async fn query_multi_series(&self, query: MultiSeriesQuery) -> KairosResult<MultiSeriesResult> {
        let mut all_series = Vec::new();
        let mut total_points = 0;
        
        for metric_query in query.metrics {
            let tag_filter = TagFilter::All; // Simplified for default impl
            let points = self.query_points(&metric_query.metric, &tag_filter, query.time_range).await?;
            total_points += points.len();
            
            // Group points by series (naive implementation)
            let series = TimeSeries {
                series_key: SeriesKey::from_metric(&metric_query.metric), // Simplified
                data_points: points,
            };
            all_series.push(series);
        }
        
        Ok(MultiSeriesResult {
            series: all_series,
            query_stats: MultiQueryStatistics {
                total_series: all_series.len(),
                total_points,
                partitions_scanned: 1, // Unknown in default impl
                read_latency_ms: 0,    // Not tracked in default impl
                compression_ratio: 1.0,
            }
        })
    }
    
    /// Default: Not supported, return error
    async fn scan_partitions(&self, _scan: PartitionScan) -> KairosResult<ScanResult> {
        Err(KairosError::not_supported("Partition scanning not supported by this backend"))
    }
}
```

### 2. Data Model

#### Time-Based Partitioning

```rust
#[derive(Debug, Clone)]
pub struct Partition {
    pub id: PartitionId,
    pub time_boundary: Timestamp,  // Start of partition (e.g., 3-week boundary)
    pub duration_ms: i64,          // Partition duration (configurable)
    pub format_version: u32,       // Data format version
}

#[derive(Debug, Clone)]
pub struct PartitionId {
    pub metric: MetricName,
    pub time_bucket: i64,           // Time bucket identifier
    pub shard: Option<u32>,         // Optional sharding for scale
}
```

#### Series Identification

```rust
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct SeriesId {
    pub metric: MetricName,
    pub tags_hash: u64,             // Fast hash of tags for lookups
    pub data_type: String,
}

#[derive(Debug, Clone)]
pub struct SeriesKey {
    pub id: SeriesId,
    pub tags: TagSet,               // Actual tag key-value pairs
    pub time_range: TimeRange,      // Time range this series covers
    pub metadata: SeriesMetadata,
}

#[derive(Debug, Clone)]
pub struct SeriesMetadata {
    pub first_seen: Timestamp,
    pub last_seen: Timestamp,
    pub data_points_count: u64,
    pub approximate_size_bytes: u64,
}
```

### 3. Write Operations

#### Efficient Batch Writing

```rust
#[derive(Debug)]
pub struct WriteBatch {
    pub points: Vec<DataPoint>,
    pub write_options: WriteOptions,
}

// Multi-series batch for optimal storage layout
#[derive(Debug)]
pub struct MultiSeriesBatch {
    pub series_batches: Vec<SeriesBatch>,
    pub write_options: WriteOptions,
}

#[derive(Debug)]
pub struct SeriesBatch {
    pub series_key: SeriesKey,
    pub points: Vec<DataPoint>,
    pub time_range: TimeRange,
}

#[derive(Debug, Clone)]
pub struct WriteOptions {
    pub consistency: ConsistencyLevel,
    pub ttl_seconds: Option<u32>,
    pub deduplicate: bool,
    pub compression: CompressionType,
    pub coalesce_strategy: CoalesceStrategy,
}

#[derive(Debug)]
pub struct WriteResult {
    pub points_written: usize,
    pub points_rejected: usize,
    pub partitions_affected: Vec<PartitionId>,
    pub series_affected: usize,
    pub write_latency_ms: u64,
    pub compression_ratio: f64,
}
```

#### Coalescing Strategies for Optimal Writes

```rust
#[derive(Debug, Clone)]
pub enum CoalesceStrategy {
    // Group points by time windows for sequential writes
    TimeWindow {
        window_size: Duration,
        max_batch_size: usize,
    },
    
    // Group by series to optimize per-series writes
    BySeries {
        max_series_per_batch: usize,
        max_points_per_series: usize,
    },
    
    // Group by partition to minimize partition switching
    ByPartition {
        max_partitions_per_batch: usize,
    },
    
    // Adaptive based on data patterns
    Adaptive {
        target_batch_size_mb: usize,
        prefer_sequential: bool,
    },
}
```

#### Stream Writing (for real-time ingestion)

```rust
use futures::Stream;

pub type DataPointStream = Box<dyn Stream<Item = DataPoint> + Send>;

pub struct StreamWriteOptions {
    pub batch_size: usize,
    pub flush_interval_ms: u64,
    pub backpressure_threshold: usize,
    pub auto_coalesce: bool,
}
```

### 4. Query Operations

#### Time Series Query

```rust
#[derive(Debug, Clone)]
pub struct TimeSeriesQuery {
    pub metric: MetricName,
    pub time_range: TimeRange,
    pub tag_filters: Vec<TagFilter>,
    pub query_options: QueryOptions,
}

#[derive(Debug, Clone)]
pub enum TagFilter {
    Exact(String, String),           // key = value
    Prefix(String, String),          // key starts with value
    Regex(String, String),           // key matches regex
    In(String, Vec<String>),         // key in [values]
    Exists(String),                  // key exists
    NotExists(String),               // key doesn't exist
}

#[derive(Debug, Clone)]
pub struct QueryOptions {
    pub limit: Option<usize>,
    pub order: SortOrder,
    pub downsample: Option<DownsampleOptions>,
    pub fill_policy: FillPolicy,
    pub timeout_ms: u64,
}

#[derive(Debug)]
pub struct QueryResult {
    pub series: Vec<TimeSeries>,
    pub query_stats: QueryStatistics,
}

#[derive(Debug)]
pub struct TimeSeries {
    pub series_key: SeriesKey,
    pub data_points: Vec<DataPoint>,
}
```

#### Multi-Series Reading

```rust
#[derive(Debug, Clone)]
pub struct MultiSeriesQuery {
    pub metrics: Vec<MetricQuery>,
    pub time_range: TimeRange,
    pub read_options: ReadOptions,
}

#[derive(Debug, Clone)]
pub struct MetricQuery {
    pub metric: MetricName,
    pub tag_filters: Vec<TagFilter>,
    pub series_limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct ReadOptions {
    pub batch_size: usize,
    pub parallel_reads: usize,
    pub memory_limit_mb: usize,
    pub compression: bool,
    pub read_strategy: ReadStrategy,
}

#[derive(Debug, Clone)]
pub enum ReadStrategy {
    // Read series sequentially (memory efficient)
    Sequential,
    
    // Read series in parallel (faster but more memory)
    Parallel { max_concurrent: usize },
    
    // Read by time chunks across all series
    TimeChunked { chunk_size: Duration },
    
    // Adaptive based on cardinality and time range
    Adaptive,
}

#[derive(Debug)]
pub struct MultiSeriesResult {
    pub series: Vec<TimeSeries>,
    pub query_stats: MultiQueryStatistics,
}

#[derive(Debug)]
pub struct MultiQueryStatistics {
    pub total_series: usize,
    pub total_points: usize,
    pub partitions_scanned: usize,
    pub read_latency_ms: u64,
    pub compression_ratio: f64,
}
```

#### Partition Scanning (for aggregations)

```rust
#[derive(Debug)]
pub struct PartitionScan {
    pub partitions: Vec<PartitionId>,
    pub filter: DataPointFilter,
    pub projection: Projection,
    pub scan_options: ScanOptions,
}

#[derive(Debug)]
pub struct ScanOptions {
    pub parallel_scans: usize,
    pub batch_size: usize,
    pub memory_limit_mb: usize,
    pub partition_pruning: bool,
    pub pushdown_predicates: Vec<Predicate>,
}

#[derive(Debug)]
pub struct DataPointFilter {
    pub time_range: TimeRange,
    pub value_filters: Vec<ValueFilter>,
    pub sampling_rate: Option<f64>,
}

#[derive(Debug)]
pub enum ValueFilter {
    GreaterThan(f64),
    LessThan(f64),
    Between(f64, f64),
    NotNull,
}
```

### 5. Index Operations (Time-Partitioned)

#### Index Types

```rust
#[derive(Debug, Clone)]
pub enum IndexType {
    TimeIndex,        // Primary time-based index
    SeriesIndex,      // Metric + tags to series mapping  
    TagIndex,         // Tag key/value indexes partitioned by time
    InvertedIndex,    // Full-text search on tag values
}

#[derive(Debug)]
pub struct IndexUpdates {
    pub series_updates: Vec<SeriesIndexUpdate>,
    pub tag_updates: Vec<TagIndexUpdate>,
    pub update_options: IndexUpdateOptions,
}

#[derive(Debug)]
pub struct SeriesIndexUpdate {
    pub series_key: SeriesKey,
    pub operation: IndexOperation,
    pub time_range: TimeRange,
}

#[derive(Debug)]
pub struct TagIndexUpdate {
    pub metric: MetricName,
    pub tag_key: String,
    pub tag_value: String,
    pub time_bucket: i64,             // Time partition for efficient queries
    pub operation: IndexOperation,
    pub series_count: u64,            // Approximate count in this partition
}

#[derive(Debug)]
pub enum IndexOperation {
    Insert,
    Update,
    Delete,
    Merge,
}
```

#### Time-Partitioned Tag Indexes
The tag indexes are partitioned by time to enable efficient queries:
- When querying tags for a metric in a time range, only relevant partitions are scanned
- Avoids scanning the entire history of tags
- Each partition contains tags that appear in data within that time window

#### Tag Information

```rust
#[derive(Debug, Clone)]
pub struct TagInfo {
    pub tag_key: String,
    pub tag_values: Vec<TagValue>,
    pub time_range: TimeRange,        // Time range this info covers
}

#[derive(Debug, Clone)]
pub struct TagValue {
    pub value: String,
    pub series_count: u64,             // Number of series with this tag in time range
    pub approximate_cardinality: u64,  // Estimated unique values
}

#[derive(Debug, Clone)]
pub struct MetricInfo {
    pub name: MetricName,
    pub data_types: Vec<String>,
    pub tag_keys: Vec<String>,        // Tag keys found in the queried time range
    pub series_count: u64,             // Series count in the queried time range
    pub data_points_count: u64,
    pub time_range: Option<TimeRange>, // Time range this info represents
}
```

#### Index Queries

```rust
#[derive(Debug)]
pub struct IndexQuery {
    pub index_type: IndexType,
    pub filter: IndexFilter,
    pub time_range: Option<TimeRange>,  // Scope query to time range
    pub projection: Vec<String>,
}

#[derive(Debug)]
pub enum IndexFilter {
    MetricName(String),
    TagKey(String),
    TagValue(String, String),
    TimeRange(TimeRange),
    Composite(Vec<IndexFilter>),
}

#[derive(Debug)]
pub struct IndexResult {
    pub entries: Vec<IndexEntry>,
    pub is_partial: bool,                // If results are incomplete
}
```

### 6. Tag Cardinality Management

```rust
#[derive(Debug)]
pub struct CardinalityControl {
    pub strategy: CardinalityStrategy,
    pub limits: CardinalityLimits,
}

#[derive(Debug)]
pub enum CardinalityStrategy {
    // Store high-cardinality tags separately
    SeparateHighCardinality {
        threshold: usize,
        storage_tier: StorageTier,
    },
    
    // Use bloom filters for existence checks
    BloomFilter {
        false_positive_rate: f64,
        estimated_items: usize,
    },
    
    // Hierarchical aggregation
    HierarchicalRollup {
        levels: Vec<AggregationLevel>,
    },
    
    // Tag sampling for high cardinality
    Sampling {
        sample_rate: f64,
        priority_tags: Vec<String>,
    },
}

#[derive(Debug)]
pub struct CardinalityLimits {
    pub max_series_per_metric: usize,
    pub max_tags_per_series: usize,
    pub max_tag_values: usize,
}
```

### 7. Storage Backend Implementations

#### Cassandra Implementation using Time-Windowed Strategy

Based on KairosDB's proven approach but with cleaner abstractions:

```rust
pub struct CassandraTimeSeriesStore {
    session: Session,
    schema: CassandraTimeSeriesSchema,
    time_partitioner: TimePartitioner,
    prepared_statements: PreparedStatements,
}

/// Cassandra schema optimized for time series storage
pub struct CassandraTimeSeriesSchema {
    // Core data storage - wide row format optimized for time range queries
    pub metrics_data: String,          // (series_key) -> (timestamp) -> value
    
    // Time window discovery - find which time windows contain data for a metric
    pub metric_time_windows: String,   // (metric, table_name, time_window) for discovery
    
    // Series metadata - full series information partitioned by time windows  
    pub series_metadata: String,       // (metric, table_name, time_window, data_type, tags) -> metadata
    
    // Discovery indexes for UI/API
    pub metric_names: String,          // Global metric name discovery
    pub tag_catalog: String,           // Tag key/value catalog
}

impl CassandraTimeSeriesStore {
    /// Three-phase query strategy for optimal performance
    async fn query_points(&self, metric: &MetricName, tags: &TagFilter, time_range: TimeRange) -> KairosResult<Vec<DataPoint>> {
        // Phase 1: Find time windows that contain data for this metric
        let time_windows = self.find_active_time_windows(metric, time_range).await?;
        
        // Phase 2: For each time window, get all series metadata (parallel queries)
        let series_metadata = self.get_series_in_time_windows(metric, &time_windows).await?;
        
        // Phase 3: Filter by tags and query data points (parallel queries)
        let filtered_series = self.filter_series_by_tags(series_metadata, tags);
        let data_points = self.query_series_data(filtered_series, time_range).await?;
        
        Ok(data_points.into_iter().flatten().collect())
    }
}
```

### Cassandra Table Schemas

```sql
-- Core data storage: wide rows for efficient time range scans
CREATE TABLE metrics_data (
    series_key blob,              -- Encoded: metric + data_type + tags_hash
    timestamp bigint,             -- Milliseconds since epoch  
    value blob,                   -- Encoded data point value
    PRIMARY KEY (series_key, timestamp)
) WITH CLUSTERING ORDER BY (timestamp ASC)
  AND compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'TimeWindowCompactionStrategy'};

-- Time window discovery: find when metrics have data
CREATE TABLE metric_time_windows (
    metric text,
    table_name text,              -- Always 'metrics_data' (for future extensibility)
    time_window timestamp,        -- Start of time window (e.g., 3-week boundary)
    PRIMARY KEY (metric, table_name, time_window)
) WITH CLUSTERING ORDER BY (table_name ASC, time_window ASC);

-- Series metadata: comprehensive series info partitioned by time windows
CREATE TABLE series_metadata (
    metric text,
    table_name text,
    time_window timestamp,        -- Time window this series appeared in
    data_type text,               -- kairos_long, kairos_double, etc.
    tags map<text, text>,         -- Full tag key-value pairs
    series_key blob,              -- Encoded series key for data lookup
    first_seen timestamp,         -- When series first appeared
    last_seen timestamp,          -- When series was last written
    point_count bigint,           -- Approximate point count
    PRIMARY KEY ((metric, table_name, time_window), data_type, tags)
);

-- Metric discovery: global catalog of metrics
CREATE TABLE metric_names (
    partition_key text,           -- Static partition key for discovery
    metric_name text,
    first_seen timestamp,
    last_seen timestamp,
    PRIMARY KEY (partition_key, metric_name)
);

-- Tag catalog: tag key/value discovery
CREATE TABLE tag_catalog (
    metric text,
    tag_key text,
    tag_value text,
    series_count counter,         -- Count of series with this tag
    PRIMARY KEY ((metric, tag_key), tag_value)
);
```

### Three-Phase Query Implementation

```rust
impl CassandraTimeSeriesStore {
    /// Phase 1: Find time windows containing data for the metric
    async fn find_active_time_windows(&self, metric: &MetricName, time_range: TimeRange) -> KairosResult<Vec<Timestamp>> {
        // Query: SELECT time_window FROM metric_time_windows 
        //        WHERE metric = ? AND table_name = 'metrics_data' 
        //        AND time_window >= ? AND time_window <= ?
        
        let start_window = self.time_partitioner.calculate_time_window(time_range.start);
        let end_window = self.time_partitioner.calculate_time_window(time_range.end);
        
        let statement = &self.prepared_statements.find_time_windows;
        let rows = self.session.execute(statement.bind((
            metric.as_str(),
            start_window.timestamp_millis(),
            end_window.timestamp_millis()
        ))).await?;
        
        let mut time_windows = Vec::new();
        for row in rows {
            if let Ok(ts) = row.try_get::<i64>("time_window") {
                time_windows.push(Timestamp::from_millis(ts)?);
            }
        }
        
        Ok(time_windows)
    }
    
    /// Phase 2: Get all series metadata for the time windows (parallel queries)
    async fn get_series_in_time_windows(&self, metric: &MetricName, time_windows: &[Timestamp]) -> KairosResult<Vec<SeriesMetadata>> {
        // Launch parallel queries for each time window
        let futures = time_windows.iter().map(|&time_window| {
            self.get_series_in_single_window(metric, time_window)
        });
        
        let results = futures::future::try_join_all(futures).await?;
        Ok(results.into_iter().flatten().collect())
    }
    
    async fn get_series_in_single_window(&self, metric: &MetricName, time_window: Timestamp) -> KairosResult<Vec<SeriesMetadata>> {
        // Query: SELECT data_type, tags, series_key, first_seen, last_seen, point_count
        //        FROM series_metadata 
        //        WHERE metric = ? AND table_name = 'metrics_data' AND time_window = ?
        
        let statement = &self.prepared_statements.get_series_metadata;
        let rows = self.session.execute(statement.bind((
            metric.as_str(),
            "metrics_data",
            time_window.timestamp_millis()
        ))).await?;
        
        let mut series_list = Vec::new();
        for row in rows {
            let metadata = SeriesMetadata {
                data_type: row.try_get("data_type")?,
                tags: row.try_get("tags")?,
                series_key: row.try_get("series_key")?,
                first_seen: Timestamp::from_millis(row.try_get("first_seen")?)?,
                last_seen: Timestamp::from_millis(row.try_get("last_seen")?)?,
                point_count: row.try_get("point_count")?,
            };
            series_list.push(metadata);
        }
        
        Ok(series_list)
    }
    
    /// Phase 3: Filter series by tag criteria and query data
    fn filter_series_by_tags(&self, series_list: Vec<SeriesMetadata>, tags: &TagFilter) -> Vec<SeriesMetadata> {
        match tags {
            TagFilter::All => series_list,
            TagFilter::Exact(required_tags) => {
                series_list.into_iter()
                    .filter(|series| {
                        required_tags.iter().all(|(key, value)| {
                            series.tags.get(key).map(|v| v == value).unwrap_or(false)
                        })
                    })
                    .collect()
            }
            TagFilter::Any(tag_sets) => {
                series_list.into_iter()
                    .filter(|series| {
                        tag_sets.iter().any(|required_tags| {
                            required_tags.iter().all(|(key, value)| {
                                series.tags.get(key).map(|v| v == value).unwrap_or(false)
                            })
                        })
                    })
                    .collect()
            }
        }
    }
    
    async fn query_series_data(&self, series_list: Vec<SeriesMetadata>, time_range: TimeRange) -> KairosResult<Vec<Vec<DataPoint>>> {
        // Launch parallel queries to metrics_data table
        let futures = series_list.into_iter().map(|series| {
            self.query_single_series_data(series, time_range)
        });
        
        futures::future::try_join_all(futures).await
    }
    
    async fn query_single_series_data(&self, series: SeriesMetadata, time_range: TimeRange) -> KairosResult<Vec<DataPoint>> {
        // Query: SELECT timestamp, value FROM metrics_data 
        //        WHERE series_key = ? AND timestamp >= ? AND timestamp <= ?
        
        let statement = &self.prepared_statements.query_series_data;
        let rows = self.session.execute(statement.bind((
            &series.series_key,
            time_range.start.timestamp_millis(),
            time_range.end.timestamp_millis()
        ))).await?;
        
        let mut data_points = Vec::new();
        for row in rows {
            let timestamp = Timestamp::from_millis(row.try_get("timestamp")?)?;
            let value_bytes: Vec<u8> = row.try_get("value")?;
            let value = self.decode_value(&value_bytes, &series.data_type)?;
            
            data_points.push(DataPoint {
                metric: series.metric.clone(),
                timestamp,
                value,
                tags: series.tags.clone(),
                ttl: 0, // Not stored in this implementation
            });
        }
        
        Ok(data_points)
    }
}
```

### Write Path Implementation

```rust
impl CassandraTimeSeriesStore {
    async fn write_points(&self, points: Vec<DataPoint>) -> KairosResult<WriteResult> {
        let mut write_batch = Vec::new();
        let mut index_updates = Vec::new();
        
        // Group points by series for efficient writing
        let grouped_points = self.group_points_by_series(points);
        
        for (series_key, points) in grouped_points {
            // Generate series metadata for index updates
            let series_metadata = self.create_series_metadata(&series_key, &points)?;
            index_updates.push(series_metadata);
            
            // Prepare data point writes
            for point in points {
                let encoded_value = self.encode_value(&point.value)?;
                write_batch.push(DataWrite {
                    series_key: series_key.clone(),
                    timestamp: point.timestamp.timestamp_millis(),
                    value: encoded_value,
                });
            }
        }
        
        // Execute writes in parallel
        let data_write_future = self.write_data_points(write_batch);
        let index_update_future = self.update_indexes(index_updates);
        
        let (data_result, index_result) = futures::future::try_join(
            data_write_future,
            index_update_future
        ).await?;
        
        Ok(WriteResult {
            points_written: data_result.points_written,
            errors: vec![], // Collect any partial failures
        })
    }
    
    async fn update_indexes(&self, updates: Vec<SeriesMetadata>) -> KairosResult<()> {
        let mut futures = Vec::new();
        
        for metadata in updates {
            let time_window = self.time_partitioner.calculate_time_window(metadata.first_seen);
            
            // Update metric_time_windows
            futures.push(self.update_metric_time_window(&metadata.metric, time_window));
            
            // Update series_metadata
            futures.push(self.update_series_metadata(metadata, time_window));
            
            // Update metric discovery
            futures.push(self.update_metric_catalog(&metadata.metric));
            
            // Update tag catalog
            futures.push(self.update_tag_catalog(&metadata.metric, &metadata.tags));
        }
        
        futures::future::try_join_all(futures).await?;
        Ok(())
    }
}
```

### Key Benefits of This Approach

1. **Time Window Efficiency**: Only query time windows that actually contain data
2. **Parallel Execution**: All phases use parallel queries where possible  
3. **Memory-Efficient Filtering**: Tag filtering happens in memory after metadata retrieval
4. **Scalable Schema**: Each table is properly partitioned for Cassandra's strengths
5. **Write Optimization**: Batch writes with parallel index updates
6. **Discovery Support**: Efficient metric and tag discovery for UI/API

### Performance Characteristics

- **Time Window Discovery**: O(log N) where N = number of time windows
- **Series Metadata Retrieval**: O(S/W) where S = total series, W = number of time windows
- **Tag Filtering**: O(M) where M = series in matching time windows  
- **Data Retrieval**: O(P) where P = number of matching series (parallel)

This approach scales well because:
- Time window pruning eliminates unnecessary work
- Parallel queries utilize Cassandra's distributed nature
- In-memory tag filtering is fast for reasonable cardinalities
- Schema design aligns with Cassandra's partition model

#### ClickHouse Implementation

```rust
pub struct ClickHouseStore {
    client: ClickHouseClient,
    tables: ClickHouseTables,
}

impl ClickHouseStore {
    // Use MergeTree engine with time-based partitioning
    // Leverage ClickHouse's columnar storage
    // Use materialized views for pre-aggregation
    
    // ClickHouse advantages for time-partitioned tags:
    // - Native DateTime partitioning for efficient range queries
    // - AggregatingMergeTree for tag cardinality stats
    // - Automatic partition pruning based on time predicates
}

// ClickHouse schema with time partitioning
pub struct ClickHouseTimePartitionedSchema {
    // Main time series table with tags as Map(String, String)
    // PARTITION BY toYYYYMM(timestamp)
    // ORDER BY (metric, tags_hash, timestamp)
    
    // Tag index materialized view
    // ENGINE = AggregatingMergeTree
    // PARTITION BY toStartOfWeek(timestamp)
    // GROUP BY metric, tag_key, tag_value
}
```

#### S3 + Parquet Implementation

```rust
pub struct ParquetStore {
    s3_client: S3Client,
    catalog: DataCatalog,
    cache: LocalCache,
}

impl ParquetStore {
    // Store data in Parquet files partitioned by time
    // Use AWS Athena or DuckDB for queries
    // Maintain metadata catalog for fast lookups
}
```

### 8. Optimization Strategies

#### Batch Operations

```rust
pub struct BatchOptimizer {
    pub coalesce_strategy: CoalesceStrategy,
    pub compression: CompressionSettings,
    pub parallelism: ParallelismSettings,
}

#[derive(Debug)]
pub enum CoalesceStrategy {
    TimeWindow(Duration),           // Group by time windows
    SizeThreshold(usize),           // Group by size
    Adaptive(AdaptiveSettings),     // Dynamic batching
}
```

#### Efficient Scanning

```rust
pub struct ScanOptimizer {
    pub pushdown_predicates: Vec<Predicate>,
    pub column_pruning: ColumnPruning,
    pub partition_pruning: PartitionPruning,
    pub cache_strategy: CacheStrategy,
}

#[derive(Debug)]
pub struct PartitionPruning {
    pub time_based: bool,
    pub statistics_based: bool,
    pub bloom_filter: Option<BloomFilter>,
}
```

### 9. Migration Path

#### Phase 1: Abstract Current Operations
1. Create trait implementations wrapping existing Cassandra code
2. Maintain backward compatibility with current storage format
3. Add comprehensive tests for trait compliance

#### Phase 2: Optimize Abstractions
1. Identify Cassandra-specific optimizations
2. Generalize them for other backends
3. Add performance benchmarks

#### Phase 3: Implement Alternative Backends
1. Start with ClickHouse for analytical workloads
2. Add S3+Parquet for cold storage
3. Support tiered storage strategies

### 10. Configuration

```rust
#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub backend: BackendType,
    pub connection: ConnectionConfig,
    pub partitioning: PartitioningConfig,
    pub indexing: IndexingConfig,
    pub performance: PerformanceConfig,
}

#[derive(Debug, Clone)]
pub struct PartitioningConfig {
    pub partition_duration_ms: i64,  // Default: 3 weeks
    pub max_partition_size_mb: usize,
    pub compaction_strategy: CompactionStrategy,
}

#[derive(Debug, Clone)]
pub struct IndexingConfig {
    pub enable_tag_index: bool,
    pub enable_series_cache: bool,
    pub cardinality_control: CardinalityControl,
}
```

## Benefits of This Design

1. **Storage Flexibility**: Easy to add new storage backends without changing application code
2. **Performance**: Optimized for time series workloads with proper partitioning and indexing
3. **Scalability**: Handles high cardinality through multiple strategies
4. **Maintainability**: Clear separation of concerns between storage and business logic
5. **Evolution**: Supports gradual migration and A/B testing of storage backends
6. **Efficient Tag Queries**: Time-partitioned indexes prevent scanning entire dataset
7. **Query Performance**: Tag lookups scoped to time ranges access only relevant partitions

## Implementation Strategy

### Phase 1: Minimal Viable Implementation (Week 1-2)
**Goal**: Get basic functionality working with existing Cassandra backend

1. **Define Core Trait**: Only 4 methods in `TimeSeriesStore`
2. **Cassandra Adapter**: Implement the 4 core methods using existing code
3. **Basic Tests**: Verify core operations work
4. **Simple CLI**: Test read/write operations

**Minimal Implementation Requirements:**
```rust
struct CassandraStore {
    client: CassandraClientImpl,  // Reuse existing
}

#[async_trait]
impl TimeSeriesStore for CassandraStore {
    async fn write_points(&self, points: Vec<DataPoint>) -> KairosResult<WriteResult> {
        // Use existing DataPointBatch and write_batch()
        // Automatically maintains:
        // - Time-based partitioning (3-week row boundaries)
        // - String index updates for metric names and tags
        // - Row key index for efficient querying
    }
    
    async fn query_points(&self, metric: &MetricName, tags: &TagFilter, time_range: TimeRange) -> KairosResult<Vec<DataPoint>> {
        // Efficient two-phase query using KairosDB index pattern:
        
        // Phase 1: Query row_key_index to get all series for this metric
        // Partition key: metric:data_type
        // Column range: time-based filtering
        let series_candidates = self.query_row_key_index(metric, time_range).await?;
        
        // Phase 2: Filter candidates by tag criteria in memory
        let matching_row_keys = self.filter_by_tags(series_candidates, tags)?;
        
        // Phase 3: Parallel queries to data_points table
        // Each query: row_key + column range (time_range)
        let futures = matching_row_keys.into_iter().map(|row_key| {
            self.query_data_points_for_row_key(row_key, time_range)
        });
        
        let results = futures::future::try_join_all(futures).await?;
        Ok(results.into_iter().flatten().collect())
    }
    
    async fn list_metrics(&self, time_range: Option<TimeRange>) -> KairosResult<Vec<MetricName>> {
        // Query string_index table for metric_names
        // Efficiently scoped by time partitions if time_range provided
    }
    
    async fn list_tags(&self, metric: &MetricName, time_range: Option<TimeRange>) -> KairosResult<TagSet> {
        // Query string_index table for tag_names/tag_values
        // Use metric + time-based filtering for efficiency
    }
}
```

### Phase 2: Add Default Implementations (Week 3)
**Goal**: Provide naive implementations of advanced features

1. **Default Trait Implementations**: Use blanket implementations
2. **Extended Testing**: Test that advanced features work (even if slow)
3. **Documentation**: Usage examples

### Phase 3: Optimize Cassandra Implementation (Week 4-5)
**Goal**: Override defaults with optimized Cassandra-specific implementations

1. **Implement BatchedTimeSeriesStore**: Use existing batch logic
2. **Implement AdvancedTimeSeriesStore**: Optimize multi-series queries
3. **Performance Testing**: Compare optimized vs default implementations

### Phase 4: Add Alternative Backend (Week 6+)
**Goal**: Prove the abstraction works with different storage

1. **ClickHouse Backend**: Implement core 4 methods
2. **Cross-Backend Tests**: Same test suite, different backends
3. **Performance Comparison**: Benchmark different backends

## Complexity Reduction Benefits

**Before (13 methods)** → **After (4 core methods)**
- **Implementation Effort**: 70% reduction in required code
- **Testing Complexity**: Simpler test matrix
- **Maintenance**: Fewer breaking changes
- **Onboarding**: New backends start simple, add optimizations later

**Progressive Enhancement Pattern**:
1. Implement 4 core methods → **Basic functionality works**
2. Get all advanced features for free via defaults → **Full API available**  
3. Override defaults selectively → **Optimize critical paths**
4. Add backend-specific traits → **Leverage unique capabilities**

## Open Questions

1. Should we support cross-backend queries (federated queries)?
2. How to handle consistency models across different backends?
3. Should we implement a query planner for optimization?
4. How to manage schema evolution and backward compatibility?
5. Should we support custom storage backends via plugins?
6. What's the optimal time bucket size for tag index partitions (hourly, daily, weekly)?
7. Should tag index partitions align with data partitions (3-week boundaries)?
8. How to handle sparse tags that appear infrequently?

## Next Steps

1. Review and refine the trait definitions
2. Create proof-of-concept implementation
3. Benchmark against current Cassandra implementation
4. Gather feedback and iterate on design
5. Plan incremental migration strategy