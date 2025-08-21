# KairosDB-rs Cassandra Schema

## Overview

This document defines the complete Cassandra keyspace schema for KairosDB-rs, optimized for time series storage with efficient time-based and tag-based indexing.

## Design Principles

1. **Time-Windowed Storage**: Data is partitioned by time windows (default: 3 weeks) for optimal range query performance
2. **Three-Phase Query Pattern**: Time window discovery → Series metadata retrieval → Data point queries
3. **Tag Cardinality Management**: Separate indexes for metric discovery and tag filtering
4. **Cassandra-Optimized**: Schema designed around Cassandra's strengths (wide rows, partition locality)

## Keyspace Definition

```sql
CREATE KEYSPACE IF NOT EXISTS kairosdb 
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
};

USE kairosdb;
```

## Core Data Tables

### 1. metrics_data - Primary Time Series Storage

The main table storing actual time series data points using Cassandra's wide row pattern.

```sql
CREATE TABLE metrics_data (
    series_key blob,                    -- Encoded: metric + data_type + tags_hash + time_window
    timestamp bigint,                   -- Data point timestamp (milliseconds since epoch)
    value blob,                         -- Encoded data point value (supports all KairosDB types)
    
    PRIMARY KEY (series_key, timestamp)
) WITH CLUSTERING ORDER BY (timestamp ASC)
  AND compression = {'class': 'LZ4Compressor'}
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': 1
  }
  AND gc_grace_seconds = 864000;
```

**Key Design:**
- `series_key`: Partition key containing all series identification info
- `timestamp`: Clustering column for efficient time range queries
- Wide row design: One row per series, many columns per row
- Time Window Compaction Strategy for automatic cleanup of old data

### 2. metric_time_windows - Time Window Discovery

Enables efficient discovery of which time windows contain data for a metric.

```sql
CREATE TABLE metric_time_windows (
    metric text,                        -- Metric name (e.g., "cpu.usage")
    table_name text,                    -- Always "metrics_data" (for future extensibility)
    time_window timestamp,              -- Start of time window (e.g., 3-week boundary)
    
    PRIMARY KEY (metric, table_name, time_window)
) WITH CLUSTERING ORDER BY (table_name ASC, time_window ASC)
  AND compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
```

**Query Pattern:**
```sql
-- Find all time windows with data for a metric in a time range
SELECT time_window 
FROM metric_time_windows 
WHERE metric = 'cpu.usage' 
  AND table_name = 'metrics_data'
  AND time_window >= '2024-01-01' 
  AND time_window <= '2024-02-01';
```

### 3. series_metadata - Series Information by Time Window

Contains detailed series metadata partitioned by time windows for efficient parallel queries.

```sql
CREATE TABLE series_metadata (
    metric text,                        -- Metric name
    table_name text,                    -- Always "metrics_data"
    time_window timestamp,              -- Time window boundary
    data_type text,                     -- kairos_long, kairos_double, kairos_histogram, etc.
    tags map<text, text>,              -- Full tag key-value pairs
    
    -- Metadata columns
    series_key blob,                    -- Encoded series key for data_points lookup
    first_seen timestamp,               -- When series first appeared in this time window
    last_seen timestamp,                -- When series was last written in this time window
    point_count bigint,                 -- Approximate number of points in this window
    
    PRIMARY KEY ((metric, table_name, time_window), data_type, tags)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
```

**Query Pattern:**
```sql
-- Get all series for a metric in a specific time window
SELECT data_type, tags, series_key, first_seen, last_seen, point_count
FROM series_metadata 
WHERE metric = 'cpu.usage' 
  AND table_name = 'metrics_data' 
  AND time_window = '2024-01-15 00:00:00';
```

## Discovery and Catalog Tables

### 4. metric_catalog - Metric Discovery

Time-partitioned catalog for efficient metric name discovery.

```sql
CREATE TABLE metric_catalog (
    time_bucket text,                   -- Time bucket (e.g., "2024-01", "2024-02")
    metric_name text,                   -- Full metric name
    
    -- Metadata
    first_seen timestamp,               -- When metric first appeared in this bucket
    last_seen timestamp,                -- When metric was last seen in this bucket
    data_types set<text>,              -- Set of data types seen for this metric
    series_count_estimate bigint,       -- Approximate series count in this bucket
    
    PRIMARY KEY (time_bucket, metric_name)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
```

**Query Pattern:**
```sql
-- Discover metrics active in recent time buckets
SELECT metric_name, data_types, series_count_estimate
FROM metric_catalog 
WHERE time_bucket IN ('2024-01', '2024-02', '2024-03');
```

### 5. tag_catalog - Tag Key/Value Discovery

Tag discovery partitioned by metric for efficient tag queries.

```sql
CREATE TABLE tag_catalog (
    metric text,                        -- Metric name (partition key)
    tag_key text,                       -- Tag key name
    tag_value text,                     -- Tag value
    
    -- Statistics
    series_count counter,               -- Number of series with this tag combination
    first_seen timestamp,               -- When this tag combo first appeared
    last_seen timestamp,                -- When this tag combo was last seen
    
    PRIMARY KEY ((metric, tag_key), tag_value)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
```

**Query Pattern:**
```sql
-- Get all values for a specific tag key on a metric
SELECT tag_value, series_count
FROM tag_catalog 
WHERE metric = 'cpu.usage' 
  AND tag_key = 'host';

-- Get all tag keys for a metric
SELECT DISTINCT tag_key 
FROM tag_catalog 
WHERE metric = 'cpu.usage';
```

## Legacy Compatibility Tables

### 6. string_index - Legacy String Index (Optional)

Maintained for backward compatibility with existing KairosDB deployments.

```sql
CREATE TABLE string_index (
    key blob,                           -- Index key (metric names, tag names, tag values)
    column1 text,                       -- Indexed string value
    value blob,                         -- Always empty (0x00)
    
    PRIMARY KEY (key, column1)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
```

## Time Partitioning Strategy

### Time Window Calculation

```rust
// Default: 3-week boundaries aligned to epoch
const DEFAULT_TIME_WINDOW_SIZE: Duration = Duration::weeks(3);
const EPOCH_OFFSET: Duration = Duration::weeks(0); // No offset

fn calculate_time_window(timestamp: Timestamp) -> Timestamp {
    let millis_since_epoch = timestamp.timestamp_millis();
    let window_size_millis = DEFAULT_TIME_WINDOW_SIZE.num_milliseconds();
    
    let window_boundary = (millis_since_epoch / window_size_millis) * window_size_millis;
    Timestamp::from_millis(window_boundary)
}
```

### Series Key Encoding

```rust
fn encode_series_key(
    metric: &str,
    data_type: &str, 
    tags: &BTreeMap<String, String>,
    time_window: Timestamp
) -> Vec<u8> {
    let mut key = Vec::new();
    
    // 1. Time window (for partition distribution)
    key.extend_from_slice(&time_window.timestamp_millis().to_be_bytes());
    
    // 2. Metric name length + bytes
    let metric_bytes = metric.as_bytes();
    key.extend_from_slice(&(metric_bytes.len() as u32).to_be_bytes());
    key.extend_from_slice(metric_bytes);
    
    // 3. Data type length + bytes  
    let type_bytes = data_type.as_bytes();
    key.extend_from_slice(&(type_bytes.len() as u32).to_be_bytes());
    key.extend_from_slice(type_bytes);
    
    // 4. Tags hash for uniqueness
    let tags_hash = calculate_tags_hash(tags);
    key.extend_from_slice(&tags_hash.to_be_bytes());
    
    key
}
```

## Prepared Statements

### Core Data Operations

```rust
pub struct PreparedStatements {
    // Data point operations
    pub insert_data_point: PreparedStatement,
    pub query_data_points: PreparedStatement,
    pub delete_data_points: PreparedStatement,
    
    // Time window discovery
    pub insert_time_window: PreparedStatement,
    pub find_time_windows: PreparedStatement,
    
    // Series metadata
    pub insert_series_metadata: PreparedStatement,
    pub query_series_metadata: PreparedStatement,
    
    // Discovery operations
    pub insert_metric_catalog: PreparedStatement,
    pub query_metric_catalog: PreparedStatement,
    pub insert_tag_catalog: PreparedStatement,
    pub query_tag_catalog: PreparedStatement,
}

impl PreparedStatements {
    pub async fn prepare_all(session: &Session) -> KairosResult<Self> {
        Ok(Self {
            // Data operations
            insert_data_point: session.prepare(
                "INSERT INTO metrics_data (series_key, timestamp, value) VALUES (?, ?, ?) USING TTL ?"
            ).await?,
            
            query_data_points: session.prepare(
                "SELECT timestamp, value FROM metrics_data WHERE series_key = ? AND timestamp >= ? AND timestamp <= ?"
            ).await?,
            
            delete_data_points: session.prepare(
                "DELETE FROM metrics_data WHERE series_key = ? AND timestamp >= ? AND timestamp <= ?"
            ).await?,
            
            // Time window operations
            insert_time_window: session.prepare(
                "INSERT INTO metric_time_windows (metric, table_name, time_window) VALUES (?, 'metrics_data', ?)"
            ).await?,
            
            find_time_windows: session.prepare(
                "SELECT time_window FROM metric_time_windows WHERE metric = ? AND table_name = 'metrics_data' AND time_window >= ? AND time_window <= ?"
            ).await?,
            
            // Series metadata operations
            insert_series_metadata: session.prepare(
                "INSERT INTO series_metadata (metric, table_name, time_window, data_type, tags, series_key, first_seen, last_seen, point_count) VALUES (?, 'metrics_data', ?, ?, ?, ?, ?, ?, ?)"
            ).await?,
            
            query_series_metadata: session.prepare(
                "SELECT data_type, tags, series_key, first_seen, last_seen, point_count FROM series_metadata WHERE metric = ? AND table_name = 'metrics_data' AND time_window = ?"
            ).await?,
            
            // Discovery operations
            insert_metric_catalog: session.prepare(
                "INSERT INTO metric_catalog (time_bucket, metric_name, first_seen, last_seen, data_types, series_count_estimate) VALUES (?, ?, ?, ?, ?, ?)"
            ).await?,
            
            query_metric_catalog: session.prepare(
                "SELECT metric_name, data_types, series_count_estimate FROM metric_catalog WHERE time_bucket = ?"
            ).await?,
            
            insert_tag_catalog: session.prepare(
                "UPDATE tag_catalog SET series_count = series_count + ?, first_seen = ?, last_seen = ? WHERE metric = ? AND tag_key = ? AND tag_value = ?"
            ).await?,
            
            query_tag_catalog: session.prepare(
                "SELECT tag_value, series_count FROM tag_catalog WHERE metric = ? AND tag_key = ?"
            ).await?,
        })
    }
}
```

## Query Execution Examples

### Three-Phase Query Implementation

```rust
impl CassandraTimeSeriesStore {
    /// Complete query implementation using three-phase pattern
    pub async fn query_time_series(
        &self,
        metric: &str,
        tag_filter: &TagFilter,
        time_range: (Timestamp, Timestamp)
    ) -> KairosResult<Vec<DataPoint>> {
        
        // Phase 1: Find active time windows for this metric
        let time_windows = self.find_active_time_windows(metric, time_range).await?;
        
        // Phase 2: Get series metadata for all time windows (parallel)
        let series_metadata = self.get_series_metadata_parallel(metric, &time_windows).await?;
        
        // Phase 3: Filter by tags and query data points (parallel)
        let filtered_series = self.filter_series_by_tags(series_metadata, tag_filter);
        let data_points = self.query_data_points_parallel(filtered_series, time_range).await?;
        
        Ok(data_points.into_iter().flatten().collect())
    }
    
    async fn find_active_time_windows(
        &self, 
        metric: &str, 
        (start, end): (Timestamp, Timestamp)
    ) -> KairosResult<Vec<Timestamp>> {
        let start_window = calculate_time_window(start);
        let end_window = calculate_time_window(end);
        
        let rows = self.session.execute(
            &self.prepared.find_time_windows,
            (metric, start_window.timestamp_millis(), end_window.timestamp_millis())
        ).await?;
        
        let mut windows = Vec::new();
        for row in rows {
            let window_millis: i64 = row.get("time_window")?;
            windows.push(Timestamp::from_millis(window_millis)?);
        }
        
        Ok(windows)
    }
    
    async fn get_series_metadata_parallel(
        &self, 
        metric: &str, 
        time_windows: &[Timestamp]
    ) -> KairosResult<Vec<SeriesMetadata>> {
        let futures = time_windows.iter().map(|&window| {
            let session = self.session.clone();
            let prepared = self.prepared.query_series_metadata.clone();
            let metric = metric.to_string();
            
            async move {
                let rows = session.execute(&prepared, (metric, window.timestamp_millis())).await?;
                let mut series = Vec::new();
                
                for row in rows {
                    series.push(SeriesMetadata {
                        data_type: row.get("data_type")?,
                        tags: row.get("tags")?,
                        series_key: row.get("series_key")?,
                        first_seen: Timestamp::from_millis(row.get("first_seen")?)?,
                        last_seen: Timestamp::from_millis(row.get("last_seen")?)?,
                        point_count: row.get("point_count")?,
                    });
                }
                
                KairosResult::Ok(series)
            }
        });
        
        let results = futures::future::try_join_all(futures).await?;
        Ok(results.into_iter().flatten().collect())
    }
}
```

### Write Path Implementation

```rust
impl CassandraTimeSeriesStore {
    /// Write data points with automatic index maintenance
    pub async fn write_data_points(&self, points: Vec<DataPoint>) -> KairosResult<WriteResult> {
        // Group points by series for efficient batching
        let grouped = self.group_by_series(points);
        let mut write_futures = Vec::new();
        let mut index_futures = Vec::new();
        
        for (series_info, points) in grouped {
            let time_window = calculate_time_window(points[0].timestamp);
            let series_key = encode_series_key(
                &series_info.metric,
                &series_info.data_type,
                &series_info.tags,
                time_window
            );
            
            // Prepare data point writes
            for point in &points {
                let encoded_value = encode_data_point_value(&point.value)?;
                write_futures.push(self.session.execute(
                    &self.prepared.insert_data_point,
                    (series_key.clone(), point.timestamp.timestamp_millis(), encoded_value, point.ttl)
                ));
            }
            
            // Prepare index updates
            index_futures.push(self.update_time_window_index(&series_info.metric, time_window));
            index_futures.push(self.update_series_metadata(&series_info, &series_key, &points, time_window));
            index_futures.push(self.update_metric_catalog(&series_info.metric, time_window));
            index_futures.push(self.update_tag_catalog(&series_info.metric, &series_info.tags));
        }
        
        // Execute all writes in parallel
        let (write_results, index_results) = futures::future::try_join(
            futures::future::try_join_all(write_futures),
            futures::future::try_join_all(index_futures)
        ).await?;
        
        Ok(WriteResult {
            points_written: write_results.len(),
            errors: vec![], // Collect any failures
        })
    }
}
```

## Performance Considerations

### Table-Specific Optimizations

**metrics_data:**
- Use TimeWindowCompactionStrategy for automatic cleanup
- Set appropriate TTL based on retention requirements
- Consider using DTCS for cold data
- Monitor partition sizes (should stay under 100MB)

**metric_time_windows:**
- Lightweight table, SizeTieredCompactionStrategy is fine
- Consider TTL if you don't need indefinite history
- Partition by metric provides good distribution

**series_metadata:**
- Most complex queries, ensure adequate caching
- Consider bloom filter tuning for tag lookups
- Monitor clustering column cardinality

**metric_catalog:**
- Time-bucket partitioning prevents hotspots
- Regularly compact old time buckets
- Tune bucket size based on metric ingestion rate

**tag_catalog:**
- Counter updates can be expensive
- Consider batching counter increments
- Monitor for counter overflow (very rare but possible)

### Query Performance Tips

1. **Time Window Pruning**: Always query smallest possible time range
2. **Parallel Execution**: Leverage multiple time windows with parallel queries
3. **Tag Filtering**: Do exact tag matching in CQL when possible, regex in application
4. **Result Limiting**: Use LIMIT clauses to prevent large result sets
5. **Prepared Statements**: Always use prepared statements for performance

### Monitoring Queries

```sql
-- Check partition sizes
SELECT token(series_key), count(*) as points_count 
FROM metrics_data 
WHERE series_key = ? 
GROUP BY token(series_key);

-- Monitor time window distribution
SELECT metric, count(*) as window_count 
FROM metric_time_windows 
GROUP BY metric 
ORDER BY window_count DESC;

-- Tag cardinality analysis
SELECT metric, tag_key, count(*) as value_count 
FROM tag_catalog 
GROUP BY metric, tag_key 
ORDER BY value_count DESC;
```

## Migration and Maintenance

### Schema Deployment

```sql
-- Create all tables with proper settings
SOURCE 'create_schema.cql';

-- Verify table creation
DESCRIBE TABLES;

-- Check table settings
DESCRIBE TABLE metrics_data;
```

### Data Migration from Legacy KairosDB

```rust
pub async fn migrate_from_legacy_kairosdb(&self) -> KairosResult<()> {
    // 1. Read from legacy data_points table
    // 2. Convert row keys to new series_key format
    // 3. Populate new index tables
    // 4. Verify data integrity
    // 5. Switch over applications
}
```

### Backup and Restore

```bash
# Create snapshot
nodetool snapshot kairosdb

# Restore specific tables
nodetool refresh kairosdb metrics_data
nodetool refresh kairosdb series_metadata
```

This schema provides a solid foundation for high-performance time series storage while maintaining compatibility with KairosDB's proven patterns.

<function_calls>
<invoke name="TodoWrite">
<parameter name="todos">[{"content": "Create comprehensive Cassandra schema document", "status": "completed"}, {"content": "Include all table definitions with comments", "status": "completed"}, {"content": "Add prepared statement examples", "status": "in_progress"}, {"content": "Document time partitioning strategy", "status": "completed"}, {"content": "Include performance considerations", "status": "pending"}]