//! Cassandra Operation Metrics
//!
//! Provides comprehensive Prometheus metrics for all Cassandra operations including
//! data point writes, index writes broken down by type, and latency tracking.

use prometheus::{register_counter, register_histogram, Counter, Histogram, HistogramOpts};
use std::time::Instant;

/// Comprehensive Cassandra operation metrics for monitoring performance
#[derive(Debug, Clone)]
pub struct CassandraOperationMetrics {
    // Data point write metrics
    pub datapoints_written_total: Counter,
    pub datapoints_write_errors_total: Counter,
    pub datapoint_write_duration_seconds: Histogram,

    // Index write metrics broken down by index type
    pub string_index_writes_total: Counter,
    pub string_index_write_errors_total: Counter,
    pub string_index_write_duration_seconds: Histogram,

    pub row_keys_index_writes_total: Counter,
    pub row_keys_index_write_errors_total: Counter,
    pub row_keys_index_write_duration_seconds: Histogram,

    pub row_key_time_index_writes_total: Counter,
    pub row_key_time_index_write_errors_total: Counter,
    pub row_key_time_index_write_duration_seconds: Histogram,

    // Metric name specific metrics (subset of string_index)
    pub metric_name_index_writes_total: Counter,
    pub metric_name_index_write_errors_total: Counter,
    pub metric_name_index_write_duration_seconds: Histogram,

    // Overall batch processing metrics
    pub batch_processing_duration_seconds: Histogram,
    pub batch_size_processed: Histogram,

    // Cache deduplication metrics
    pub index_writes_skipped_cache_hit_total: Counter,
    pub index_writes_performed_cache_miss_total: Counter,
}

impl CassandraOperationMetrics {
    /// Create new Cassandra operation metrics with Prometheus registration
    pub fn new() -> Result<Self, prometheus::Error> {
        Ok(Self {
            // Data point write metrics
            datapoints_written_total: register_counter!(
                "cassandra_datapoints_written_total",
                "Total number of data points successfully written to Cassandra data_points table"
            )?,
            datapoints_write_errors_total: register_counter!(
                "cassandra_datapoints_write_errors_total", 
                "Total number of data point write errors to Cassandra data_points table"
            )?,
            datapoint_write_duration_seconds: register_histogram!(HistogramOpts::new(
                "cassandra_datapoint_write_duration_seconds",
                "Duration of individual data point writes to Cassandra data_points table"
            ).buckets(vec![
                0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0
            ]))?,

            // String index metrics (metric names)
            string_index_writes_total: register_counter!(
                "cassandra_string_index_writes_total",
                "Total number of entries successfully written to Cassandra string_index table"
            )?,
            string_index_write_errors_total: register_counter!(
                "cassandra_string_index_write_errors_total",
                "Total number of string index write errors to Cassandra string_index table"  
            )?,
            string_index_write_duration_seconds: register_histogram!(HistogramOpts::new(
                "cassandra_string_index_write_duration_seconds",
                "Duration of individual string index writes to Cassandra string_index table"
            ).buckets(vec![
                0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0
            ]))?,

            // Row keys index metrics
            row_keys_index_writes_total: register_counter!(
                "cassandra_row_keys_index_writes_total",
                "Total number of entries successfully written to Cassandra row_keys table"
            )?,
            row_keys_index_write_errors_total: register_counter!(
                "cassandra_row_keys_index_write_errors_total",
                "Total number of row_keys index write errors to Cassandra row_keys table"
            )?,
            row_keys_index_write_duration_seconds: register_histogram!(HistogramOpts::new(
                "cassandra_row_keys_index_write_duration_seconds",
                "Duration of individual row_keys index writes to Cassandra row_keys table"
            ).buckets(vec![
                0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0
            ]))?,

            // Row key time index metrics
            row_key_time_index_writes_total: register_counter!(
                "cassandra_row_key_time_index_writes_total", 
                "Total number of entries successfully written to Cassandra row_key_time_index table"
            )?,
            row_key_time_index_write_errors_total: register_counter!(
                "cassandra_row_key_time_index_write_errors_total",
                "Total number of row_key_time_index write errors to Cassandra row_key_time_index table"
            )?,
            row_key_time_index_write_duration_seconds: register_histogram!(HistogramOpts::new(
                "cassandra_row_key_time_index_write_duration_seconds",
                "Duration of individual row_key_time_index writes to Cassandra row_key_time_index table"
            ).buckets(vec![
                0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0
            ]))?,

            // Metric name specific metrics (more granular than string_index)
            metric_name_index_writes_total: register_counter!(
                "cassandra_metric_name_index_writes_total",
                "Total number of metric name entries successfully written to string_index table"
            )?,
            metric_name_index_write_errors_total: register_counter!(
                "cassandra_metric_name_index_write_errors_total",
                "Total number of metric name index write errors to string_index table"
            )?,
            metric_name_index_write_duration_seconds: register_histogram!(HistogramOpts::new(
                "cassandra_metric_name_index_write_duration_seconds", 
                "Duration of individual metric name index writes to string_index table"
            ).buckets(vec![
                0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0
            ]))?,

            // Batch processing metrics
            batch_processing_duration_seconds: register_histogram!(HistogramOpts::new(
                "cassandra_batch_processing_duration_seconds",
                "Duration of complete batch processing including all writes and indexes"
            ).buckets(vec![
                0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0
            ]))?,
            batch_size_processed: register_histogram!(HistogramOpts::new(
                "cassandra_batch_size_processed",
                "Number of data points processed in each batch"
            ).buckets(vec![
                1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0
            ]))?,

            // Cache deduplication metrics
            index_writes_skipped_cache_hit_total: register_counter!(
                "cassandra_index_writes_skipped_cache_hit_total",
                "Total number of index writes skipped due to cache hits (already written)"
            )?,
            index_writes_performed_cache_miss_total: register_counter!(
                "cassandra_index_writes_performed_cache_miss_total", 
                "Total number of index writes performed due to cache misses (not previously written)"
            )?,
        })
    }

    /// Record successful data point write with timing
    pub fn record_datapoint_write(&self, duration: std::time::Duration) {
        self.datapoints_written_total.inc();
        self.datapoint_write_duration_seconds.observe(duration.as_secs_f64());
    }

    /// Record data point write error
    pub fn record_datapoint_write_error(&self) {
        self.datapoints_write_errors_total.inc();
    }

    /// Record successful string index write (metric names) with timing
    pub fn record_metric_name_index_write(&self, duration: std::time::Duration) {
        // Record in both general string_index and specific metric_name counters
        self.string_index_writes_total.inc();
        self.metric_name_index_writes_total.inc();
        
        let duration_secs = duration.as_secs_f64();
        self.string_index_write_duration_seconds.observe(duration_secs);
        self.metric_name_index_write_duration_seconds.observe(duration_secs);
    }

    /// Record string index write error (metric names)
    pub fn record_metric_name_index_write_error(&self) {
        self.string_index_write_errors_total.inc();
        self.metric_name_index_write_errors_total.inc();
    }

    /// Record successful row_keys index write with timing
    pub fn record_row_keys_index_write(&self, duration: std::time::Duration) {
        self.row_keys_index_writes_total.inc();
        self.row_keys_index_write_duration_seconds.observe(duration.as_secs_f64());
    }

    /// Record row_keys index write error
    pub fn record_row_keys_index_write_error(&self) {
        self.row_keys_index_write_errors_total.inc();
    }

    /// Record successful row_key_time_index write with timing
    pub fn record_row_key_time_index_write(&self, duration: std::time::Duration) {
        self.row_key_time_index_writes_total.inc();
        self.row_key_time_index_write_duration_seconds.observe(duration.as_secs_f64());
    }

    /// Record row_key_time_index write error
    pub fn record_row_key_time_index_write_error(&self) {
        self.row_key_time_index_write_errors_total.inc();
    }

    /// Record complete batch processing with timing and size
    pub fn record_batch_processed(&self, duration: std::time::Duration, batch_size: usize) {
        self.batch_processing_duration_seconds.observe(duration.as_secs_f64());
        self.batch_size_processed.observe(batch_size as f64);
    }

    /// Record cache hit (index write skipped)
    pub fn record_cache_hit(&self) {
        self.index_writes_skipped_cache_hit_total.inc();
    }

    /// Record cache miss (index write performed)
    pub fn record_cache_miss(&self) {
        self.index_writes_performed_cache_miss_total.inc();
    }
}

/// Timer helper for measuring operation durations
pub struct OperationTimer {
    start: Instant,
}

impl OperationTimer {
    /// Start a new timer
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Get elapsed duration since timer started
    pub fn elapsed(&self) -> std::time::Duration {
        self.start.elapsed()
    }

    /// Stop timer and return elapsed duration
    pub fn stop(self) -> std::time::Duration {
        self.elapsed()
    }
}