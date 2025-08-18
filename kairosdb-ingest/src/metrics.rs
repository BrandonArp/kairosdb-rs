//! Metrics collection and reporting for the ingest service

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Thread-safe metrics collector
#[derive(Debug)]
pub struct MetricsCollector {
    /// Total data points ingested
    pub datapoints_total: AtomicU64,

    /// Total batches processed
    pub batches_total: AtomicU64,

    /// Total errors encountered
    pub errors_total: AtomicU64,

    /// Validation errors
    pub validation_errors_total: AtomicU64,

    /// Cassandra errors
    pub cassandra_errors_total: AtomicU64,

    /// Total processing time
    pub processing_time_total_ms: AtomicU64,

    /// Start time for rate calculations
    start_time: Instant,
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self {
            datapoints_total: AtomicU64::new(0),
            batches_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            validation_errors_total: AtomicU64::new(0),
            cassandra_errors_total: AtomicU64::new(0),
            processing_time_total_ms: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment data points counter
    pub fn increment_datapoints(&self, count: u64) {
        self.datapoints_total.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment batches counter
    pub fn increment_batches(&self) {
        self.batches_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment errors counter
    pub fn increment_errors(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment validation errors counter
    pub fn increment_validation_errors(&self) {
        self.validation_errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment Cassandra errors counter
    pub fn increment_cassandra_errors(&self) {
        self.cassandra_errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Add processing time
    pub fn add_processing_time(&self, duration: Duration) {
        let millis = duration.as_millis() as u64;
        self.processing_time_total_ms
            .fetch_add(millis, Ordering::Relaxed);
    }

    /// Get current metrics snapshot
    pub fn snapshot(&self) -> MetricsSnapshot {
        let uptime = self.start_time.elapsed();
        let datapoints = self.datapoints_total.load(Ordering::Relaxed);
        let batches = self.batches_total.load(Ordering::Relaxed);

        MetricsSnapshot {
            datapoints_total: datapoints,
            batches_total: batches,
            errors_total: self.errors_total.load(Ordering::Relaxed),
            validation_errors_total: self.validation_errors_total.load(Ordering::Relaxed),
            cassandra_errors_total: self.cassandra_errors_total.load(Ordering::Relaxed),
            processing_time_total_ms: self.processing_time_total_ms.load(Ordering::Relaxed),
            uptime_seconds: uptime.as_secs(),
            datapoints_per_second: if uptime.as_secs() > 0 {
                datapoints / uptime.as_secs()
            } else {
                0
            },
            batches_per_second: if uptime.as_secs() > 0 {
                batches / uptime.as_secs()
            } else {
                0
            },
        }
    }

    /// Generate Prometheus format metrics
    pub fn prometheus_format(&self) -> String {
        let snapshot = self.snapshot();

        format!(
            "# HELP kairosdb_ingest_datapoints_total Total number of data points ingested\n\
             # TYPE kairosdb_ingest_datapoints_total counter\n\
             kairosdb_ingest_datapoints_total {}\n\
             \n\
             # HELP kairosdb_ingest_batches_total Total number of batches processed\n\
             # TYPE kairosdb_ingest_batches_total counter\n\
             kairosdb_ingest_batches_total {}\n\
             \n\
             # HELP kairosdb_ingest_errors_total Total number of ingestion errors\n\
             # TYPE kairosdb_ingest_errors_total counter\n\
             kairosdb_ingest_errors_total {}\n\
             \n\
             # HELP kairosdb_ingest_validation_errors_total Total number of validation errors\n\
             # TYPE kairosdb_ingest_validation_errors_total counter\n\
             kairosdb_ingest_validation_errors_total {}\n\
             \n\
             # HELP kairosdb_ingest_cassandra_errors_total Total number of Cassandra errors\n\
             # TYPE kairosdb_ingest_cassandra_errors_total counter\n\
             kairosdb_ingest_cassandra_errors_total {}\n\
             \n\
             # HELP kairosdb_ingest_processing_time_total_ms Total processing time in milliseconds\n\
             # TYPE kairosdb_ingest_processing_time_total_ms counter\n\
             kairosdb_ingest_processing_time_total_ms {}\n\
             \n\
             # HELP kairosdb_ingest_uptime_seconds Service uptime in seconds\n\
             # TYPE kairosdb_ingest_uptime_seconds gauge\n\
             kairosdb_ingest_uptime_seconds {}\n\
             \n\
             # HELP kairosdb_ingest_datapoints_per_second Data points ingested per second\n\
             # TYPE kairosdb_ingest_datapoints_per_second gauge\n\
             kairosdb_ingest_datapoints_per_second {}\n\
             \n\
             # HELP kairosdb_ingest_batches_per_second Batches processed per second\n\
             # TYPE kairosdb_ingest_batches_per_second gauge\n\
             kairosdb_ingest_batches_per_second {}\n",
            snapshot.datapoints_total,
            snapshot.batches_total,
            snapshot.errors_total,
            snapshot.validation_errors_total,
            snapshot.cassandra_errors_total,
            snapshot.processing_time_total_ms,
            snapshot.uptime_seconds,
            snapshot.datapoints_per_second,
            snapshot.batches_per_second
        )
    }
}

/// Snapshot of metrics at a point in time
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub datapoints_total: u64,
    pub batches_total: u64,
    pub errors_total: u64,
    pub validation_errors_total: u64,
    pub cassandra_errors_total: u64,
    pub processing_time_total_ms: u64,
    pub uptime_seconds: u64,
    pub datapoints_per_second: u64,
    pub batches_per_second: u64,
}

/// Helper for timing operations
pub struct Timer {
    start: Instant,
}

impl Timer {
    /// Start a new timer
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Get elapsed duration
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Finish timing and record to metrics collector
    pub fn finish(self, collector: &MetricsCollector) {
        collector.add_processing_time(self.elapsed());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_metrics_collector() {
        let collector = MetricsCollector::new();

        collector.increment_datapoints(100);
        collector.increment_batches();
        collector.increment_errors();

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.datapoints_total, 100);
        assert_eq!(snapshot.batches_total, 1);
        assert_eq!(snapshot.errors_total, 1);
    }

    #[test]
    fn test_prometheus_format() {
        let collector = MetricsCollector::new();
        collector.increment_datapoints(42);

        let metrics = collector.prometheus_format();
        assert!(metrics.contains("kairosdb_ingest_datapoints_total 42"));
        assert!(metrics.contains("# HELP"));
        assert!(metrics.contains("# TYPE"));
    }

    #[test]
    fn test_timer() {
        let collector = MetricsCollector::new();
        let timer = Timer::start();

        thread::sleep(Duration::from_millis(10));
        timer.finish(&collector);

        let snapshot = collector.snapshot();
        assert!(snapshot.processing_time_total_ms >= 10);
    }
}
