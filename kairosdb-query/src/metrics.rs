//! Metrics collection and reporting for the query service

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Thread-safe metrics collector for query service
#[derive(Debug)]
pub struct QueryMetricsCollector {
    /// Total queries executed
    pub queries_total: AtomicU64,

    /// Total query errors
    pub errors_total: AtomicU64,

    /// Cache hits
    pub cache_hits_total: AtomicU64,

    /// Cache misses
    pub cache_misses_total: AtomicU64,

    /// Total data points returned
    pub datapoints_returned_total: AtomicU64,

    /// Total query execution time
    pub query_time_total_ms: AtomicU64,

    /// Slow queries (above threshold)
    pub slow_queries_total: AtomicU64,

    /// Service start time
    start_time: Instant,
}

impl Default for QueryMetricsCollector {
    fn default() -> Self {
        Self {
            queries_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            cache_hits_total: AtomicU64::new(0),
            cache_misses_total: AtomicU64::new(0),
            datapoints_returned_total: AtomicU64::new(0),
            query_time_total_ms: AtomicU64::new(0),
            slow_queries_total: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }
}

impl QueryMetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a completed query
    pub fn record_query(
        &self,
        duration: Duration,
        datapoints_returned: usize,
        slow_query_threshold_ms: u64,
    ) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
        self.datapoints_returned_total
            .fetch_add(datapoints_returned as u64, Ordering::Relaxed);

        let duration_ms = duration.as_millis() as u64;
        self.query_time_total_ms
            .fetch_add(duration_ms, Ordering::Relaxed);

        if duration_ms > slow_query_threshold_ms {
            self.slow_queries_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a query error
    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache hit
    pub fn record_cache_hit(&self) {
        self.cache_hits_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss
    pub fn record_cache_miss(&self) {
        self.cache_misses_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current metrics snapshot
    pub fn snapshot(&self) -> QueryMetricsSnapshot {
        let uptime = self.start_time.elapsed();
        let queries = self.queries_total.load(Ordering::Relaxed);
        let total_time = self.query_time_total_ms.load(Ordering::Relaxed);
        let cache_hits = self.cache_hits_total.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses_total.load(Ordering::Relaxed);

        QueryMetricsSnapshot {
            queries_total: queries,
            errors_total: self.errors_total.load(Ordering::Relaxed),
            cache_hits_total: cache_hits,
            cache_misses_total: cache_misses,
            datapoints_returned_total: self.datapoints_returned_total.load(Ordering::Relaxed),
            query_time_total_ms: total_time,
            slow_queries_total: self.slow_queries_total.load(Ordering::Relaxed),
            uptime_seconds: uptime.as_secs(),
            queries_per_second: if uptime.as_secs() > 0 {
                queries / uptime.as_secs()
            } else {
                0
            },
            avg_query_time_ms: if queries > 0 {
                total_time as f64 / queries as f64
            } else {
                0.0
            },
            cache_hit_rate: if cache_hits + cache_misses > 0 {
                cache_hits as f64 / (cache_hits + cache_misses) as f64
            } else {
                0.0
            },
        }
    }

    /// Generate Prometheus format metrics
    pub fn prometheus_format(&self) -> String {
        let snapshot = self.snapshot();

        format!(
            "# HELP kairosdb_query_queries_total Total number of queries executed\n\
             # TYPE kairosdb_query_queries_total counter\n\
             kairosdb_query_queries_total {}\n\
             \n\
             # HELP kairosdb_query_errors_total Total number of query errors\n\
             # TYPE kairosdb_query_errors_total counter\n\
             kairosdb_query_errors_total {}\n\
             \n\
             # HELP kairosdb_query_cache_hits_total Total number of cache hits\n\
             # TYPE kairosdb_query_cache_hits_total counter\n\
             kairosdb_query_cache_hits_total {}\n\
             \n\
             # HELP kairosdb_query_cache_misses_total Total number of cache misses\n\
             # TYPE kairosdb_query_cache_misses_total counter\n\
             kairosdb_query_cache_misses_total {}\n\
             \n\
             # HELP kairosdb_query_datapoints_returned_total Total number of data points returned\n\
             # TYPE kairosdb_query_datapoints_returned_total counter\n\
             kairosdb_query_datapoints_returned_total {}\n\
             \n\
             # HELP kairosdb_query_time_total_ms Total query execution time in milliseconds\n\
             # TYPE kairosdb_query_time_total_ms counter\n\
             kairosdb_query_time_total_ms {}\n\
             \n\
             # HELP kairosdb_query_slow_queries_total Total number of slow queries\n\
             # TYPE kairosdb_query_slow_queries_total counter\n\
             kairosdb_query_slow_queries_total {}\n\
             \n\
             # HELP kairosdb_query_uptime_seconds Service uptime in seconds\n\
             # TYPE kairosdb_query_uptime_seconds gauge\n\
             kairosdb_query_uptime_seconds {}\n\
             \n\
             # HELP kairosdb_query_queries_per_second Queries executed per second\n\
             # TYPE kairosdb_query_queries_per_second gauge\n\
             kairosdb_query_queries_per_second {}\n\
             \n\
             # HELP kairosdb_query_avg_time_ms Average query execution time in milliseconds\n\
             # TYPE kairosdb_query_avg_time_ms gauge\n\
             kairosdb_query_avg_time_ms {}\n\
             \n\
             # HELP kairosdb_query_cache_hit_rate Cache hit rate (0.0 to 1.0)\n\
             # TYPE kairosdb_query_cache_hit_rate gauge\n\
             kairosdb_query_cache_hit_rate {}\n",
            snapshot.queries_total,
            snapshot.errors_total,
            snapshot.cache_hits_total,
            snapshot.cache_misses_total,
            snapshot.datapoints_returned_total,
            snapshot.query_time_total_ms,
            snapshot.slow_queries_total,
            snapshot.uptime_seconds,
            snapshot.queries_per_second,
            snapshot.avg_query_time_ms,
            snapshot.cache_hit_rate
        )
    }
}

/// Snapshot of query metrics at a point in time
#[derive(Debug, Clone)]
pub struct QueryMetricsSnapshot {
    pub queries_total: u64,
    pub errors_total: u64,
    pub cache_hits_total: u64,
    pub cache_misses_total: u64,
    pub datapoints_returned_total: u64,
    pub query_time_total_ms: u64,
    pub slow_queries_total: u64,
    pub uptime_seconds: u64,
    pub queries_per_second: u64,
    pub avg_query_time_ms: f64,
    pub cache_hit_rate: f64,
}

/// Helper for timing query operations
pub struct QueryTimer {
    start: Instant,
}

impl QueryTimer {
    /// Start a new query timer
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
    pub fn finish(
        self,
        collector: &QueryMetricsCollector,
        datapoints_returned: usize,
        slow_threshold_ms: u64,
    ) {
        collector.record_query(self.elapsed(), datapoints_returned, slow_threshold_ms);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_query_metrics_collector() {
        let collector = QueryMetricsCollector::new();

        collector.record_query(Duration::from_millis(100), 1000, 500);
        collector.record_cache_hit();
        collector.record_error();

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.queries_total, 1);
        assert_eq!(snapshot.errors_total, 1);
        assert_eq!(snapshot.cache_hits_total, 1);
        assert_eq!(snapshot.datapoints_returned_total, 1000);
        assert_eq!(snapshot.avg_query_time_ms, 100.0);
    }

    #[test]
    fn test_prometheus_format() {
        let collector = QueryMetricsCollector::new();
        collector.record_query(Duration::from_millis(250), 500, 1000);

        let metrics = collector.prometheus_format();
        assert!(metrics.contains("kairosdb_query_queries_total 1"));
        assert!(metrics.contains("kairosdb_query_datapoints_returned_total 500"));
        assert!(metrics.contains("# HELP"));
        assert!(metrics.contains("# TYPE"));
    }

    #[test]
    fn test_cache_hit_rate() {
        let collector = QueryMetricsCollector::new();

        collector.record_cache_hit();
        collector.record_cache_hit();
        collector.record_cache_miss();

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.cache_hit_rate, 2.0 / 3.0);
    }

    #[test]
    fn test_query_timer() {
        let collector = QueryMetricsCollector::new();
        let timer = QueryTimer::start();

        thread::sleep(Duration::from_millis(10));
        timer.finish(&collector, 100, 1000);

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.queries_total, 1);
        assert_eq!(snapshot.datapoints_returned_total, 100);
        assert!(snapshot.avg_query_time_ms >= 10.0);
    }
}
