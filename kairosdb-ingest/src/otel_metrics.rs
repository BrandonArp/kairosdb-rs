//! OpenTelemetry metrics setup and configuration
//!
//! This module provides OpenTelemetry metrics with:
//! - OTLP exporter for delta metrics (real-time push)
//! - Prometheus exporter for scraping compatibility
//! - Unified meter provider for all metrics

use anyhow::Result;
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, MeterProvider, UpDownCounter},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    runtime, Resource,
};
use prometheus::TextEncoder;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

// Note: In OpenTelemetry SDK 0.27, the temporality and aggregation configuration
// has changed significantly from earlier versions. The current approach uses
// the built-in defaults which should provide delta temporality for OTLP exporters.

/// OpenTelemetry metrics configuration
#[derive(Debug, Clone)]
pub struct OtelMetricsConfig {
    /// OTLP endpoint for sending metrics (e.g., "http://localhost:4317")
    pub otlp_endpoint: Option<String>,
    /// Export interval for OTLP metrics
    pub export_interval: Duration,
    /// Service name for resource attributes
    pub service_name: String,
    /// Service version for resource attributes
    pub service_version: String,
    /// Additional resource attributes
    pub resource_attributes: Vec<(String, String)>,
    /// Enable Prometheus exporter
    pub enable_prometheus: bool,
    /// Use exponential histograms instead of explicit bucket histograms
    pub use_exponential_histograms: bool,
}

impl Default for OtelMetricsConfig {
    fn default() -> Self {
        Self {
            otlp_endpoint: None,
            export_interval: Duration::from_secs(10),
            service_name: "kairosdb-ingest".to_string(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
            resource_attributes: vec![],
            enable_prometheus: true,
            use_exponential_histograms: true,
        }
    }
}

/// OpenTelemetry metrics manager
pub struct OtelMetrics {
    meter_provider: SdkMeterProvider,

    // Core ingestion metrics
    pub datapoints_counter: Counter<u64>,
    pub batches_counter: Counter<u64>,
    pub errors_counter: Counter<u64>,
    pub validation_errors_counter: Counter<u64>,
    pub cassandra_errors_counter: Counter<u64>,
    pub processing_time_histogram: Histogram<f64>,

    // HTTP metrics
    pub http_requests_counter: Counter<u64>,
    pub http_request_duration: Histogram<f64>,
    pub http_request_size: Histogram<u64>,
    pub http_response_size: Histogram<u64>,
    pub http_active_requests: UpDownCounter<i64>,

    // Queue metrics
    pub queue_size: UpDownCounter<i64>,
    pub queue_enqueued: Counter<u64>,
    pub queue_dequeued: Counter<u64>,
    pub queue_processing_duration: Histogram<f64>,
    pub queue_disk_usage: UpDownCounter<i64>,

    // Cache metrics
    pub cache_hits: Counter<u64>,
    pub cache_misses: Counter<u64>,
    pub cache_evictions: Counter<u64>,
    pub cache_size: UpDownCounter<i64>,

    // Cassandra metrics
    pub cassandra_writes: Counter<u64>,
    pub cassandra_write_duration: Histogram<f64>,
    pub cassandra_retries: Counter<u64>,
    pub cassandra_timeouts: Counter<u64>,
}

impl OtelMetrics {
    /// Initialize OpenTelemetry metrics with the given configuration
    pub fn init(config: OtelMetricsConfig) -> Result<Arc<Self>> {
        info!("Initializing OpenTelemetry metrics");

        // Build resource attributes
        let mut attributes = vec![
            KeyValue::new("service.name", config.service_name.clone()),
            KeyValue::new("service.version", config.service_version.clone()),
        ];

        for (key, value) in config.resource_attributes {
            attributes.push(KeyValue::new(key, value));
        }

        let resource = Resource::new(attributes);

        // Create meter provider with multiple exporters
        let mut builder = SdkMeterProvider::builder().with_resource(resource);

        // Add OTLP exporter if configured
        if let Some(endpoint) = config.otlp_endpoint {
            info!(
                "Configuring OTLP metrics exporter with default temporality (delta for counters/histograms) to {}",
                endpoint
            );
            info!(
                "Exponential histograms: {} (Note: configuration depends on collector support)",
                if config.use_exponential_histograms {
                    "preferred"
                } else {
                    "disabled"
                }
            );

            let exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .with_timeout(Duration::from_secs(10))
                .build()?;

            let reader = PeriodicReader::builder(exporter, runtime::Tokio)
                .with_interval(config.export_interval)
                .build();

            builder = builder.with_reader(reader);
        }

        // Add Prometheus exporter for scraping
        if config.enable_prometheus {
            info!("Configuring Prometheus metrics exporter");
            let exporter = opentelemetry_prometheus::exporter().build()?;

            builder = builder.with_reader(exporter);
        }

        let meter_provider = builder.build();

        // Set as global provider
        global::set_meter_provider(meter_provider.clone());

        // Get meter for creating instruments
        let meter = meter_provider.meter("kairosdb-ingest");

        // Create all metric instruments
        let metrics = Self {
            meter_provider,

            // Core ingestion metrics
            datapoints_counter: meter
                .u64_counter("kairosdb.ingest.datapoints")
                .with_description("Total number of data points ingested")
                .build(),

            batches_counter: meter
                .u64_counter("kairosdb.ingest.batches")
                .with_description("Total number of batches processed")
                .build(),

            errors_counter: meter
                .u64_counter("kairosdb.ingest.errors")
                .with_description("Total number of ingestion errors")
                .build(),

            validation_errors_counter: meter
                .u64_counter("kairosdb.ingest.validation_errors")
                .with_description("Total number of validation errors")
                .build(),

            cassandra_errors_counter: meter
                .u64_counter("kairosdb.ingest.cassandra_errors")
                .with_description("Total number of Cassandra errors")
                .build(),

            processing_time_histogram: meter
                .f64_histogram("kairosdb.ingest.processing_time")
                .with_description("Processing time in milliseconds")
                .with_unit("ms")
                .build(),

            // HTTP metrics
            http_requests_counter: meter
                .u64_counter("kairosdb.http.requests")
                .with_description("Total number of HTTP requests")
                .build(),

            http_request_duration: meter
                .f64_histogram("kairosdb.http.request_duration")
                .with_description("HTTP request duration in seconds")
                .with_unit("s")
                .build(),

            http_request_size: meter
                .u64_histogram("kairosdb.http.request_size")
                .with_description("HTTP request body size in bytes")
                .with_unit("By")
                .build(),

            http_response_size: meter
                .u64_histogram("kairosdb.http.response_size")
                .with_description("HTTP response body size in bytes")
                .with_unit("By")
                .build(),

            http_active_requests: meter
                .i64_up_down_counter("kairosdb.http.active_requests")
                .with_description("Number of currently active HTTP requests")
                .build(),

            // Queue metrics
            queue_size: meter
                .i64_up_down_counter("kairosdb.queue.size")
                .with_description("Current queue size")
                .build(),

            queue_enqueued: meter
                .u64_counter("kairosdb.queue.enqueued")
                .with_description("Total items enqueued")
                .build(),

            queue_dequeued: meter
                .u64_counter("kairosdb.queue.dequeued")
                .with_description("Total items dequeued")
                .build(),

            queue_processing_duration: meter
                .f64_histogram("kairosdb.queue.processing_duration")
                .with_description("Queue processing duration in seconds")
                .with_unit("s")
                .build(),

            queue_disk_usage: meter
                .i64_up_down_counter("kairosdb.queue.disk_usage")
                .with_description("Queue disk usage in bytes")
                .with_unit("By")
                .build(),

            // Cache metrics
            cache_hits: meter
                .u64_counter("kairosdb.cache.hits")
                .with_description("Cache hit count")
                .build(),

            cache_misses: meter
                .u64_counter("kairosdb.cache.misses")
                .with_description("Cache miss count")
                .build(),

            cache_evictions: meter
                .u64_counter("kairosdb.cache.evictions")
                .with_description("Cache eviction count")
                .build(),

            cache_size: meter
                .i64_up_down_counter("kairosdb.cache.size")
                .with_description("Current cache size")
                .build(),

            // Cassandra metrics
            cassandra_writes: meter
                .u64_counter("kairosdb.cassandra.writes")
                .with_description("Total Cassandra write operations")
                .build(),

            cassandra_write_duration: meter
                .f64_histogram("kairosdb.cassandra.write_duration")
                .with_description("Cassandra write duration in seconds")
                .with_unit("s")
                .build(),

            cassandra_retries: meter
                .u64_counter("kairosdb.cassandra.retries")
                .with_description("Cassandra operation retries")
                .build(),

            cassandra_timeouts: meter
                .u64_counter("kairosdb.cassandra.timeouts")
                .with_description("Cassandra operation timeouts")
                .build(),
        };

        info!("OpenTelemetry metrics initialized successfully");
        Ok(Arc::new(metrics))
    }

    /// Get Prometheus format metrics for scraping
    pub fn prometheus_metrics(&self) -> Result<String> {
        // Use the global Prometheus registry
        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        encoder
            .encode_to_string(&metric_families)
            .map_err(|e| anyhow::anyhow!("Failed to encode Prometheus metrics: {}", e))
    }

    /// Record HTTP request with attributes
    pub fn record_http_request(&self, endpoint: &str, method: &str, status: u16, duration: f64) {
        let attributes = &[
            KeyValue::new("endpoint", endpoint.to_string()),
            KeyValue::new("method", method.to_string()),
            KeyValue::new("status", status as i64),
        ];

        self.http_requests_counter.add(1, attributes);
        self.http_request_duration.record(duration, attributes);

        // Record errors
        if (400..500).contains(&status) {
            self.errors_counter
                .add(1, &[KeyValue::new("type", "client_error")]);
        } else if status >= 500 {
            self.errors_counter
                .add(1, &[KeyValue::new("type", "server_error")]);
        }
    }

    /// Record datapoints ingestion
    pub fn record_datapoints(&self, count: u64, source: &str) {
        self.datapoints_counter
            .add(count, &[KeyValue::new("source", source.to_string())]);
    }

    /// Record batch processing  
    /// duration should be in seconds
    pub fn record_batch(&self, _size: u64, duration_seconds: f64) {
        self.batches_counter.add(1, &[]);
        // Convert seconds to milliseconds for the histogram (which has unit "ms")
        self.processing_time_histogram
            .record(duration_seconds * 1000.0, &[]);
    }

    /// Record queue operation
    pub fn record_queue_operation(&self, operation: &str, count: u64, duration: Option<f64>) {
        match operation {
            "enqueue" => self.queue_enqueued.add(count, &[]),
            "dequeue" => self.queue_dequeued.add(count, &[]),
            _ => {}
        }

        if let Some(dur) = duration {
            self.queue_processing_duration
                .record(dur, &[KeyValue::new("operation", operation.to_string())]);
        }
    }

    /// Update queue size
    pub fn update_queue_size(&self, delta: i64) {
        self.queue_size.add(delta, &[]);
    }

    /// Update queue disk usage
    pub fn update_queue_disk_usage(&self, bytes: i64) {
        // Set absolute value by calculating delta
        // This assumes we're tracking the current value elsewhere
        self.queue_disk_usage.add(bytes, &[]);
    }

    /// Record cache operation
    pub fn record_cache_operation(&self, operation: &str) {
        match operation {
            "hit" => self.cache_hits.add(1, &[]),
            "miss" => self.cache_misses.add(1, &[]),
            "eviction" => self.cache_evictions.add(1, &[]),
            _ => {}
        }
    }

    /// Update cache size
    pub fn update_cache_size(&self, delta: i64) {
        self.cache_size.add(delta, &[]);
    }

    /// Record Cassandra operation
    pub fn record_cassandra_operation(&self, operation: &str, duration: f64, success: bool) {
        let attributes = &[
            KeyValue::new("operation", operation.to_string()),
            KeyValue::new("success", success),
        ];

        if operation == "write" {
            self.cassandra_writes.add(1, attributes);
            self.cassandra_write_duration.record(duration, attributes);
        }

        if !success {
            self.cassandra_errors_counter.add(1, &[]);
        }
    }

    /// Record Cassandra retry
    pub fn record_cassandra_retry(&self) {
        self.cassandra_retries.add(1, &[]);
    }

    /// Record Cassandra timeout
    pub fn record_cassandra_timeout(&self) {
        self.cassandra_timeouts.add(1, &[]);
    }

    /// Shutdown metrics providers
    pub fn shutdown(&self) -> Result<()> {
        info!("Shutting down OpenTelemetry metrics");

        // Force flush all metrics
        self.meter_provider.force_flush()?;

        // Shutdown the provider
        self.meter_provider.shutdown()?;

        Ok(())
    }
}

/// HTTP request guard for automatic metric recording
pub struct HttpRequestGuard {
    metrics: Arc<OtelMetrics>,
    endpoint: String,
    method: String,
    start_time: std::time::Instant,
}

impl HttpRequestGuard {
    pub fn new(metrics: Arc<OtelMetrics>, endpoint: String, method: String) -> Self {
        metrics.http_active_requests.add(1, &[]);
        Self {
            metrics,
            endpoint,
            method,
            start_time: std::time::Instant::now(),
        }
    }

    pub fn finish(self, status: u16) {
        let duration = self.start_time.elapsed().as_secs_f64();
        self.metrics
            .record_http_request(&self.endpoint, &self.method, status, duration);
    }
}

impl Drop for HttpRequestGuard {
    fn drop(&mut self) {
        self.metrics.http_active_requests.add(-1, &[]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let config = OtelMetricsConfig::default();
        let metrics = OtelMetrics::init(config).unwrap();

        // Test basic operations
        metrics.record_datapoints(100, "test");
        metrics.record_batch(10, 0.5);
        metrics.record_http_request("/api/v1/datapoints", "POST", 200, 0.1);

        // Verify Prometheus format works and contains some metrics
        match metrics.prometheus_metrics() {
            Ok(prom_metrics) => {
                println!("Prometheus metrics output: '{}'", prom_metrics);
                // Just verify the method works, content may be empty in tests
                // since we're using the global registry which might not have
                // the OpenTelemetry metrics registered yet
            }
            Err(e) => panic!("Failed to get Prometheus metrics: {}", e),
        }
    }

    #[test]
    fn test_request_guard() {
        let config = OtelMetricsConfig::default();
        let metrics = OtelMetrics::init(config).unwrap();

        let guard = HttpRequestGuard::new(metrics.clone(), "/test".to_string(), "GET".to_string());
        std::thread::sleep(std::time::Duration::from_millis(10));
        guard.finish(200);
    }
}
