//! HTTP Handler Metrics
//!
//! Comprehensive metrics for all HTTP operations including request counts,
//! response times, error rates, and detailed breakdowns by endpoint.

use prometheus::{register_counter, register_histogram, register_gauge, Counter, Histogram, Gauge};
use std::time::Instant;
use axum::http::StatusCode;

/// Comprehensive HTTP metrics for all endpoints
#[derive(Debug, Clone)]
pub struct HttpMetrics {
    // Request counters by endpoint and status
    pub requests_total: Counter,
    pub requests_by_endpoint: HttpEndpointMetrics,
    
    // Response time histograms
    pub request_duration: Histogram,
    pub parse_duration: Histogram,
    pub validation_duration: Histogram,
    pub queue_write_duration: Histogram,
    
    // Size metrics
    pub request_size_bytes: Histogram,
    pub response_size_bytes: Histogram,
    
    // Error counters
    pub parse_errors: Counter,
    pub validation_errors: Counter,
    pub queue_errors: Counter,
    pub http_4xx_errors: Counter,
    pub http_5xx_errors: Counter,
    
    // Current active requests
    pub active_requests: Gauge,
}

/// Metrics for specific HTTP endpoints
#[derive(Debug, Clone)]
pub struct HttpEndpointMetrics {
    // Ingestion endpoints
    pub ingest_requests: Counter,
    pub ingest_gzip_requests: Counter,
    pub ingest_success: Counter,
    pub ingest_errors: Counter,
    
    // Monitoring endpoints
    pub health_requests: Counter,
    pub metrics_requests: Counter,
    pub profile_requests: Counter,
    
    // Response time by endpoint
    pub ingest_duration: Histogram,
    pub health_duration: Histogram,
    pub metrics_duration: Histogram,
}

/// Request timing helper for detailed metrics
pub struct RequestTimer {
    pub start_time: Instant,
    endpoint: &'static str,
    metrics: HttpMetrics,
}

impl HttpMetrics {
    pub fn new() -> anyhow::Result<Self> {
        Self::new_with_prefix("")
    }
    
    pub fn new_with_prefix(prefix: &str) -> anyhow::Result<Self> {
        let suffix = if prefix.is_empty() {
            String::new()
        } else {
            format!("_{}", prefix)
        };

        // Global request metrics
        let requests_total = register_counter!(
            format!("kairosdb_http_requests_total{}", suffix),
            "Total number of HTTP requests received"
        ).unwrap_or_else(|_| Counter::new("test_counter", "test").unwrap());

        let request_duration = register_histogram!(
            format!("kairosdb_http_request_duration_seconds{}", suffix),
            "HTTP request processing duration"
        ).unwrap_or_else(|_| {
            prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
                "test_histogram", "test"
            )).unwrap()
        });

        let parse_duration = register_histogram!(
            format!("kairosdb_http_parse_duration_seconds{}", suffix),
            "Time spent parsing request body"
        ).unwrap_or_else(|_| {
            prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
                "test_histogram2", "test"
            )).unwrap()
        });

        let validation_duration = register_histogram!(
            format!("kairosdb_http_validation_duration_seconds{}", suffix),
            "Time spent validating parsed data"
        ).unwrap_or_else(|_| {
            prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
                "test_histogram3", "test"
            )).unwrap()
        });

        let queue_write_duration = register_histogram!(
            format!("kairosdb_http_queue_write_duration_seconds{}", suffix),
            "Time spent writing to persistent queue"
        ).unwrap_or_else(|_| {
            prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
                "test_histogram4", "test"
            )).unwrap()
        });

        let request_size_bytes = register_histogram!(
            format!("kairosdb_http_request_size_bytes{}", suffix),
            "Size of HTTP request bodies"
        ).unwrap_or_else(|_| {
            prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
                "test_histogram5", "test"
            )).unwrap()
        });

        let response_size_bytes = register_histogram!(
            format!("kairosdb_http_response_size_bytes{}", suffix),
            "Size of HTTP response bodies"
        ).unwrap_or_else(|_| {
            prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
                "test_histogram6", "test"
            )).unwrap()
        });

        // Error counters
        let parse_errors = register_counter!(
            format!("kairosdb_http_parse_errors_total{}", suffix),
            "Total number of request parsing errors"
        ).unwrap_or_else(|_| Counter::new("test_counter2", "test").unwrap());

        let validation_errors = register_counter!(
            format!("kairosdb_http_validation_errors_total{}", suffix),
            "Total number of validation errors"
        ).unwrap_or_else(|_| Counter::new("test_counter3", "test").unwrap());

        let queue_errors = register_counter!(
            format!("kairosdb_http_queue_errors_total{}", suffix),
            "Total number of queue write errors"
        ).unwrap_or_else(|_| Counter::new("test_counter4", "test").unwrap());

        let http_4xx_errors = register_counter!(
            format!("kairosdb_http_4xx_errors_total{}", suffix),
            "Total number of 4xx HTTP errors"
        ).unwrap_or_else(|_| Counter::new("test_counter5", "test").unwrap());

        let http_5xx_errors = register_counter!(
            format!("kairosdb_http_5xx_errors_total{}", suffix),
            "Total number of 5xx HTTP errors"
        ).unwrap_or_else(|_| Counter::new("test_counter6", "test").unwrap());

        let active_requests = register_gauge!(
            format!("kairosdb_http_active_requests{}", suffix),
            "Number of currently active HTTP requests"
        ).unwrap_or_else(|_| Gauge::new("test_gauge", "test").unwrap());

        // Endpoint-specific metrics
        let requests_by_endpoint = HttpEndpointMetrics::new_with_prefix(prefix)?;

        Ok(Self {
            requests_total,
            requests_by_endpoint,
            request_duration,
            parse_duration,
            validation_duration,
            queue_write_duration,
            request_size_bytes,
            response_size_bytes,
            parse_errors,
            validation_errors,
            queue_errors,
            http_4xx_errors,
            http_5xx_errors,
            active_requests,
        })
    }

    /// Start timing a request
    pub fn start_request_timer(&self, endpoint: &'static str) -> RequestTimer {
        self.active_requests.inc();
        self.requests_total.inc();
        
        RequestTimer {
            start_time: Instant::now(),
            endpoint,
            metrics: self.clone(),
        }
    }

    /// Record HTTP status code metrics
    pub fn record_status_code(&self, status: StatusCode) {
        match status.as_u16() {
            400..=499 => self.http_4xx_errors.inc(),
            500..=599 => self.http_5xx_errors.inc(),
            _ => {}
        }
    }

    /// Record request and response sizes
    pub fn record_sizes(&self, request_size: usize, response_size: usize) {
        self.request_size_bytes.observe(request_size as f64);
        self.response_size_bytes.observe(response_size as f64);
    }
}

impl HttpEndpointMetrics {
    pub fn new_with_prefix(prefix: &str) -> anyhow::Result<Self> {
        let suffix = if prefix.is_empty() {
            String::new()
        } else {
            format!("_{}", prefix)
        };

        let ingest_requests = register_counter!(
            format!("kairosdb_http_ingest_requests_total{}", suffix),
            "Total ingest endpoint requests"
        ).unwrap_or_else(|_| Counter::new("test_counter7", "test").unwrap());

        let ingest_gzip_requests = register_counter!(
            format!("kairosdb_http_ingest_gzip_requests_total{}", suffix),
            "Total gzip ingest endpoint requests"
        ).unwrap_or_else(|_| Counter::new("test_counter8", "test").unwrap());

        let ingest_success = register_counter!(
            format!("kairosdb_http_ingest_success_total{}", suffix),
            "Total successful ingest requests"
        ).unwrap_or_else(|_| Counter::new("test_counter9", "test").unwrap());

        let ingest_errors = register_counter!(
            format!("kairosdb_http_ingest_errors_total{}", suffix),
            "Total failed ingest requests"
        ).unwrap_or_else(|_| Counter::new("test_counter10", "test").unwrap());

        let health_requests = register_counter!(
            format!("kairosdb_http_health_requests_total{}", suffix),
            "Total health check requests"
        ).unwrap_or_else(|_| Counter::new("test_counter11", "test").unwrap());

        let metrics_requests = register_counter!(
            format!("kairosdb_http_metrics_requests_total{}", suffix),
            "Total metrics endpoint requests"
        ).unwrap_or_else(|_| Counter::new("test_counter12", "test").unwrap());

        let profile_requests = register_counter!(
            format!("kairosdb_http_profile_requests_total{}", suffix),
            "Total profile endpoint requests"
        ).unwrap_or_else(|_| Counter::new("test_counter13", "test").unwrap());

        let ingest_duration = register_histogram!(
            format!("kairosdb_http_ingest_duration_seconds{}", suffix),
            "Ingest endpoint processing duration"
        ).unwrap_or_else(|_| {
            prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
                "test_histogram7", "test"
            )).unwrap()
        });

        let health_duration = register_histogram!(
            format!("kairosdb_http_health_duration_seconds{}", suffix),
            "Health endpoint processing duration"
        ).unwrap_or_else(|_| {
            prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
                "test_histogram8", "test"
            )).unwrap()
        });

        let metrics_duration = register_histogram!(
            format!("kairosdb_http_metrics_duration_seconds{}", suffix),
            "Metrics endpoint processing duration"
        ).unwrap_or_else(|_| {
            prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
                "test_histogram9", "test"
            )).unwrap()
        });

        Ok(Self {
            ingest_requests,
            ingest_gzip_requests,
            ingest_success,
            ingest_errors,
            health_requests,
            metrics_requests,
            profile_requests,
            ingest_duration,
            health_duration,
            metrics_duration,
        })
    }
}

impl RequestTimer {
    /// Record parse timing
    pub fn record_parse_time(&self, duration: std::time::Duration) {
        self.metrics.parse_duration.observe(duration.as_secs_f64());
    }

    /// Record validation timing
    pub fn record_validation_time(&self, duration: std::time::Duration) {
        self.metrics.validation_duration.observe(duration.as_secs_f64());
    }

    /// Record queue write timing
    pub fn record_queue_write_time(&self, duration: std::time::Duration) {
        self.metrics.queue_write_duration.observe(duration.as_secs_f64());
    }

    /// Record parse error
    pub fn record_parse_error(&self) {
        self.metrics.parse_errors.inc();
    }

    /// Record validation error
    pub fn record_validation_error(&self) {
        self.metrics.validation_errors.inc();
    }

    /// Record queue error
    pub fn record_queue_error(&self) {
        self.metrics.queue_errors.inc();
    }
}

impl Drop for RequestTimer {
    fn drop(&mut self) {
        // Record total request duration and endpoint-specific duration
        let total_duration = self.start_time.elapsed();
        self.metrics.request_duration.observe(total_duration.as_secs_f64());
        self.metrics.active_requests.dec();

        // Record endpoint-specific metrics
        match self.endpoint {
            "ingest" => {
                self.metrics.requests_by_endpoint.ingest_requests.inc();
                self.metrics.requests_by_endpoint.ingest_duration.observe(total_duration.as_secs_f64());
            }
            "ingest_gzip" => {
                self.metrics.requests_by_endpoint.ingest_gzip_requests.inc();
                self.metrics.requests_by_endpoint.ingest_duration.observe(total_duration.as_secs_f64());
            }
            "health" => {
                self.metrics.requests_by_endpoint.health_requests.inc();
                self.metrics.requests_by_endpoint.health_duration.observe(total_duration.as_secs_f64());
            }
            "metrics" => {
                self.metrics.requests_by_endpoint.metrics_requests.inc();
                self.metrics.requests_by_endpoint.metrics_duration.observe(total_duration.as_secs_f64());
            }
            "profile" => {
                self.metrics.requests_by_endpoint.profile_requests.inc();
            }
            _ => {}
        }
    }
}