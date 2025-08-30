//! HTTP handlers for KairosDB ingestion API
//!
//! This module provides HTTP handlers that are fully compatible with the Java KairosDB REST API.
//! It implements the `/api/v1/datapoints` endpoint and health check endpoints.

use axum::{
    body::Body,
    extract::{Query, Request, State},
    http::{header, HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Json, Response},
};
use flate2::read::GzDecoder;
use kairosdb_core::error::KairosError;
use serde_json::json;
use std::{io::Read, time::Instant};
use tracing::{error, info, trace, warn};

#[cfg(feature = "profiling")]
use pprof;

use crate::{
    config::PerformanceMode,
    ingestion::HealthStatus,
    json_parser::{ErrorResponse, IngestResponse, JsonParser},
    AppState,
};

/// Query parameters for ingestion requests
#[derive(serde::Deserialize)]
pub struct IngestParams {
    /// Override performance mode for this request
    #[serde(default)]
    perf_mode: Option<String>,
    /// Force sync to disk before returning success (default: false for performance)
    #[serde(default)]
    sync: Option<bool>,
}

/// Health check endpoint compatible with KairosDB format
pub async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let _timer = state.http_metrics.start_request_timer("health");

    // Check if shutdown manager indicates we should fail health checks
    if state.shutdown_manager.should_fail_health_checks() {
        info!("Health check failing due to shutdown in progress");
        let response = json!({
            "status": "unhealthy",
            "service": "kairosdb-ingest",
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "reason": "shutdown_in_progress",
            "checks": {
                "cassandra": "unknown",
                "memory": "unknown",
                "queue": "unknown"
            }
        });
        state
            .http_metrics
            .record_status_code(StatusCode::SERVICE_UNAVAILABLE);
        return (StatusCode::SERVICE_UNAVAILABLE, Json(response));
    }

    match state.ingestion_service.health_check().await {
        Ok(status) => {
            let response = match status {
                HealthStatus::Healthy => json!({
                    "status": "healthy",
                    "service": "kairosdb-ingest",
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "checks": {
                        "cassandra": "healthy",
                        "memory": "healthy",
                        "queue": "healthy"
                    }
                }),
                HealthStatus::Degraded => json!({
                    "status": "degraded",
                    "service": "kairosdb-ingest",
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "checks": {
                        "cassandra": "healthy",
                        "memory": "degraded",
                        "queue": "degraded"
                    }
                }),
                HealthStatus::Unhealthy => json!({
                    "status": "unhealthy",
                    "service": "kairosdb-ingest",
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "checks": {
                        "cassandra": "unhealthy",
                        "memory": "unknown",
                        "queue": "unknown"
                    }
                }),
            };

            let status_code = match status {
                HealthStatus::Healthy => StatusCode::OK,
                HealthStatus::Degraded => StatusCode::OK, // Still serving traffic
                HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
            };

            state.http_metrics.record_status_code(status_code);
            (status_code, Json(response))
        }
        Err(e) => {
            error!("Health check failed: {}", e);
            state
                .http_metrics
                .record_status_code(StatusCode::INTERNAL_SERVER_ERROR);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "service": "kairosdb-ingest",
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "error": e.to_string()
                })),
            )
        }
    }
}

/// Metrics endpoint (Prometheus format)
pub async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let _timer = state.http_metrics.start_request_timer("metrics");
    let start_time = Instant::now();

    // Use OpenTelemetry's Prometheus exporter
    match state.otel_metrics.prometheus_metrics() {
        Ok(metrics_string) => {
            let duration = start_time.elapsed().as_secs_f64();
            let response_size = metrics_string.len();

            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
            );
            state.http_metrics.record_status_code(StatusCode::OK);
            state
                .otel_metrics
                .record_http_request("metrics", "GET", 200, duration);
            state
                .otel_metrics
                .http_response_size
                .record(response_size as u64, &[]);
            (StatusCode::OK, headers, metrics_string)
        }
        Err(e) => {
            let duration = start_time.elapsed().as_secs_f64();
            let error_message = format!("Failed to encode metrics: {}", e);
            let response_size = error_message.len();

            error!("Failed to encode metrics: {}", e);
            state
                .http_metrics
                .record_status_code(StatusCode::INTERNAL_SERVER_ERROR);
            state
                .otel_metrics
                .record_http_request("metrics", "GET", 500, duration);
            state
                .otel_metrics
                .http_response_size
                .record(response_size as u64, &[]);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                error_message,
            )
        }
    }
}

/// Detailed metrics endpoint (JSON format)
pub async fn metrics_json_handler(State(state): State<AppState>) -> impl IntoResponse {
    let metrics = state.ingestion_service.get_metrics_snapshot();
    (StatusCode::OK, Json(metrics))
}

/// Main data ingestion endpoint - compatible with Java KairosDB
pub async fn ingest_handler(
    State(state): State<AppState>,
    Query(params): Query<IngestParams>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    // Start request timing and metrics
    let timer = state.http_metrics.start_request_timer("ingest");
    let otel_guard = crate::otel_metrics::HttpRequestGuard::new(
        state.otel_metrics.clone(),
        "/api/v1/datapoints".to_string(),
        "POST".to_string(),
    );
    let request_size = body.len();
    state
        .otel_metrics
        .http_request_size
        .record(request_size as u64, &[]);

    trace!("Received ingestion request, size: {} bytes", request_size);

    // Determine performance mode (query param overrides config)
    let perf_mode = if let Some(mode_str) = &params.perf_mode {
        match mode_str.to_lowercase().as_str() {
            "no_parse" => PerformanceMode::NoParseMode,
            "parse_only" => PerformanceMode::ParseOnlyMode,
            "parse_and_store" => PerformanceMode::ParseAndStoreMode,
            _ => {
                let error_response = ErrorResponse::from_error(format!(
                    "Invalid perf_mode: {}. Valid options: no_parse, parse_only, parse_and_store",
                    mode_str
                ));
                state
                    .http_metrics
                    .record_status_code(StatusCode::BAD_REQUEST);
                return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
            }
        }
    } else {
        state.config.ingestion.performance_mode.clone()
    };

    // Handle performance modes for testing
    if perf_mode == PerformanceMode::NoParseMode {
        // Skip all processing - just return success immediately
        let processing_time = timer.start_time.elapsed().as_millis() as u64;
        let response = IngestResponse {
            datapoints_ingested: 1, // Fake count since we didn't parse
            ingest_time: processing_time,
            warnings: vec!["Performance mode: no_parse - skipped all processing".to_string()],
        };
        let response_size = serde_json::to_string(&response).unwrap_or_default().len();
        state.http_metrics.record_sizes(request_size, response_size);
        state.http_metrics.record_status_code(StatusCode::OK);
        return (StatusCode::OK, Json(response)).into_response();
    }

    // Handle gzipped content if present
    let json_str = if headers
        .get("content-encoding")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.contains("gzip"))
        .unwrap_or(false)
    {
        match decompress_gzip(&body) {
            Ok(decompressed) => decompressed,
            Err(e) => {
                warn!("Failed to decompress gzip content: {}", e);
                let error_response = ErrorResponse::from_error("Failed to decompress gzip content");
                state
                    .http_metrics
                    .record_status_code(StatusCode::BAD_REQUEST);
                return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
            }
        }
    } else {
        body
    };

    // Create JSON parser with configuration
    let parser = JsonParser::new(
        state.config.ingestion.max_batch_size,
        state.config.ingestion.enable_validation,
    );

    // Parse JSON into data points
    let parse_start = Instant::now();
    let (batch, warnings) = match parser.parse_json(&json_str) {
        Ok(result) => {
            timer.record_parse_time(parse_start.elapsed());
            result
        }
        Err(e) => {
            timer.record_parse_error();
            warn!("Failed to parse JSON payload: {}", e);
            let error_response = ErrorResponse::from_kairos_error(&e);
            state
                .http_metrics
                .record_status_code(StatusCode::BAD_REQUEST);
            return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
        }
    };

    let batch_size = batch.len();
    trace!("Parsed {} data points", batch_size);

    // Handle parse_only mode - skip Cassandra storage
    if perf_mode == PerformanceMode::ParseOnlyMode {
        let processing_time = timer.start_time.elapsed().as_millis() as u64;
        trace!(
            "Parse-only mode: Successfully parsed {} data points in {}ms",
            batch_size,
            processing_time
        );

        let mut response_warnings = warnings;
        response_warnings
            .push("Performance mode: parse_only - skipped Cassandra storage".to_string());

        let response = IngestResponse {
            datapoints_ingested: batch_size,
            ingest_time: processing_time,
            warnings: response_warnings,
        };

        let response_size = serde_json::to_string(&response).unwrap_or_default().len();
        state.http_metrics.record_sizes(request_size, response_size);
        state.http_metrics.record_status_code(StatusCode::OK);
        return (StatusCode::OK, Json(response)).into_response();
    }

    // Submit batch for ingestion (normal mode)
    let queue_start = Instant::now();
    let should_sync = params.sync.unwrap_or(state.config.ingestion.default_sync);
    match state
        .ingestion_service
        .ingest_batch_with_sync(batch, should_sync)
    {
        Ok(_) => {
            timer.record_queue_write_time(queue_start.elapsed());
            state.http_metrics.requests_by_endpoint.ingest_success.inc();

            let processing_time = queue_start.elapsed().as_millis() as u64;
            trace!(
                "Successfully ingested {} data points in {}ms",
                batch_size,
                processing_time
            );

            let response = IngestResponse {
                datapoints_ingested: batch_size,
                ingest_time: processing_time,
                warnings,
            };

            let response_json = Json(response);
            let response_size = serde_json::to_string(&response_json.0)
                .unwrap_or_default()
                .len();
            state.http_metrics.record_sizes(request_size, response_size);
            state.http_metrics.record_status_code(StatusCode::OK);

            // Record OpenTelemetry metrics
            state
                .otel_metrics
                .record_datapoints(batch_size as u64, "http");
            // Convert processing_time from milliseconds to seconds for OpenTelemetry
            state
                .otel_metrics
                .record_batch(batch_size as u64, queue_start.elapsed().as_secs_f64());
            state
                .otel_metrics
                .http_response_size
                .record(response_size as u64, &[]);
            otel_guard.finish(200);

            (StatusCode::OK, response_json).into_response()
        }
        Err(e) => {
            timer.record_queue_error();
            state.http_metrics.requests_by_endpoint.ingest_errors.inc();
            error!("Failed to ingest batch: {}", e);

            let status_code = match &e {
                KairosError::RateLimit { .. } => StatusCode::TOO_MANY_REQUESTS,
                KairosError::Validation(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };

            let error_response = ErrorResponse::from_kairos_error(&e);
            let response_size = serde_json::to_string(&error_response)
                .unwrap_or_default()
                .len();
            state.http_metrics.record_sizes(request_size, response_size);
            state.http_metrics.record_status_code(status_code);

            // Record OpenTelemetry error metrics
            state.otel_metrics.errors_counter.add(1, &[]);
            state
                .otel_metrics
                .http_response_size
                .record(response_size as u64, &[]);
            otel_guard.finish(status_code.as_u16());

            (status_code, Json(error_response)).into_response()
        }
    }
}

/// Handle gzipped ingestion requests
pub async fn ingest_gzip_handler(
    State(state): State<AppState>,
    Query(params): Query<IngestParams>,
    body: Body,
) -> impl IntoResponse {
    let timer = state.http_metrics.start_request_timer("ingest_gzip");

    // Determine performance mode (query param overrides config)
    let perf_mode = if let Some(mode_str) = &params.perf_mode {
        match mode_str.to_lowercase().as_str() {
            "no_parse" => PerformanceMode::NoParseMode,
            "parse_only" => PerformanceMode::ParseOnlyMode,
            "parse_and_store" => PerformanceMode::ParseAndStoreMode,
            _ => {
                let error_response = ErrorResponse::from_error(format!(
                    "Invalid perf_mode: {}. Valid options: no_parse, parse_only, parse_and_store",
                    mode_str
                ));
                state
                    .http_metrics
                    .record_status_code(StatusCode::BAD_REQUEST);
                return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
            }
        }
    } else {
        state.config.ingestion.performance_mode.clone()
    };

    // Handle performance modes for testing
    if perf_mode == PerformanceMode::NoParseMode {
        // Skip all processing - just return success immediately
        let processing_time = timer.start_time.elapsed().as_millis() as u64;
        let response = IngestResponse {
            datapoints_ingested: 1, // Fake count since we didn't parse
            ingest_time: processing_time,
            warnings: vec!["Performance mode: no_parse - skipped all processing (gzip)".to_string()],
        };
        let response_size = serde_json::to_string(&response).unwrap_or_default().len();
        // For no_parse mode, we don't have request_size yet, so use 0 or skip size recording
        state.http_metrics.record_sizes(0, response_size);
        state.http_metrics.record_status_code(StatusCode::OK);
        return (StatusCode::OK, Json(response)).into_response();
    }

    // Read the gzipped body
    let body_bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!("Failed to read request body: {}", e);
            let error_response = ErrorResponse::from_error("Failed to read request body");
            state
                .http_metrics
                .record_status_code(StatusCode::BAD_REQUEST);
            return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
        }
    };

    let request_size = body_bytes.len();

    // Decompress the body
    let json_str = match decompress_gzip_bytes(&body_bytes) {
        Ok(decompressed) => decompressed,
        Err(e) => {
            warn!("Failed to decompress gzip content: {}", e);
            let error_response = ErrorResponse::from_error("Failed to decompress gzip content");
            state
                .http_metrics
                .record_status_code(StatusCode::BAD_REQUEST);
            return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
        }
    };

    trace!(
        "Decompressed {} bytes to {} bytes",
        body_bytes.len(),
        json_str.len()
    );

    // Create JSON parser
    let parser = JsonParser::new(
        state.config.ingestion.max_batch_size,
        state.config.ingestion.enable_validation,
    );

    // Parse and process the same way as regular ingest handler
    let parse_start = Instant::now();
    let (batch, warnings) = match parser.parse_json(&json_str) {
        Ok(result) => {
            timer.record_parse_time(parse_start.elapsed());
            result
        }
        Err(e) => {
            timer.record_parse_error();
            warn!("Failed to parse JSON payload: {}", e);
            let error_response = ErrorResponse::from_kairos_error(&e);
            state
                .http_metrics
                .record_status_code(StatusCode::BAD_REQUEST);
            return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
        }
    };

    let batch_size = batch.len();

    // Handle parse_only mode - skip Cassandra storage
    if perf_mode == PerformanceMode::ParseOnlyMode {
        let processing_time = timer.start_time.elapsed().as_millis() as u64;
        trace!(
            "Parse-only mode: Successfully parsed {} data points from gzipped request in {}ms",
            batch_size,
            processing_time
        );

        let mut response_warnings = warnings;
        response_warnings
            .push("Performance mode: parse_only - skipped Cassandra storage (gzip)".to_string());

        let response = IngestResponse {
            datapoints_ingested: batch_size,
            ingest_time: processing_time,
            warnings: response_warnings,
        };

        let response_size = serde_json::to_string(&response).unwrap_or_default().len();
        state.http_metrics.record_sizes(request_size, response_size);
        state.http_metrics.record_status_code(StatusCode::OK);
        return (StatusCode::OK, Json(response)).into_response();
    }

    let queue_start = Instant::now();
    let should_sync = params.sync.unwrap_or(state.config.ingestion.default_sync);
    match state
        .ingestion_service
        .ingest_batch_with_sync(batch, should_sync)
    {
        Ok(_) => {
            timer.record_queue_write_time(queue_start.elapsed());
            state.http_metrics.requests_by_endpoint.ingest_success.inc();

            let processing_time = queue_start.elapsed().as_millis() as u64;
            trace!(
                "Successfully ingested {} data points from gzipped request in {}ms",
                batch_size,
                processing_time
            );

            let response = IngestResponse {
                datapoints_ingested: batch_size,
                ingest_time: processing_time,
                warnings,
            };

            let response_json = Json(response);
            let response_size = serde_json::to_string(&response_json.0)
                .unwrap_or_default()
                .len();
            state.http_metrics.record_sizes(request_size, response_size);
            state.http_metrics.record_status_code(StatusCode::OK);

            (StatusCode::OK, response_json).into_response()
        }
        Err(e) => {
            timer.record_queue_error();
            state.http_metrics.requests_by_endpoint.ingest_errors.inc();
            error!("Failed to ingest gzipped batch: {}", e);

            let status_code = match &e {
                KairosError::RateLimit { .. } => StatusCode::TOO_MANY_REQUESTS,
                KairosError::Validation(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };

            let error_response = ErrorResponse::from_kairos_error(&e);
            let response_size = serde_json::to_string(&error_response)
                .unwrap_or_default()
                .len();
            state.http_metrics.record_sizes(request_size, response_size);
            state.http_metrics.record_status_code(status_code);
            (status_code, Json(error_response)).into_response()
        }
    }
}

/// Decompress gzip string content
fn decompress_gzip(content: &str) -> Result<String, std::io::Error> {
    let bytes = content.as_bytes();
    decompress_gzip_bytes(bytes)
}

/// Decompress gzip byte content
fn decompress_gzip_bytes(bytes: &[u8]) -> Result<String, std::io::Error> {
    let mut decoder = GzDecoder::new(bytes);
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed)?;
    Ok(decompressed)
}

/// CORS preflight handler for datapoints endpoint
pub async fn cors_preflight_datapoints() -> impl IntoResponse {
    let mut headers = HeaderMap::new();
    headers.insert("Access-Control-Allow-Origin", "*".parse().unwrap());
    headers.insert(
        "Access-Control-Allow-Methods",
        "POST, GET, OPTIONS".parse().unwrap(),
    );
    headers.insert(
        "Access-Control-Allow-Headers",
        "Content-Type, Content-Encoding".parse().unwrap(),
    );
    headers.insert("Access-Control-Max-Age", "86400".parse().unwrap());

    (StatusCode::OK, headers)
}

/// Request timing middleware
pub async fn timing_middleware(request: Request, next: Next) -> Response {
    let start = Instant::now();
    let method = request.method().clone();
    let uri = request.uri().clone();

    let response = next.run(request).await;

    let duration = start.elapsed();
    trace!("{} {} - {}ms", method, uri, duration.as_millis());

    response
}

/// Request size limiting middleware  
pub async fn request_size_middleware(headers: HeaderMap, request: Request, next: Next) -> Response {
    const MAX_REQUEST_SIZE: usize = 100 * 1024 * 1024; // 100MB

    if let Some(content_length) = headers.get("content-length") {
        if let Ok(length_str) = content_length.to_str() {
            if let Ok(length) = length_str.parse::<usize>() {
                if length > MAX_REQUEST_SIZE {
                    let error_response = ErrorResponse::from_error("Request too large");
                    return (StatusCode::PAYLOAD_TOO_LARGE, Json(error_response)).into_response();
                }
            }
        }
    }

    next.run(request).await
}

/// Connection tracking middleware - tracks active HTTP connections for graceful shutdown
pub async fn connection_tracking_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    // Increment connection count when request starts
    state.shutdown_manager.connection_tracker().increment();

    // Process the request
    let response = next.run(request).await;

    // Decrement connection count when request completes
    state.shutdown_manager.connection_tracker().decrement();

    response
}

/// Connection draining middleware - rejects new requests during graceful shutdown
pub async fn connection_draining_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    // Check if we should reject new requests due to connection draining
    if state.shutdown_manager.should_drain_connections() {
        warn!("Rejecting new request due to connection draining in progress");
        let error_response =
            ErrorResponse::from_error("Service is shutting down, please retry on another instance");
        let mut response = (StatusCode::SERVICE_UNAVAILABLE, Json(error_response)).into_response();

        // Set Connection: close header to signal client to close connection
        response
            .headers_mut()
            .insert("Connection", "close".parse().unwrap());
        return response;
    }

    // Process the request normally
    let mut response = next.run(request).await;

    // If we're in draining phase, add Connection: close header to all responses
    // to encourage clients to close connections after this response
    if state.shutdown_manager.should_drain_connections() {
        response
            .headers_mut()
            .insert("Connection", "close".parse().unwrap());
    }

    response
}

/// CPU profiling endpoint - generates flame graph
#[cfg(feature = "profiling")]
pub async fn profile_handler(Query(params): Query<ProfileParams>) -> impl IntoResponse {
    let duration = params.duration.unwrap_or(30); // Default 30 seconds
    let frequency = params.frequency.unwrap_or(99); // Default 99Hz sampling

    info!(
        "Starting CPU profiling for {} seconds at {}Hz",
        duration, frequency
    );

    // Start profiling
    let guard = match pprof::ProfilerGuardBuilder::default()
        .frequency(frequency)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
    {
        Ok(guard) => guard,
        Err(e) => {
            error!("Failed to start profiler: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Profiler error: {}", e),
            )
                .into_response();
        }
    };

    // Wait for the specified duration
    tokio::time::sleep(tokio::time::Duration::from_secs(duration)).await;

    // Stop profiling and generate report
    match guard.report().build() {
        Ok(report) => {
            info!("Profiling completed, generating flame graph");

            // Generate flame graph
            let mut flame_graph = Vec::new();
            if let Err(e) = report.flamegraph(&mut flame_graph) {
                error!("Failed to generate flame graph: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Flame graph error: {}", e),
                )
                    .into_response();
            }

            // Return flame graph as SVG
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "image/svg+xml")
                .header(
                    "Content-Disposition",
                    "attachment; filename=\"profile.svg\"",
                )
                .body(Body::from(flame_graph))
                .unwrap()
                .into_response()
        }
        Err(e) => {
            error!("Failed to build profiling report: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Report error: {}", e),
            )
                .into_response()
        }
    }
}

/// Profiling endpoint parameters
#[cfg(feature = "profiling")]
#[derive(serde::Deserialize)]
pub struct ProfileParams {
    /// Duration to profile in seconds (default: 30)
    duration: Option<u64>,
    /// Sampling frequency in Hz (default: 99)
    frequency: Option<i32>,
}

/// Stub profiling handler when profiling feature is disabled
#[cfg(not(feature = "profiling"))]
pub async fn profile_handler() -> impl IntoResponse {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        "Profiling not enabled. Rebuild with --features profiling",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gzip_decompression() {
        // This would test gzip decompression with real data
        // For now, just verify the function exists
        let result = decompress_gzip("invalid");
        assert!(result.is_err()); // Should fail on invalid gzip data
    }

    #[tokio::test]
    async fn test_cors_preflight() {
        let response = cors_preflight_datapoints().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
