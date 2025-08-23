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
use prometheus::TextEncoder;
use serde_json::json;
use std::{io::Read, time::Instant};
use tracing::{debug, error, info, trace, warn};

use crate::{
    config::PerformanceMode,
    ingestion::HealthStatus,
    json_parser::{ErrorResponse, IngestResponse, JsonParser},
    AppState,
};

/// Query parameters for performance testing
#[derive(serde::Deserialize)]
pub struct PerfParams {
    /// Override performance mode for this request
    #[serde(default)]
    perf_mode: Option<String>,
}

/// Health check endpoint compatible with KairosDB format
pub async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
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

            (status_code, Json(response))
        }
        Err(e) => {
            error!("Health check failed: {}", e);
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
pub async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();

    match encoder.encode_to_string(&metric_families) {
        Ok(metrics_string) => {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
            );
            (StatusCode::OK, headers, metrics_string)
        }
        Err(e) => {
            error!("Failed to encode metrics: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                HeaderMap::new(),
                format!("Failed to encode metrics: {}", e),
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
    Query(params): Query<PerfParams>,
    headers: HeaderMap,
    body: String,
) -> impl IntoResponse {
    let start_time = Instant::now();
    trace!("Received ingestion request, size: {} bytes", body.len());

    // Determine performance mode (query param overrides config)
    let perf_mode = if let Some(mode_str) = &params.perf_mode {
        match mode_str.to_lowercase().as_str() {
            "no_parse" => PerformanceMode::NoParseMode,
            "parse_only" => PerformanceMode::ParseOnlyMode,
            "parse_and_store" => PerformanceMode::ParseAndStoreMode,
            _ => {
                let error_response = ErrorResponse::from_error(&format!("Invalid perf_mode: {}. Valid options: no_parse, parse_only, parse_and_store", mode_str));
                return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
            }
        }
    } else {
        state.config.ingestion.performance_mode.clone()
    };

    // Handle performance modes for testing
    match perf_mode {
        PerformanceMode::NoParseMode => {
            // Skip all processing - just return success immediately
            let processing_time = start_time.elapsed().as_millis() as u64;
            let response = IngestResponse {
                datapoints_ingested: 1, // Fake count since we didn't parse
                ingest_time: processing_time,
                warnings: vec!["Performance mode: no_parse - skipped all processing".to_string()],
            };
            return (StatusCode::OK, Json(response)).into_response();
        }
        _ => {} // Continue with normal processing for other modes
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
    let (batch, warnings) = match parser.parse_json(&json_str) {
        Ok(result) => result,
        Err(e) => {
            warn!("Failed to parse JSON payload: {}", e);
            let error_response = ErrorResponse::from_kairos_error(&e);
            return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
        }
    };

    let batch_size = batch.len();
    trace!("Parsed {} data points", batch_size);

    // Handle parse_only mode - skip Cassandra storage
    if perf_mode == PerformanceMode::ParseOnlyMode {
        let processing_time = start_time.elapsed().as_millis() as u64;
        trace!(
            "Parse-only mode: Successfully parsed {} data points in {}ms",
            batch_size, processing_time
        );

        let mut response_warnings = warnings;
        response_warnings.push("Performance mode: parse_only - skipped Cassandra storage".to_string());

        let response = IngestResponse {
            datapoints_ingested: batch_size,
            ingest_time: processing_time,
            warnings: response_warnings,
        };

        return (StatusCode::OK, Json(response)).into_response();
    }

    // Submit batch for ingestion (normal mode)
    match state.ingestion_service.ingest_batch(batch).await {
        Ok(_) => {
            let processing_time = start_time.elapsed().as_millis() as u64;
            trace!(
                "Successfully ingested {} data points in {}ms",
                batch_size, processing_time
            );

            let response = IngestResponse {
                datapoints_ingested: batch_size,
                ingest_time: processing_time,
                warnings,
            };

            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            error!("Failed to ingest batch: {}", e);

            let status_code = match &e {
                KairosError::RateLimit { .. } => StatusCode::TOO_MANY_REQUESTS,
                KairosError::Validation(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };

            let error_response = ErrorResponse::from_kairos_error(&e);
            (status_code, Json(error_response)).into_response()
        }
    }
}

/// Handle gzipped ingestion requests
pub async fn ingest_gzip_handler(State(state): State<AppState>, Query(params): Query<PerfParams>, body: Body) -> impl IntoResponse {
    let start_time = Instant::now();

    // Determine performance mode (query param overrides config)
    let perf_mode = if let Some(mode_str) = &params.perf_mode {
        match mode_str.to_lowercase().as_str() {
            "no_parse" => PerformanceMode::NoParseMode,
            "parse_only" => PerformanceMode::ParseOnlyMode,
            "parse_and_store" => PerformanceMode::ParseAndStoreMode,
            _ => {
                let error_response = ErrorResponse::from_error(&format!("Invalid perf_mode: {}. Valid options: no_parse, parse_only, parse_and_store", mode_str));
                return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
            }
        }
    } else {
        state.config.ingestion.performance_mode.clone()
    };

    // Handle performance modes for testing
    match perf_mode {
        PerformanceMode::NoParseMode => {
            // Skip all processing - just return success immediately
            let processing_time = start_time.elapsed().as_millis() as u64;
            let response = IngestResponse {
                datapoints_ingested: 1, // Fake count since we didn't parse
                ingest_time: processing_time,
                warnings: vec!["Performance mode: no_parse - skipped all processing (gzip)".to_string()],
            };
            return (StatusCode::OK, Json(response)).into_response();
        }
        _ => {} // Continue with normal processing for other modes
    }

    // Read the gzipped body
    let body_bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!("Failed to read request body: {}", e);
            let error_response = ErrorResponse::from_error("Failed to read request body");
            return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
        }
    };

    // Decompress the body
    let json_str = match decompress_gzip_bytes(&body_bytes) {
        Ok(decompressed) => decompressed,
        Err(e) => {
            warn!("Failed to decompress gzip content: {}", e);
            let error_response = ErrorResponse::from_error("Failed to decompress gzip content");
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
    let (batch, warnings) = match parser.parse_json(&json_str) {
        Ok(result) => result,
        Err(e) => {
            warn!("Failed to parse JSON payload: {}", e);
            let error_response = ErrorResponse::from_kairos_error(&e);
            return (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
        }
    };

    let batch_size = batch.len();

    // Handle parse_only mode - skip Cassandra storage
    if perf_mode == PerformanceMode::ParseOnlyMode {
        let processing_time = start_time.elapsed().as_millis() as u64;
        trace!(
            "Parse-only mode: Successfully parsed {} data points from gzipped request in {}ms",
            batch_size, processing_time
        );

        let mut response_warnings = warnings;
        response_warnings.push("Performance mode: parse_only - skipped Cassandra storage (gzip)".to_string());

        let response = IngestResponse {
            datapoints_ingested: batch_size,
            ingest_time: processing_time,
            warnings: response_warnings,
        };

        return (StatusCode::OK, Json(response)).into_response();
    }

    match state.ingestion_service.ingest_batch(batch).await {
        Ok(_) => {
            let processing_time = start_time.elapsed().as_millis() as u64;
            trace!(
                "Successfully ingested {} data points from gzipped request in {}ms",
                batch_size, processing_time
            );

            let response = IngestResponse {
                datapoints_ingested: batch_size,
                ingest_time: processing_time,
                warnings,
            };

            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            error!("Failed to ingest gzipped batch: {}", e);

            let status_code = match &e {
                KairosError::RateLimit { .. } => StatusCode::TOO_MANY_REQUESTS,
                KairosError::Validation(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };

            let error_response = ErrorResponse::from_kairos_error(&e);
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
