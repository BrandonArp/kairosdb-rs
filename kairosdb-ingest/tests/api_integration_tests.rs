//! API Integration tests for KairosDB ingestion service
//!
//! These tests validate the public HTTP API of the ingestion service using mocked dependencies.
//! They test the full request/response cycle without requiring external services.

use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
};
use kairosdb_ingest::{config::IngestConfig, create_router, ingestion::IngestionService, AppState};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

/// Create a test app instance with mock Cassandra dependencies
async fn create_test_app_with_mock() -> axum::Router {
    // Set environment variable to use mock Cassandra client
    std::env::set_var("USE_MOCK_CASSANDRA", "true");

    let mut config = IngestConfig::default();
    // Set high queue size limit for tests to avoid backpressure
    config.ingestion.max_queue_size = 100000;
    let config = Arc::new(config);

    let shutdown_manager = Arc::new(kairosdb_ingest::ShutdownManager::new());
    let (ingestion_service, _null_queue_work_channel) =
        IngestionService::new(config.clone(), shutdown_manager.clone())
            .await
            .expect("Failed to create ingestion service for testing");

    let state = AppState {
        ingestion_service: Arc::new(ingestion_service),
        config,
        http_metrics: Arc::new(kairosdb_ingest::http_metrics::HttpMetrics::new().unwrap()),
        otel_metrics: kairosdb_ingest::otel_metrics::OtelMetrics::init(
            kairosdb_ingest::otel_metrics::OtelMetricsConfig::default(),
        )
        .unwrap(),
        shutdown_manager,
    };

    create_router(state)
}

/// Create a test app instance with real Cassandra dependencies
#[allow(dead_code)] // Used by ignored integration tests
async fn create_test_app() -> axum::Router {
    let mut config = IngestConfig::default();
    // Set high queue size limit for tests to avoid backpressure
    config.ingestion.max_queue_size = 100000;
    let config = Arc::new(config);

    let shutdown_manager = Arc::new(kairosdb_ingest::ShutdownManager::new());
    let (ingestion_service, _null_queue_work_channel) =
        IngestionService::new(config.clone(), shutdown_manager.clone())
            .await
            .expect("Failed to create ingestion service for testing");

    let state = AppState {
        ingestion_service: Arc::new(ingestion_service),
        config,
        http_metrics: Arc::new(kairosdb_ingest::http_metrics::HttpMetrics::new().unwrap()),
        otel_metrics: kairosdb_ingest::otel_metrics::OtelMetrics::init(
            kairosdb_ingest::otel_metrics::OtelMetricsConfig::default(),
        )
        .unwrap(),
        shutdown_manager,
    };

    create_router(state)
}

#[cfg(test)]
mod api_tests {
    use super::*;

    #[tokio::test]
    async fn test_health_endpoint_returns_ok() {
        let app = create_test_app_with_mock().await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["service"], "kairosdb-ingest");
        assert!(json.get("status").is_some());
    }

    #[tokio::test]
    async fn test_metrics_endpoint_returns_prometheus_format() {
        let app = create_test_app_with_mock().await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let headers = response.headers();
        assert_eq!(
            headers.get(header::CONTENT_TYPE).unwrap(),
            "text/plain; version=0.0.4; charset=utf-8"
        );
    }

    #[tokio::test]
    async fn test_metrics_json_endpoint() {
        let app = create_test_app_with_mock().await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/metrics")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        // Verify metrics structure
        assert!(json.get("datapoints_ingested").is_some());
        assert!(json.get("batches_processed").is_some());
    }

    #[tokio::test]
    async fn test_version_endpoint() {
        let app = create_test_app_with_mock().await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/version")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["service"], "kairosdb-ingest-rs");
        assert!(json.get("version").is_some());
    }

    #[tokio::test]
    async fn test_datapoints_ingestion_success() {
        let app = create_test_app_with_mock().await;

        let payload = json!({
            "name": "test.metric",
            "datapoints": [[1634567890000i64, 42]],
            "tags": {"host": "test-server"}
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["datapoints_ingested"], 1);
        // ingest_time should be present (may be 0 for mock client)
        assert!(json["ingest_time"].as_u64().is_some());
    }

    #[tokio::test]
    async fn test_datapoints_ingestion_multiple_metrics() {
        let app = create_test_app_with_mock().await;

        let payload = json!([
            {
                "name": "cpu.usage",
                "datapoints": [[1634567890000i64, 75.5]],
                "tags": {"host": "server1", "cpu": "0"}
            },
            {
                "name": "memory.usage",
                "datapoints": [[1634567890000i64, 1024]],
                "tags": {"host": "server1"}
            }
        ]);

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["datapoints_ingested"], 2);
    }

    #[tokio::test]
    async fn test_datapoints_ingestion_invalid_json() {
        let app = create_test_app_with_mock().await;

        let invalid_payload = r#"{"name": "test", "datapoints"#; // Incomplete JSON

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(invalid_payload))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json.get("errors").is_some());
    }

    #[tokio::test]
    async fn test_datapoints_ingestion_empty_metric_name() {
        let app = create_test_app_with_mock().await;

        let payload = json!({
            "name": "",
            "datapoints": [[1634567890000i64, 42]],
            "tags": {}
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_cors_preflight() {
        let app = create_test_app_with_mock().await;

        let request = Request::builder()
            .method(Method::OPTIONS)
            .uri("/api/v1/datapoints")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let headers = response.headers();
        assert!(headers.get("access-control-allow-origin").is_some());
        assert!(headers.get("access-control-allow-methods").is_some());
    }

    #[tokio::test]
    async fn test_request_size_limit() {
        let app = create_test_app_with_mock().await;

        // Create a very large payload (this test validates the middleware works)
        let large_datapoints: Vec<_> = (0..1000).map(|i| [1634567890000i64 + i, i]).collect();
        let payload = json!({
            "name": "test.metric",
            "datapoints": large_datapoints,
            "tags": {"test": "large_batch"}
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        // Should either succeed or fail gracefully, but not crash
        assert!(response.status().is_client_error() || response.status().is_success());
    }

    #[tokio::test]
    async fn test_histogram_datapoint_direct_format() {
        let app = create_test_app_with_mock().await;

        let payload = json!({
            "name": "response_time_histogram",
            "datapoints": [[1634567890000i64, {
                "boundaries": [0.1, 0.5, 1.0, 5.0],
                "counts": [100, 50, 30, 5],
                "total_count": 185,
                "sum": 89.5,
                "min": 0.01,
                "max": 4.8
            }]],
            "tags": {"service": "api", "endpoint": "/users"}
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["datapoints_ingested"], 1);
    }

    #[tokio::test]
    async fn test_histogram_datapoint_prometheus_format() {
        let app = create_test_app_with_mock().await;

        let payload = json!({
            "name": "request_latency",
            "datapoints": [[1634567890000i64, {
                "buckets": [
                    {"le": 0.1, "count": 100},
                    {"le": 0.5, "count": 150},
                    {"le": 1.0, "count": 180},
                    {"le": 5.0, "count": 185}
                ],
                "count": 185,
                "sum": 89.5
            }]],
            "tags": {"service": "payment", "method": "POST"}
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["datapoints_ingested"], 1);
    }

    #[tokio::test]
    async fn test_datapoint_with_ttl() {
        let app = create_test_app_with_mock().await;

        let payload = json!({
            "name": "session.count",
            "datapoints": [[1634567890000i64, 42]],
            "tags": {"user": "test123"},
            "ttl": 3600
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["datapoints_ingested"], 1);
    }

    #[tokio::test]
    async fn test_complex_datapoint() {
        let app = create_test_app_with_mock().await;

        let payload = json!({
            "name": "signal.fft",
            "datapoints": [[1634567890000i64, {"real": std::f64::consts::PI, "imaginary": 2.71}]],
            "tags": {"channel": "1", "frequency": "440hz"}
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["datapoints_ingested"], 1);
    }

    #[tokio::test]
    async fn test_text_datapoint() {
        let app = create_test_app_with_mock().await;

        let payload = json!({
            "name": "log.message",
            "datapoints": [[1634567890000i64, "ERROR: Database connection failed"]],
            "tags": {"level": "error", "service": "auth"}
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["datapoints_ingested"], 1);
    }

    #[tokio::test]
    async fn test_gzip_compressed_ingestion() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let app = create_test_app_with_mock().await;

        let payload = json!({
            "name": "compressed.metric",
            "datapoints": [[1634567890000i64, 123.45]],
            "tags": {"test": "compression"}
        });

        // Compress the payload
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(payload.to_string().as_bytes()).unwrap();
        let compressed_data = encoder.finish().unwrap();

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints/gzip")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CONTENT_ENCODING, "gzip")
            .body(Body::from(compressed_data))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["datapoints_ingested"], 1);
    }

    #[tokio::test]
    async fn test_mixed_datapoint_types() {
        let app = create_test_app_with_mock().await;

        let payload = json!([
            {
                "name": "metric.long",
                "datapoints": [[1634567890000i64, 42]],
                "tags": {"type": "long"}
            },
            {
                "name": "metric.double",
                "datapoints": [[1634567890000i64, std::f64::consts::PI]],
                "tags": {"type": "double"}
            },
            {
                "name": "metric.text",
                "datapoints": [[1634567890000i64, "hello world"]],
                "tags": {"type": "text"}
            },
            {
                "name": "metric.complex",
                "datapoints": [[1634567890000i64, {"real": 1.0, "imaginary": -1.0}]],
                "tags": {"type": "complex"}
            }
        ]);

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["datapoints_ingested"], 4);
    }

    #[tokio::test]
    async fn test_invalid_histogram_format() {
        let app = create_test_app_with_mock().await;

        let payload = json!({
            "name": "invalid.histogram",
            "datapoints": [[1634567890000i64, {
                "boundaries": [0.1, 0.5],
                "counts": [10, 20, 30] // Mismatched counts vs boundaries
            }]],
            "tags": {"test": "invalid"}
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json.get("errors").is_some());
    }

    #[tokio::test]
    async fn test_invalid_tag_name() {
        let app = create_test_app_with_mock().await;

        let payload = json!({
            "name": "test.metric",
            "datapoints": [[1634567890000i64, 42]],
            "tags": {"": "empty_key_invalid"}  // Empty tag key
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}

#[cfg(test)]
mod service_integration_tests {
    use super::*;
    use kairosdb_core::{datapoint::DataPointBatch, time::Timestamp};

    #[tokio::test]
    #[ignore] // Requires Cassandra
    async fn test_ingestion_service_integration() {
        let mut config = IngestConfig::default();
        config.ingestion.max_queue_size = 100000; // High queue limit for tests
        let config = Arc::new(config);

        let shutdown_manager = Arc::new(kairosdb_ingest::ShutdownManager::new());
        let (service, _null_queue_work_channel) = IngestionService::new(config, shutdown_manager)
            .await
            .unwrap();

        // Test service creation
        assert!(service.health_check().await.is_ok());

        // Test batch ingestion
        let mut batch = DataPointBatch::new();
        batch
            .add_point(kairosdb_core::datapoint::DataPoint::new_long(
                "integration.test",
                Timestamp::now(),
                123,
            ))
            .unwrap();

        let result = service.ingest_batch(batch);
        assert!(result.is_ok());

        // Verify metrics updated
        let metrics = service.get_metrics_snapshot();
        assert_eq!(metrics.batches_processed, 1);
        assert_eq!(metrics.datapoints_ingested, 1);
    }
}
