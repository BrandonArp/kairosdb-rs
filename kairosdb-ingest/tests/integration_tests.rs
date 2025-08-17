//! Integration tests for KairosDB ingestion service
//!
//! These tests require a running Cassandra instance and test the full HTTP API.

use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
    Router,
};
use kairosdb_ingest::{
    config::IngestConfig,
    ingestion::IngestionService,
    AppState,
};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

/// Create a test app instance
async fn create_test_app() -> Router {
    let config = Arc::new(IngestConfig::default());
    
    // Note: This will fail without Cassandra running
    // In a real test environment, we'd use testcontainers
    let ingestion_service = match IngestionService::new(config.clone()).await {
        Ok(service) => Arc::new(service),
        Err(_) => {
            // Return a mock service for unit testing
            panic!("Integration tests require Cassandra to be running");
        }
    };
    
    let state = AppState {
        ingestion_service,
        config,
    };
    
    kairosdb_ingest::create_router(state)
}

#[cfg(test)]
mod http_api_tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires Cassandra
    async fn test_health_endpoint() {
        let app = create_test_app().await;
        
        let request = Request::builder()
            .method(Method::GET)
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        
        assert_eq!(json["service"], "kairosdb-ingest");
        assert!(json.get("status").is_some());
    }

    #[tokio::test]
    #[ignore] // Requires Cassandra
    async fn test_metrics_endpoint() {
        let app = create_test_app().await;
        
        let request = Request::builder()
            .method(Method::GET)
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();
        
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        
        let content_type = response.headers().get("content-type").unwrap();
        assert!(content_type.to_str().unwrap().starts_with("text/plain"));
    }

    #[tokio::test]
    #[ignore] // Requires Cassandra
    async fn test_version_endpoint() {
        let app = create_test_app().await;
        
        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/version")
            .body(Body::empty())
            .unwrap();
        
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        
        assert_eq!(json["service"], "kairosdb-ingest-rs");
        assert!(json.get("version").is_some());
    }

    #[tokio::test]
    #[ignore] // Requires Cassandra
    async fn test_ingest_single_metric() {
        let app = create_test_app().await;
        
        let payload = json!({
            "name": "cpu.usage",
            "datapoints": [[1634567890000i64, 75.5]],
            "tags": {"host": "server1", "cpu": "0"}
        });
        
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();
        
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        
        assert_eq!(json["datapoints_ingested"], 1);
        assert!(json.get("ingest_time").is_some());
    }

    #[tokio::test]
    #[ignore] // Requires Cassandra
    async fn test_ingest_multiple_metrics() {
        let app = create_test_app().await;
        
        let payload = json!([
            {
                "name": "cpu.usage",
                "datapoints": [[1634567890000i64, 75.5], [1634567891000i64, 80.2]],
                "tags": {"host": "server1"}
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
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();
        
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        
        assert_eq!(json["datapoints_ingested"], 3);
    }

    #[tokio::test]
    #[ignore] // Requires Cassandra
    async fn test_ingest_invalid_json() {
        let app = create_test_app().await;
        
        let invalid_payload = r#"{"name": "test", "invalid"#; // Malformed JSON
        
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header("content-type", "application/json")
            .body(Body::from(invalid_payload))
            .unwrap();
        
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        
        assert!(json.get("errors").is_some());
    }

    #[tokio::test]
    #[ignore] // Requires Cassandra
    async fn test_ingest_empty_metric_name() {
        let app = create_test_app().await;
        
        let payload = json!({
            "name": "",
            "datapoints": [[1634567890000i64, 42]],
            "tags": {}
        });
        
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();
        
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        
        assert!(json.get("errors").is_some());
    }

    #[tokio::test]
    #[ignore] // Requires Cassandra  
    async fn test_cors_preflight() {
        let app = create_test_app().await;
        
        let request = Request::builder()
            .method(Method::OPTIONS)
            .uri("/api/v1/datapoints")
            .header("Access-Control-Request-Method", "POST")
            .header("Access-Control-Request-Headers", "Content-Type")
            .body(Body::empty())
            .unwrap();
        
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        
        let headers = response.headers();
        assert!(headers.get("Access-Control-Allow-Origin").is_some());
        assert!(headers.get("Access-Control-Allow-Methods").is_some());
    }

    #[tokio::test]
    #[ignore] // Requires Cassandra
    async fn test_request_too_large() {
        let app = create_test_app().await;
        
        // Create a very large payload
        let large_payload = "x".repeat(200 * 1024 * 1024); // 200MB
        
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header("content-type", "application/json")
            .header("content-length", large_payload.len().to_string())
            .body(Body::from(large_payload))
            .unwrap();
        
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
}

#[cfg(test)]
mod compression_tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    fn compress_string(data: &str) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data.as_bytes()).unwrap();
        encoder.finish().unwrap()
    }

    #[tokio::test]
    #[ignore] // Requires Cassandra
    async fn test_gzipped_request() {
        let app = create_test_app().await;
        
        let payload = json!({
            "name": "compressed.metric",
            "datapoints": [[1634567890000i64, 42]],
            "tags": {"compression": "gzip"}
        });
        
        let compressed_data = compress_string(&payload.to_string());
        
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints/gzip")
            .header("content-type", "application/json")
            .header("content-encoding", "gzip")
            .body(Body::from(compressed_data))
            .unwrap();
        
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        
        assert_eq!(json["datapoints_ingested"], 1);
    }
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    #[ignore] // Requires Cassandra and is slow
    async fn test_batch_ingestion_performance() {
        let app = create_test_app().await;
        
        // Create a batch of 1000 data points
        let mut datapoints = Vec::new();
        let base_time = 1634567890000i64;
        
        for i in 0..1000 {
            datapoints.push([base_time + i, i]);
        }
        
        let payload = json!({
            "name": "performance.test",
            "datapoints": datapoints,
            "tags": {"test": "performance", "batch_size": "1000"}
        });
        
        let start = Instant::now();
        
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();
        
        let response = app.oneshot(request).await.unwrap();
        let duration = start.elapsed();
        
        assert_eq!(response.status(), StatusCode::OK);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        
        assert_eq!(json["datapoints_ingested"], 1000);
        
        // Performance assertion: should process 1000 points in less than 1 second
        assert!(duration.as_millis() < 1000, "Batch ingestion took too long: {:?}", duration);
        
        println!("Ingested 1000 data points in {:?}", duration);
    }

    #[tokio::test]
    #[ignore] // Requires Cassandra and is very slow
    async fn test_concurrent_requests() {
        let app = Arc::new(create_test_app().await);
        
        let mut handles = Vec::new();
        let concurrent_requests = 10;
        
        for i in 0..concurrent_requests {
            let app_clone = app.clone();
            let handle = tokio::spawn(async move {
                let payload = json!({
                    "name": format!("concurrent.test.{}", i),
                    "datapoints": [[1634567890000i64 + i, i * 10]],
                    "tags": {"thread": i.to_string(), "test": "concurrent"}
                });
                
                let request = Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/datapoints")
                    .header("content-type", "application/json")
                    .body(Body::from(payload.to_string()))
                    .unwrap();
                
                app_clone.oneshot(request).await.unwrap()
            });
            
            handles.push(handle);
        }
        
        let start = Instant::now();
        
        // Wait for all requests to complete
        for handle in handles {
            let response = handle.await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
        
        let duration = start.elapsed();
        
        println!("Processed {} concurrent requests in {:?}", concurrent_requests, duration);
        
        // All requests should complete in reasonable time
        assert!(duration.as_secs() < 10, "Concurrent requests took too long: {:?}", duration);
    }
}

#[cfg(test)]
mod error_handling_tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires Cassandra
    async fn test_malformed_datapoints() {
        let app = create_test_app().await;
        
        let payload = json!({
            "name": "test.metric",
            "datapoints": [["invalid_timestamp", 42]],
            "tags": {}
        });
        
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();
        
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    #[ignore] // Requires Cassandra
    async fn test_missing_required_fields() {
        let app = create_test_app().await;
        
        let payload = json!({
            "datapoints": [[1634567890000i64, 42]],
            "tags": {}
            // Missing "name" field
        });
        
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();
        
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    #[ignore] // Requires Cassandra
    async fn test_invalid_metric_name() {
        let app = create_test_app().await;
        
        let payload = json!({
            "name": "a".repeat(300), // Exceeds maximum length
            "datapoints": [[1634567890000i64, 42]],
            "tags": {}
        });
        
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();
        
        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}

// Helper functions for integration tests
impl kairosdb_ingest {
    pub fn create_router(state: AppState) -> Router {
        use kairosdb_ingest::handlers::*;
        
        Router::new()
            .route("/health", get(health_handler))
            .route("/metrics", get(metrics_handler))
            .route("/api/v1/metrics", get(metrics_json_handler))
            .route("/api/v1/datapoints", 
                post(ingest_handler).options(cors_preflight_datapoints))
            .route("/api/v1/datapoints/gzip", 
                post(ingest_gzip_handler).options(cors_preflight_datapoints))
            .route("/api/v1/version", get(version_handler))
            .with_state(state)
    }
}