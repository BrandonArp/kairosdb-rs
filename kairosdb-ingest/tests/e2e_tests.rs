//! End-to-end tests for KairosDB ingestion service with Docker Cassandra
//!
//! These tests spin up a real Cassandra instance in Docker and test the complete system.

use anyhow::Result;
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
use std::{sync::Arc, time::Duration};
use testcontainers::{clients::Cli, Container, RunnableImage};
use testcontainers_modules::cassandra::Cassandra;
use tokio::time::sleep;
use tower::ServiceExt;

/// Test fixture that manages a Cassandra container and test app
pub struct TestFixture<'a> {
    #[allow(dead_code)]
    container: Container<'a, Cassandra>,
    app: Router,
    cassandra_port: u16,
}

impl<'a> TestFixture<'a> {
    /// Create a new test fixture with Cassandra container
    pub async fn new(docker: &'a Cli) -> Result<Self> {
        // Start Cassandra container
        let cassandra_image = RunnableImage::from(Cassandra::default())
            .with_env_var("CASSANDRA_CLUSTER_NAME", "Test Cluster")
            .with_env_var("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch")
            .with_env_var("CASSANDRA_DC", "datacenter1");
            
        let container = docker.run(cassandra_image);
        let cassandra_port = container.get_host_port_ipv4(9042);
        
        // Wait for Cassandra to be ready
        Self::wait_for_cassandra_ready(cassandra_port).await?;
        
        // Create test configuration
        let mut config = IngestConfig::default();
        config.cassandra.contact_points = vec![format!("localhost:{}", cassandra_port)];
        config.cassandra.keyspace = "test_kairosdb".to_string();
        config.cassandra.replication_factor = 1;
        
        let config = Arc::new(config);
        
        // Initialize ingestion service with test config
        let ingestion_service = IngestionService::new(config.clone()).await?;
        
        let state = AppState {
            ingestion_service: Arc::new(ingestion_service),
            config,
        };
        
        // Create test app
        let app = create_test_router(state);
        
        Ok(TestFixture {
            container,
            app,
            cassandra_port,
        })
    }
    
    /// Wait for Cassandra to be ready for connections
    async fn wait_for_cassandra_ready(port: u16) -> Result<()> {
        use cdrs_tokio::cluster::session::{Session, TcpSessionBuilder};
        use cdrs_tokio::cluster::NodeTcpConfigBuilder;
        use cdrs_tokio::load_balancing::RoundRobin;
        use cdrs_tokio::authenticators::NoneAuthenticator;
        
        let mut attempts = 0;
        let max_attempts = 30; // 30 seconds timeout
        
        while attempts < max_attempts {
            let node = NodeTcpConfigBuilder::new(format!("localhost:{}", port), NoneAuthenticator {}).build();
            let session_result = TcpSessionBuilder::new(RoundRobin::new(), node).build().await;
            
            if session_result.is_ok() {
                println!("Cassandra is ready on port {}", port);
                
                // Create test keyspace
                if let Ok(session) = session_result {
                    let create_keyspace = format!(
                        "CREATE KEYSPACE IF NOT EXISTS test_kairosdb WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
                    );
                    
                    let _ = session.query(create_keyspace).await;
                    println!("Test keyspace created");
                }
                
                return Ok(());
            }
            
            attempts += 1;
            sleep(Duration::from_secs(1)).await;
        }
        
        Err(anyhow::anyhow!("Cassandra failed to become ready within {} seconds", max_attempts))
    }
    
    /// Get the app for testing
    pub fn app(&self) -> &Router {
        &self.app
    }
    
    /// Get the Cassandra port
    pub fn cassandra_port(&self) -> u16 {
        self.cassandra_port
    }
}

/// Create a test router for testing
fn create_test_router(state: AppState) -> Router {
    use kairosdb_ingest::handlers::*;
    use axum::routing::{get, post};
    
    Router::new()
        .route("/health", get(health_handler))
        .route("/health/ready", get(health_handler))
        .route("/health/live", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/api/v1/metrics", get(metrics_json_handler))
        .route("/api/v1/datapoints", 
            post(ingest_handler).options(cors_preflight_datapoints))
        .route("/api/v1/datapoints/gzip", 
            post(ingest_gzip_handler).options(cors_preflight_datapoints))
        .route("/api/v1/version", get(version_handler))
        .with_state(state)
}

#[cfg(test)]
mod e2e_tests {
    use super::*;
    
    #[tokio::test]
    async fn test_full_stack_health_check() -> Result<()> {
        let docker = Cli::default();
        let fixture = TestFixture::new(&docker).await?;
        
        let request = Request::builder()
            .method(Method::GET)
            .uri("/health")
            .body(Body::empty())?;
        
        let response = fixture.app().clone().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::OK);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let json: Value = serde_json::from_slice(&body)?;
        
        assert_eq!(json["service"], "kairosdb-ingest");
        assert_eq!(json["status"], "healthy");
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_full_stack_single_metric_ingestion() -> Result<()> {
        let docker = Cli::default();
        let fixture = TestFixture::new(&docker).await?;
        
        let payload = json!({
            "name": "e2e.test.metric",
            "datapoints": [[1634567890000i64, 42.5]],
            "tags": {"test": "e2e", "host": "test-server"}
        });
        
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))?;
        
        let response = fixture.app().clone().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::OK);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let json: Value = serde_json::from_slice(&body)?;
        
        assert_eq!(json["datapoints_ingested"], 1);
        assert!(json.get("ingest_time").is_some());
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_full_stack_batch_ingestion() -> Result<()> {
        let docker = Cli::default();
        let fixture = TestFixture::new(&docker).await?;
        
        let payload = json!([
            {
                "name": "e2e.batch.cpu",
                "datapoints": [
                    [1634567890000i64, 25.5],
                    [1634567891000i64, 30.2],
                    [1634567892000i64, 35.8]
                ],
                "tags": {"host": "batch-server-1", "core": "0"}
            },
            {
                "name": "e2e.batch.memory", 
                "datapoints": [
                    [1634567890000i64, 1024],
                    [1634567891000i64, 1536],
                    [1634567892000i64, 2048]
                ],
                "tags": {"host": "batch-server-1", "type": "used"}
            }
        ]);
        
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))?;
        
        let response = fixture.app().clone().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::OK);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let json: Value = serde_json::from_slice(&body)?;
        
        assert_eq!(json["datapoints_ingested"], 6);
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_full_stack_compression() -> Result<()> {
        let docker = Cli::default();
        let fixture = TestFixture::new(&docker).await?;
        
        let payload = json!({
            "name": "e2e.compression.test",
            "datapoints": [[1634567890000i64, 123.456]],
            "tags": {"compression": "gzip", "test": "e2e"}
        });
        
        // Compress the payload
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(payload.to_string().as_bytes())?;
        let compressed_data = encoder.finish()?;
        
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints/gzip")
            .header("content-type", "application/json")
            .header("content-encoding", "gzip")
            .body(Body::from(compressed_data))?;
        
        let response = fixture.app().clone().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::OK);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let json: Value = serde_json::from_slice(&body)?;
        
        assert_eq!(json["datapoints_ingested"], 1);
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_full_stack_data_types() -> Result<()> {
        let docker = Cli::default();
        let fixture = TestFixture::new(&docker).await?;
        
        // Test different data types
        let payload = json!([
            {
                "name": "e2e.types.long",
                "datapoints": [[1634567890000i64, 9223372036854775807i64]],
                "tags": {"type": "long"}
            },
            {
                "name": "e2e.types.double",
                "datapoints": [[1634567890000i64, 3.14159265359]],
                "tags": {"type": "double"}
            },
            {
                "name": "e2e.types.string",
                "datapoints": [[1634567890000i64, "test-string-value"]],
                "tags": {"type": "string"}
            }
        ]);
        
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))?;
        
        let response = fixture.app().clone().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::OK);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let json: Value = serde_json::from_slice(&body)?;
        
        assert_eq!(json["datapoints_ingested"], 3);
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_full_stack_error_handling() -> Result<()> {
        let docker = Cli::default();
        let fixture = TestFixture::new(&docker).await?;
        
        // Test invalid JSON
        let invalid_json = r#"{"name": "test", "invalid"#;
        
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header("content-type", "application/json")
            .body(Body::from(invalid_json))?;
        
        let response = fixture.app().clone().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let json: Value = serde_json::from_slice(&body)?;
        assert!(json.get("errors").is_some());
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_full_stack_performance_benchmark() -> Result<()> {
        let docker = Cli::default();
        let fixture = TestFixture::new(&docker).await?;
        
        // Create a large batch of data points
        let mut datapoints = Vec::new();
        let base_time = 1634567890000i64;
        
        for i in 0..1000 {
            datapoints.push([base_time + i, i as f64 * 0.1]);
        }
        
        let payload = json!({
            "name": "e2e.performance.benchmark",
            "datapoints": datapoints,
            "tags": {"test": "performance", "batch_size": "1000"}
        });
        
        let start = std::time::Instant::now();
        
        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))?;
        
        let response = fixture.app().clone().oneshot(request).await?;
        let duration = start.elapsed();
        
        assert_eq!(response.status(), StatusCode::OK);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        let json: Value = serde_json::from_slice(&body)?;
        
        assert_eq!(json["datapoints_ingested"], 1000);
        
        // Performance assertion: should process 1000 points in less than 2 seconds
        assert!(duration.as_millis() < 2000, "Performance test failed: took {:?}", duration);
        
        println!("✅ Ingested 1000 data points in {:?}", duration);
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_full_stack_concurrent_ingestion() -> Result<()> {
        let docker = Cli::default();
        let fixture = TestFixture::new(&docker).await?;
        
        let app = Arc::new(fixture.app().clone());
        let mut handles = Vec::new();
        let concurrent_requests = 5;
        
        for i in 0..concurrent_requests {
            let app_clone = app.clone();
            let handle = tokio::spawn(async move {
                let payload = json!({
                    "name": format!("e2e.concurrent.metric.{}", i),
                    "datapoints": [[1634567890000i64 + i, i * 100]],
                    "tags": {"thread": i.to_string(), "test": "concurrent"}
                });
                
                let request = Request::builder()
                    .method(Method::POST)
                    .uri("/api/v1/datapoints")
                    .header("content-type", "application/json")
                    .body(Body::from(payload.to_string()))
                    .unwrap();
                
                app_clone.oneshot(request).await
            });
            
            handles.push(handle);
        }
        
        let start = std::time::Instant::now();
        
        // Wait for all requests to complete
        for handle in handles {
            let response = handle.await??;
            assert_eq!(response.status(), StatusCode::OK);
        }
        
        let duration = start.elapsed();
        
        println!("✅ Processed {} concurrent requests in {:?}", concurrent_requests, duration);
        
        // All requests should complete quickly
        assert!(duration.as_secs() < 5, "Concurrent test took too long: {:?}", duration);
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_full_stack_metrics_endpoint() -> Result<()> {
        let docker = Cli::default();
        let fixture = TestFixture::new(&docker).await?;
        
        // First ingest some data to generate metrics
        let payload = json!({
            "name": "e2e.metrics.test",
            "datapoints": [[1634567890000i64, 42]],
            "tags": {"test": "metrics"}
        });
        
        let ingest_request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints")
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))?;
        
        let ingest_response = fixture.app().clone().oneshot(ingest_request).await?;
        assert_eq!(ingest_response.status(), StatusCode::OK);
        
        // Now check metrics endpoint
        let metrics_request = Request::builder()
            .method(Method::GET)
            .uri("/metrics")
            .body(Body::empty())?;
        
        let metrics_response = fixture.app().clone().oneshot(metrics_request).await?;
        assert_eq!(metrics_response.status(), StatusCode::OK);
        
        let content_type = metrics_response.headers().get("content-type").unwrap();
        assert!(content_type.to_str().unwrap().starts_with("text/plain"));
        
        let body = axum::body::to_bytes(metrics_response.into_body(), usize::MAX).await?;
        let metrics_text = String::from_utf8(body.to_vec())?;
        
        // Check for expected metrics
        assert!(metrics_text.contains("kairosdb_datapoints_ingested_total"));
        assert!(metrics_text.contains("kairosdb_batches_processed_total"));
        
        Ok(())
    }
}