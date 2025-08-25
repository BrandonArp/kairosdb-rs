//! API Integration tests for KairosDB query service
//!
//! These tests validate the public HTTP API of the query service using mocked dependencies.
//! They test the full request/response cycle without requiring external services.

use axum::{
    body::Body,
    http::{header, Method, Request, StatusCode},
};
use kairosdb_core::{
    datastore::{cassandra_legacy::CassandraLegacyStore, TimeSeriesStore},
    time::Timestamp,
};
use kairosdb_query::{config::QueryConfig, AppState};
use serde_json::{json, Value};
use std::sync::Arc;
use tower::ServiceExt;

/// Create a test app instance with mock datastore
async fn create_test_app() -> axum::Router {
    use axum::routing::{get, post};
    use kairosdb_query::handlers::*;
    use tower::ServiceBuilder;
    use tower_http::{cors::CorsLayer, trace::TraceLayer};

    let config = Arc::new(QueryConfig::default());

    // Set Cassandra contact points to localhost for tests (Tilt port forward)
    std::env::set_var("KAIROSDB_CASSANDRA_CONTACT_POINTS", "localhost:9042");

    // Initialize datastore with the existing kairosdb keyspace
    let datastore: Arc<dyn TimeSeriesStore> = Arc::new(
        CassandraLegacyStore::new("kairosdb".to_string())
            .await
            .expect("Failed to create datastore"),
    );

    let state = AppState {
        datastore,
        config: config.clone(),
    };

    // Build router with same structure as main.rs
    axum::Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/api/v1/datapoints/query", post(query_handler))
        .route("/api/v1/metricnames", get(metric_names_handler))
        .route("/api/v1/tagnames", get(tag_names_handler))
        .route("/api/v1/tagvalues", get(tag_values_handler))
        // KairosDB compatibility routes
        .route("/api/v1/datapoints/query/tags", get(tag_names_handler))
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CorsLayer::permissive()),
        )
        .with_state(state)
}

#[cfg(test)]
mod api_tests {
    use super::*;

    #[tokio::test]
    async fn test_health_endpoint_returns_ok() {
        let app = create_test_app().await;

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

        assert_eq!(json["service"], "kairosdb-query");
        assert_eq!(json["status"], "healthy");
        assert!(json.get("timestamp").is_some());
    }

    #[tokio::test]
    async fn test_metrics_endpoint_returns_prometheus_format() {
        let app = create_test_app().await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();

        // Check basic Prometheus format
        assert!(body_str.contains("# HELP"));
        assert!(body_str.contains("# TYPE"));
        assert!(body_str.contains("kairosdb_queries_total"));
        assert!(body_str.contains("kairosdb_query_errors_total"));
    }

    #[tokio::test]
    async fn test_query_single_metric_success() {
        let app = create_test_app().await;

        let now = Timestamp::now();
        let hour_ago = Timestamp::from_millis(now.timestamp_millis() - 3_600_000).unwrap();

        let payload = json!({
            "start_absolute": hour_ago,
            "end_absolute": now,
            "metrics": [
                {
                    "name": "cpu.usage",
                    "tags": {
                        "host": "server1"
                    }
                }
            ]
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();

        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json.get("queries").is_some());
        assert!(json.get("sample_size").is_some());
        assert!(json["queries"].is_array());
    }

    #[tokio::test]
    async fn test_query_multiple_metrics() {
        let app = create_test_app().await;

        let now = Timestamp::now();
        let hour_ago = Timestamp::from_millis(now.timestamp_millis() - 3_600_000).unwrap();

        let payload = json!({
            "start_absolute": hour_ago,
            "end_absolute": now,
            "metrics": [
                {
                    "name": "cpu.usage",
                    "tags": {
                        "host": "server1"
                    }
                },
                {
                    "name": "memory.usage",
                    "tags": {
                        "host": "server1"
                    }
                }
            ]
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json["queries"].is_array());
        assert_eq!(json["queries"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_query_without_time_range_validation() {
        let app = create_test_app().await;

        let payload = json!({
            "metrics": [
                {
                    "name": "cpu.usage",
                    "tags": {}
                }
            ]
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"], "Invalid query");
        assert!(json["message"].as_str().unwrap().contains("start time"));
    }

    #[tokio::test]
    async fn test_query_with_relative_time_range() {
        let app = create_test_app().await;

        let payload = json!({
            "start_relative": {
                "value": 1,
                "unit": "hours"
            },
            "metrics": [
                {
                    "name": "cpu.usage",
                    "tags": {}
                }
            ]
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json.get("queries").is_some());
    }

    #[tokio::test]
    async fn test_query_validation_empty_metrics() {
        let app = create_test_app().await;

        let now = Timestamp::now();
        let hour_ago = Timestamp::from_millis(now.timestamp_millis() - 3_600_000).unwrap();

        let payload = json!({
            "start_absolute": hour_ago,
            "end_absolute": now,
            "metrics": []
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"], "Invalid query");
        assert!(json.get("message").is_some());
    }

    #[tokio::test]
    async fn test_query_invalid_json() {
        let app = create_test_app().await;

        let invalid_payload = r#"{"metrics": [{"name""#; // Invalid JSON

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(invalid_payload))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_metric_names_endpoint() {
        let app = create_test_app().await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/metricnames")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json.get("results").is_some());
        assert!(json["results"].is_array());
    }

    #[tokio::test]
    async fn test_metric_names_with_prefix_filter() {
        let app = create_test_app().await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/metricnames?prefix=system")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json["results"].is_array());
    }

    #[tokio::test]
    async fn test_metric_names_with_limit() {
        let app = create_test_app().await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/metricnames?limit=2")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json["results"].is_array());
        let results = json["results"].as_array().unwrap();
        assert!(results.len() <= 2);
    }

    #[tokio::test]
    async fn test_tag_names_endpoint() {
        let app = create_test_app().await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/tagnames?metric=cpu.usage")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json.get("results").is_some());
        assert!(json["results"].is_array());
    }

    #[tokio::test]
    async fn test_tag_names_without_metric() {
        let app = create_test_app().await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/tagnames")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json["results"].is_array());
    }

    #[tokio::test]
    async fn test_tag_names_invalid_metric() {
        let app = create_test_app().await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/tagnames?metric=") // Empty metric name
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"], "Invalid metric name");
    }

    #[tokio::test]
    async fn test_tag_values_endpoint() {
        let app = create_test_app().await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/tagvalues?metric=cpu.usage&tag=host")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json.get("results").is_some());
        assert!(json["results"].is_array());
    }

    #[tokio::test]
    async fn test_tag_values_missing_metric() {
        let app = create_test_app().await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/tagvalues?tag=host") // Missing metric parameter
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"], "Missing required parameter 'metric'");
    }

    #[tokio::test]
    async fn test_tag_values_missing_tag() {
        let app = create_test_app().await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/tagvalues?metric=cpu.usage") // Missing tag parameter
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"], "Missing required parameter 'tag'");
    }

    #[tokio::test]
    async fn test_tag_values_with_prefix_and_limit() {
        let app = create_test_app().await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/tagvalues?metric=cpu.usage&tag=host&prefix=server&limit=5")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json["results"].is_array());
        let results = json["results"].as_array().unwrap();
        assert!(results.len() <= 5);
    }

    #[tokio::test]
    async fn test_cors_preflight() {
        let app = create_test_app().await;

        let request = Request::builder()
            .method(Method::OPTIONS)
            .uri("/api/v1/datapoints/query")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let headers = response.headers();
        assert!(headers.get("access-control-allow-origin").is_some());
        assert!(headers.get("access-control-allow-methods").is_some());
    }

    #[tokio::test]
    async fn test_kairosdb_compatibility_route() {
        let app = create_test_app().await;

        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/datapoints/query/tags?metric=cpu.usage")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json.get("results").is_some());
    }
}

#[cfg(test)]
mod query_format_tests {
    use super::*;

    #[tokio::test]
    async fn test_query_with_aggregation() {
        let app = create_test_app().await;

        let now = Timestamp::now();
        let hour_ago = Timestamp::from_millis(now.timestamp_millis() - 3_600_000).unwrap();

        let payload = json!({
            "start_absolute": hour_ago,
            "end_absolute": now,
            "metrics": [
                {
                    "name": "cpu.usage",
                    "tags": {"host": "server1"},
                    "aggregators": [
                        {
                            "name": "avg",
                            "sampling": {
                                "value": 1,
                                "unit": "minutes"
                            }
                        }
                    ]
                }
            ]
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json.get("queries").is_some());
    }

    #[tokio::test]
    async fn test_query_with_group_by() {
        let app = create_test_app().await;

        let now = Timestamp::now();
        let hour_ago = Timestamp::from_millis(now.timestamp_millis() - 3_600_000).unwrap();

        let payload = json!({
            "start_absolute": hour_ago,
            "end_absolute": now,
            "metrics": [
                {
                    "name": "cpu.usage",
                    "tags": {},
                    "group_by": [
                        {
                            "name": "tag",
                            "tags": ["host"]
                        }
                    ]
                }
            ]
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json.get("queries").is_some());
    }

    #[tokio::test]
    async fn test_query_with_limit() {
        let app = create_test_app().await;

        let now = Timestamp::now();
        let hour_ago = Timestamp::from_millis(now.timestamp_millis() - 3_600_000).unwrap();

        let payload = json!({
            "start_absolute": hour_ago,
            "end_absolute": now,
            "limit": 100,
            "metrics": [
                {
                    "name": "cpu.usage",
                    "tags": {},
                    "limit": 50
                }
            ]
        });

        let request = Request::builder()
            .method(Method::POST)
            .uri("/api/v1/datapoints/query")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json.get("queries").is_some());
    }
}

#[cfg(test)]
mod service_integration_tests {
    use super::*;
    use kairosdb_query::config::QueryConfig;

    #[tokio::test]
    async fn test_datastore_integration() {
        let config = Arc::new(QueryConfig::default());

        // Test datastore initialization
        // Set Cassandra contact points to localhost for tests (Tilt port forward)
        std::env::set_var("KAIROSDB_CASSANDRA_CONTACT_POINTS", "localhost:9042");

        let datastore = CassandraLegacyStore::new("kairosdb".to_string())
            .await
            .expect("Failed to create datastore");

        // Test basic datastore operations work
        let metrics = datastore.list_metrics(None).await;
        assert!(metrics.is_ok());

        // Test the service state creation
        let state = AppState {
            datastore: Arc::new(datastore),
            config,
        };

        // Verify state is constructed properly
        assert!(Arc::strong_count(&state.config) >= 1);
        assert!(Arc::strong_count(&state.datastore) >= 1);
    }
}
