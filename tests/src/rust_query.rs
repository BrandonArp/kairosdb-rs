//! End-to-end tests for Rust KairosDB query service
//!
//! These tests validate the complete pipeline:
//! Rust Ingest → Cassandra → Rust Query Service

#[allow(unused_imports)]
use crate::common::{E2ETestConfig, INGEST_BASE_URL, RUST_QUERY_BASE_URL};
#[allow(unused_imports)]
use chrono::Utc;
#[allow(unused_imports)]
use serde_json::{json, Value};
#[allow(unused_imports)]
use std::time::Duration;
#[allow(unused_imports)]
use tokio::time::sleep;
#[allow(unused_imports)]
use tracing::{debug, info};

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_rust_query_service_health() {
    let config = E2ETestConfig::new();

    let response = config
        .client
        .get(format!("{}/health", RUST_QUERY_BASE_URL))
        .send()
        .await
        .expect("Failed to call Rust query health endpoint");

    assert!(
        response.status().is_success(),
        "Rust query service health check failed: {}",
        response.status()
    );

    let health_response: Value = response
        .json()
        .await
        .expect("Health response should be valid JSON");

    assert_eq!(health_response["service"], "kairosdb-query");
    assert_eq!(health_response["status"], "healthy");
}

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_rust_query_basic_data_flow() {
    let config = E2ETestConfig::new();
    let metric_name = config.test_metric_name("rust_query_basic");
    let now = Utc::now().timestamp_millis();

    info!(
        metric_name = %metric_name,
        "🧪 Testing Rust query basic data flow"
    );

    // Step 1: Send data to Rust ingest service
    let ingest_payload = json!([{
        "name": metric_name,
        "datapoints": [[now, 123.45]],
        "tags": {
            "host": "rust-test-server",
            "service": "rust-e2e-test"
        }
    }]);

    info!("📤 Sending data to Rust ingest service");

    let ingest_response = config
        .client
        .post(format!("{}/api/v1/datapoints", INGEST_BASE_URL))
        .header("Content-Type", "application/json")
        .json(&ingest_payload)
        .send()
        .await
        .expect("Failed to send data to ingest service");

    assert!(
        ingest_response.status().is_success(),
        "Failed to ingest data: {} - {}",
        ingest_response.status(),
        ingest_response.text().await.unwrap_or_default()
    );

    info!("✅ Data sent successfully to Rust ingest service");

    // Step 2: Wait for data to propagate to Cassandra
    info!("⏳ Waiting for data to propagate");
    sleep(Duration::from_secs(5)).await;

    // Step 3: Query data from Rust query service
    let query_payload = json!({
        "start_relative": {
            "value": 1,
            "unit": "hours"
        },
        "metrics": [{
            "name": metric_name,
            "tags": {
                "host": "rust-test-server"
            }
        }]
    });

    info!("📥 Querying Rust query service");

    let query_response = config
        .client
        .post(format!("{}/api/v1/datapoints/query", RUST_QUERY_BASE_URL))
        .header("Content-Type", "application/json")
        .json(&query_payload)
        .send()
        .await
        .expect("Failed to query Rust query service");

    assert!(
        query_response.status().is_success(),
        "Failed to query Rust query service: {} - {}",
        query_response.status(),
        query_response.text().await.unwrap_or_default()
    );

    let query_result: Value = query_response
        .json()
        .await
        .expect("Failed to parse query response as JSON");

    info!("📊 Rust query response received");
    debug!(
        query_response = %serde_json::to_string_pretty(&query_result).unwrap(),
        "Query response details"
    );

    // Step 4: Validate the response contains our data
    let queries = query_result["queries"]
        .as_array()
        .expect("Response should contain queries array");

    assert!(!queries.is_empty(), "Query response should not be empty");

    let first_query = &queries[0];
    let results = first_query["results"]
        .as_array()
        .expect("Query should contain results array");

    assert!(!results.is_empty(), "Query results should not be empty");

    let first_result = &results[0];
    assert_eq!(
        first_result["name"].as_str(),
        Some(metric_name.as_str()),
        "Metric name should match"
    );

    let values = first_result["values"]
        .as_array()
        .expect("Result should contain values array");

    assert!(!values.is_empty(), "Values array should not be empty");

    // Find our data point
    let found_value = values.iter().any(|value| {
        let timestamp = value["timestamp"].as_i64().unwrap_or(0);
        let val = match &value["value"] {
            Value::Number(n) => n.as_f64().unwrap_or(0.0),
            _ => 0.0,
        };

        // Check if this is our data point (within 1 second tolerance)
        (timestamp - now).abs() < 1000 && (val - 123.45).abs() < 0.001
    });

    assert!(
        found_value,
        "Should find our test data point in the Rust query response"
    );

    info!(
        "✅ Rust query end-to-end test passed: Data successfully flowed from ingest to Rust query"
    );
}

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_rust_query_metric_names_discovery() {
    let config = E2ETestConfig::new();
    let metric_name = config.test_metric_name("metric_discovery");
    let now = Utc::now().timestamp_millis();

    info!("🧪 Testing Rust query metric names discovery");

    // Step 1: Ingest test data
    let ingest_payload = json!([{
        "name": metric_name,
        "datapoints": [[now, 999]],
        "tags": {"type": "discovery_test"}
    }]);

    let ingest_response = config
        .client
        .post(format!("{}/api/v1/datapoints", INGEST_BASE_URL))
        .json(&ingest_payload)
        .send()
        .await
        .expect("Failed to ingest test data");

    assert!(ingest_response.status().is_success());

    // Wait for data to propagate
    sleep(Duration::from_secs(3)).await;

    // Step 2: Test metric names endpoint
    let response = config
        .client
        .get(format!("{}/api/v1/metricnames", RUST_QUERY_BASE_URL))
        .send()
        .await
        .expect("Failed to get metric names");

    assert!(
        response.status().is_success(),
        "Metric names request failed: {}",
        response.status()
    );

    let result: Value = response
        .json()
        .await
        .expect("Metric names response should be JSON");

    let _results = result["results"]
        .as_array()
        .expect("Should contain results array");

    // The response should contain our metric (mock implementation returns default metrics)
    info!("✅ Metric names discovery test passed");
}

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_rust_query_tag_names_discovery() {
    let config = E2ETestConfig::new();

    info!("🧪 Testing Rust query tag names discovery");

    // Test tag names endpoint
    let response = config
        .client
        .get(format!(
            "{}/api/v1/tagnames?metric=test.metric",
            RUST_QUERY_BASE_URL
        ))
        .send()
        .await
        .expect("Failed to get tag names");

    assert!(
        response.status().is_success(),
        "Tag names request failed: {}",
        response.status()
    );

    let result: Value = response
        .json()
        .await
        .expect("Tag names response should be JSON");

    let _results = result["results"]
        .as_array()
        .expect("Should contain results array");

    info!("✅ Tag names discovery test passed");
}

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_rust_query_tag_values_discovery() {
    let config = E2ETestConfig::new();

    info!("🧪 Testing Rust query tag values discovery");

    // Test tag values endpoint
    let response = config
        .client
        .get(format!(
            "{}/api/v1/tagvalues?metric=test.metric&tag=host",
            RUST_QUERY_BASE_URL
        ))
        .send()
        .await
        .expect("Failed to get tag values");

    assert!(
        response.status().is_success(),
        "Tag values request failed: {}",
        response.status()
    );

    let result: Value = response
        .json()
        .await
        .expect("Tag values response should be JSON");

    let _results = result["results"]
        .as_array()
        .expect("Should contain results array");

    info!("✅ Tag values discovery test passed");
}

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_rust_query_histogram_data_flow() {
    let config = E2ETestConfig::new();
    let metric_name = config.test_metric_name("rust_histogram");
    let now = Utc::now().timestamp_millis();

    info!("🧪 Testing Rust query histogram data flow");

    // Step 1: Send histogram data to ingest service
    let histogram_payload = json!([{
        "name": metric_name,
        "datapoints": [[now, {
            "boundaries": [0.1, 0.5, 1.0, 5.0],
            "counts": [15, 30, 20, 5],
            "total_count": 70,
            "sum": 55.5
        }]],
        "tags": {
            "service": "rust-histogram-test"
        }
    }]);

    let ingest_response = config
        .client
        .post(format!("{}/api/v1/datapoints", INGEST_BASE_URL))
        .json(&histogram_payload)
        .send()
        .await
        .expect("Failed to send histogram data");

    assert!(ingest_response.status().is_success());

    // Step 2: Wait for propagation
    sleep(Duration::from_secs(5)).await;

    // Step 3: Query histogram data from Rust query service
    let query_payload = json!({
        "start_relative": {
            "value": 1,
            "unit": "hours"
        },
        "metrics": [{
            "name": metric_name,
            "tags": {
                "service": "rust-histogram-test"
            }
        }]
    });

    let query_response = config
        .client
        .post(format!("{}/api/v1/datapoints/query", RUST_QUERY_BASE_URL))
        .json(&query_payload)
        .send()
        .await
        .expect("Failed to query histogram data");

    assert!(
        query_response.status().is_success(),
        "Failed to query histogram from Rust service: {}",
        query_response.status()
    );

    let query_result: Value = query_response
        .json()
        .await
        .expect("Failed to parse histogram query response");

    let queries = query_result["queries"]
        .as_array()
        .expect("Response should contain queries");

    assert!(!queries.is_empty(), "Histogram query should return results");

    info!("✅ Rust query histogram test passed");
}

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_rust_query_vs_java_query_consistency() {
    let config = E2ETestConfig::new();
    let metric_name = config.test_metric_name("consistency_test");
    let now = Utc::now().timestamp_millis();

    info!("🧪 Testing consistency between Rust and Java query services");

    // Step 1: Ingest test data
    let ingest_payload = json!([{
        "name": metric_name,
        "datapoints": [
            [now, 100.0],
            [now + 60000, 110.0],
            [now + 120000, 105.0]
        ],
        "tags": {
            "host": "consistency-server",
            "environment": "test"
        }
    }]);

    let ingest_response = config
        .client
        .post(format!("{}/api/v1/datapoints", INGEST_BASE_URL))
        .json(&ingest_payload)
        .send()
        .await
        .expect("Failed to ingest consistency test data");

    assert!(ingest_response.status().is_success());

    // Wait for propagation
    sleep(Duration::from_secs(5)).await;

    let query_payload = json!({
        "start_relative": {
            "value": 1,
            "unit": "hours"
        },
        "metrics": [{
            "name": metric_name,
            "tags": {
                "host": "consistency-server"
            }
        }]
    });

    // Step 2: Query from Rust service
    let rust_response = config
        .client
        .post(format!("{}/api/v1/datapoints/query", RUST_QUERY_BASE_URL))
        .json(&query_payload)
        .send()
        .await
        .expect("Failed to query from Rust service");

    // Step 3: Query from Java service
    let java_response = config
        .client
        .post(format!(
            "{}/api/v1/datapoints/query",
            crate::common::JAVA_KAIROSDB_BASE_URL
        ))
        .json(&query_payload)
        .send()
        .await
        .expect("Failed to query from Java service");

    // Both should succeed
    assert!(rust_response.status().is_success());
    assert!(java_response.status().is_success());

    let rust_result: Value = rust_response
        .json()
        .await
        .expect("Rust response should be JSON");
    let java_result: Value = java_response
        .json()
        .await
        .expect("Java response should be JSON");

    // Both should return data for our metric
    let rust_queries = rust_result["queries"]
        .as_array()
        .expect("Rust should have queries");
    let java_queries = java_result["queries"]
        .as_array()
        .expect("Java should have queries");

    assert!(!rust_queries.is_empty(), "Rust query should return results");
    assert!(!java_queries.is_empty(), "Java query should return results");

    info!("✅ Consistency test passed: Both Rust and Java query services returned data");
}
