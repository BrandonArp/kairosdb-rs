//! Data flow integration tests

#[allow(unused_imports)]
use crate::common::{E2ETestConfig, INGEST_BASE_URL, JAVA_KAIROSDB_BASE_URL};
#[allow(unused_imports)]
use chrono::Utc;
#[allow(unused_imports)]
use serde_json::{json, Value};
#[allow(unused_imports)]
use std::time::Duration;
#[allow(unused_imports)]
use tokio::time::sleep;

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_basic_data_flow() {
    let config = E2ETestConfig::new();
    let metric_name = config.test_metric_name("basic_flow");
    let now = Utc::now().timestamp_millis();

    println!("🧪 Testing basic data flow with metric: {}", metric_name);

    // Step 1: Send data to Rust ingest service
    let ingest_payload = json!([{
        "name": metric_name,
        "datapoints": [[now, 42.5]],
        "tags": {
            "host": "test-server",
            "service": "e2e-test"
        }
    }]);

    println!("📤 Sending data to ingest service...");

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

    println!("✅ Data sent successfully to ingest service");

    // Step 2: Wait for data to propagate to Cassandra
    println!("⏳ Waiting for data to propagate...");
    sleep(Duration::from_secs(5)).await;

    // Step 3: Query data from Java KairosDB
    let query_payload = json!({
        "start_relative": {
            "value": 1,
            "unit": "hours"
        },
        "metrics": [{
            "name": metric_name,
            "tags": {
                "host": ["test-server"]
            }
        }]
    });

    println!("📥 Querying Java KairosDB...");

    let query_response = config
        .client
        .post(format!(
            "{}/api/v1/datapoints/query",
            JAVA_KAIROSDB_BASE_URL
        ))
        .header("Content-Type", "application/json")
        .json(&query_payload)
        .send()
        .await
        .expect("Failed to query Java KairosDB");

    assert!(
        query_response.status().is_success(),
        "Failed to query Java KairosDB: {} - {}",
        query_response.status(),
        query_response.text().await.unwrap_or_default()
    );

    let query_result: Value = query_response
        .json()
        .await
        .expect("Failed to parse query response as JSON");

    println!("📊 Query response received");

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

    // Find our data point (there might be multiple if test runs multiple times)
    let found_value = values.iter().any(|value| {
        let timestamp = value[0].as_i64().unwrap_or(0);
        let val = value[1].as_f64().unwrap_or(0.0);

        // Check if this is our data point (within 1 second tolerance)
        (timestamp - now).abs() < 1000 && (val - 42.5).abs() < 0.001
    });

    assert!(
        found_value,
        "Should find our test data point in the response"
    );

    println!("✅ End-to-end test passed: Data successfully flowed from ingest to query");
}

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_histogram_data_flow() {
    let config = E2ETestConfig::new();
    let metric_name = config.test_metric_name("histogram_flow");
    let now = Utc::now().timestamp_millis();

    println!(
        "🧪 Testing histogram data flow with metric: {}",
        metric_name
    );

    // Step 1: Send histogram data to Rust ingest service
    let histogram_payload = json!([{
        "name": metric_name,
        "datapoints": [[now, {
            "boundaries": [0.1, 0.5, 1.0, 5.0, 10.0],
            "counts": [10, 25, 15, 8, 2],
            "total_count": 60,
            "sum": 95.5,
            "min": 0.05,
            "max": 9.8
        }]],
        "tags": {
            "service": "histogram-test",
            "endpoint": "/api/test"
        }
    }]);

    println!("📤 Sending histogram data to ingest service...");

    let ingest_response = config
        .client
        .post(format!("{}/api/v1/datapoints", INGEST_BASE_URL))
        .header("Content-Type", "application/json")
        .json(&histogram_payload)
        .send()
        .await
        .expect("Failed to send histogram data to ingest service");

    assert!(
        ingest_response.status().is_success(),
        "Failed to ingest histogram data: {} - {}",
        ingest_response.status(),
        ingest_response.text().await.unwrap_or_default()
    );

    println!("✅ Histogram data sent successfully to ingest service");

    // Step 2: Wait for data to propagate
    println!("⏳ Waiting for histogram data to propagate...");
    sleep(Duration::from_secs(5)).await;

    // Step 3: Query histogram data from Java KairosDB
    let query_payload = json!({
        "start_relative": {
            "value": 1,
            "unit": "hours"
        },
        "metrics": [{
            "name": metric_name,
            "tags": {
                "service": ["histogram-test"]
            }
        }]
    });

    println!("📥 Querying histogram data from Java KairosDB...");

    let query_response = config
        .client
        .post(format!(
            "{}/api/v1/datapoints/query",
            JAVA_KAIROSDB_BASE_URL
        ))
        .header("Content-Type", "application/json")
        .json(&query_payload)
        .send()
        .await
        .expect("Failed to query histogram data from Java KairosDB");

    if !query_response.status().is_success() {
        let status = query_response.status();
        let error_text = query_response.text().await.unwrap_or_default();
        panic!(
            "Failed to query histogram data from Java KairosDB: {} - {}",
            status, error_text
        );
    }

    let query_result: Value = query_response
        .json()
        .await
        .expect("Failed to parse histogram query response as JSON");

    println!("📊 Histogram query response received");

    // Step 4: Validate histogram data in response
    let queries = query_result["queries"]
        .as_array()
        .expect("Response should contain queries array");

    assert!(
        !queries.is_empty(),
        "Histogram query response should not be empty"
    );

    let first_query = &queries[0];
    let results = first_query["results"]
        .as_array()
        .expect("Histogram query should contain results array");

    assert!(
        !results.is_empty(),
        "Histogram query results should not be empty"
    );

    let first_result = &results[0];
    assert_eq!(
        first_result["name"].as_str(),
        Some(metric_name.as_str()),
        "Histogram metric name should match"
    );

    let values = first_result["values"]
        .as_array()
        .expect("Histogram result should contain values array");

    assert!(
        !values.is_empty(),
        "Histogram values array should not be empty"
    );

    println!("✅ Histogram end-to-end test passed: Histogram data successfully flowed from ingest to query");
}

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_multiple_metrics_batch() {
    let config = E2ETestConfig::new();
    let now = Utc::now().timestamp_millis();

    println!("🧪 Testing batch ingestion of multiple metrics");

    // Step 1: Send batch of multiple metrics
    let batch_payload = json!([
        {
            "name": config.test_metric_name("cpu_usage"),
            "datapoints": [
                [now, 45.2],
                [now + 1000, 46.1],
                [now + 2000, 44.8]
            ],
            "tags": {
                "host": "server-01",
                "region": "us-east-1"
            }
        },
        {
            "name": config.test_metric_name("memory_usage"),
            "datapoints": [
                [now, 78.5],
                [now + 1000, 79.2]
            ],
            "tags": {
                "host": "server-01",
                "region": "us-east-1"
            }
        },
        {
            "name": config.test_metric_name("disk_io"),
            "datapoints": [[now, 1024]],
            "tags": {
                "host": "server-02",
                "device": "/dev/sda1"
            }
        }
    ]);

    println!("📤 Sending batch of metrics to ingest service...");

    let ingest_response = config
        .client
        .post(format!("{}/api/v1/datapoints", INGEST_BASE_URL))
        .header("Content-Type", "application/json")
        .json(&batch_payload)
        .send()
        .await
        .expect("Failed to send batch to ingest service");

    assert!(
        ingest_response.status().is_success(),
        "Failed to ingest batch: {} - {}",
        ingest_response.status(),
        ingest_response.text().await.unwrap_or_default()
    );

    println!("✅ Batch sent successfully to ingest service");

    // Step 2: Wait for data to propagate
    println!("⏳ Waiting for batch data to propagate...");
    sleep(Duration::from_secs(5)).await;

    // Step 3: Query each metric and verify data exists
    for metric_suffix in ["cpu_usage", "memory_usage", "disk_io"] {
        let metric_name = config.test_metric_name(metric_suffix);

        println!("📥 Querying metric: {}", metric_name);

        let query_payload = json!({
            "start_relative": {
                "value": 1,
                "unit": "hours"
            },
            "metrics": [{
                "name": metric_name
            }]
        });

        let query_response = config
            .client
            .post(format!(
                "{}/api/v1/datapoints/query",
                JAVA_KAIROSDB_BASE_URL
            ))
            .header("Content-Type", "application/json")
            .json(&query_payload)
            .send()
            .await
            .unwrap_or_else(|_| panic!("Failed to query metric {}", metric_name));

        assert!(
            query_response.status().is_success(),
            "Failed to query metric {}: {}",
            metric_name,
            query_response.status()
        );

        let query_result: Value = query_response
            .json()
            .await
            .unwrap_or_else(|_| panic!("Failed to parse response for metric {}", metric_name));

        let queries = query_result["queries"].as_array().unwrap();
        assert!(
            !queries.is_empty(),
            "Query should return results for {}",
            metric_name
        );

        let results = queries[0]["results"].as_array().unwrap();
        assert!(
            !results.is_empty(),
            "Should have results for {}",
            metric_name
        );

        let values = results[0]["values"].as_array().unwrap();
        assert!(!values.is_empty(), "Should have values for {}", metric_name);

        println!("✅ Verified metric: {}", metric_name);
    }

    println!("✅ Batch end-to-end test passed: All metrics successfully processed");
}

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_data_flow_with_complex_tags() {
    let config = E2ETestConfig::new();
    let metric_name = config.test_metric_name("complex_tags");
    let now = Utc::now().timestamp_millis();

    println!("🧪 Testing data flow with complex tag combinations");

    // Test with multiple tags and special characters
    let ingest_payload = json!([{
        "name": metric_name,
        "datapoints": [[now, 100.0]],
        "tags": {
            "environment": "production",
            "service": "api-gateway",
            "version": "1.2.3",
            "region": "us-west-2",
            "instance": "i-1234567890abcdef0",
            "team": "platform"
        }
    }]);

    println!("📤 Sending data with complex tags...");

    let ingest_response = config
        .client
        .post(format!("{}/api/v1/datapoints", INGEST_BASE_URL))
        .header("Content-Type", "application/json")
        .json(&ingest_payload)
        .send()
        .await
        .expect("Failed to send data with complex tags");

    assert!(
        ingest_response.status().is_success(),
        "Failed to ingest data with complex tags"
    );

    println!("✅ Data with complex tags sent successfully");

    // Wait for propagation
    sleep(Duration::from_secs(5)).await;

    // Query with tag filtering
    let query_payload = json!({
        "start_relative": {
            "value": 1,
            "unit": "hours"
        },
        "metrics": [{
            "name": metric_name,
            "tags": {
                "environment": ["production"],
                "service": ["api-gateway"]
            }
        }]
    });

    println!("📥 Querying with tag filters...");

    let query_response = config
        .client
        .post(format!(
            "{}/api/v1/datapoints/query",
            JAVA_KAIROSDB_BASE_URL
        ))
        .header("Content-Type", "application/json")
        .json(&query_payload)
        .send()
        .await
        .expect("Failed to query with tag filters");

    assert!(
        query_response.status().is_success(),
        "Failed to query with tag filters"
    );

    let query_result: Value = query_response
        .json()
        .await
        .expect("Failed to parse tag query response");

    let queries = query_result["queries"]
        .as_array()
        .expect("Response should contain queries");
    assert!(
        !queries.is_empty(),
        "Tag filtered query should return results"
    );

    println!("✅ Complex tags test passed: Data found with tag filtering");
}
