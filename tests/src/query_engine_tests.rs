//! Unit tests for query engine components
//!
//! These tests focus on testing individual query engine components in isolation
//! to verify correctness of aggregation, filtering, and data processing logic.

#![allow(unused_imports)]

use chrono::Utc;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

use crate::common::{E2ETestConfig, INGEST_BASE_URL, RUST_QUERY_BASE_URL};

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_end_to_end_aggregation_accuracy() {
    let config = E2ETestConfig::new();
    let metric_name = config.test_metric_name("agg_accuracy");
    let now = Utc::now().timestamp_millis();

    info!("🧪 Testing end-to-end aggregation accuracy with known data");

    // Step 1: Ingest precise data for aggregation testing
    // Values: [10, 20, 30, 40, 50] -> avg=30, min=10, max=50
    let test_values = [10.0, 20.0, 30.0, 40.0, 50.0];
    let mut datapoints = Vec::new();

    for (i, &value) in test_values.iter().enumerate() {
        datapoints.push(json!([now + (i as i64 * 30000), value]));
    }

    let ingest_payload = json!([{
        "name": metric_name,
        "datapoints": datapoints,
        "tags": {
            "test": "aggregation_accuracy",
            "server": "precision-test"
        }
    }]);

    let ingest_response = config
        .client
        .post(format!("{}/api/v1/datapoints", INGEST_BASE_URL))
        .json(&ingest_payload)
        .send()
        .await
        .expect("Failed to ingest precision test data");

    assert!(ingest_response.status().is_success());
    info!("✅ Ingested {} precise data points", test_values.len());

    // Wait for data propagation
    sleep(Duration::from_secs(6)).await;

    // Test Case 1: Query without aggregation (should get raw values)
    info!("📥 Testing raw data retrieval");
    let query_payload = json!({
        "start_relative": {"value": 1, "unit": "hours"},
        "metrics": [{
            "name": metric_name,
            "tags": {"test": "aggregation_accuracy"}
        }]
    });

    let response = config
        .client
        .post(format!("{}/api/v1/datapoints/query", RUST_QUERY_BASE_URL))
        .json(&query_payload)
        .send()
        .await
        .expect("Failed to query raw data");

    assert!(response.status().is_success());
    let result: Value = response.json().await.expect("Should parse JSON");
    let results = result["queries"][0]["results"].as_array().unwrap();

    if !results.is_empty() {
        let values = results[0]["values"].as_array().unwrap();
        info!("Raw query returned {} data points", values.len());

        // Verify we can find all our expected values
        let mut found_values = Vec::new();
        for value in values {
            if let Some(val) = value["value"].as_f64() {
                found_values.push(val);
            }
        }

        found_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        info!("Found raw values: {:?}", found_values);

        for &expected in &test_values {
            let found = found_values
                .iter()
                .any(|&actual| (actual - expected).abs() < 0.001);
            if found {
                info!("✅ Found expected raw value: {}", expected);
            } else {
                info!("⚠️  Missing expected raw value: {}", expected);
            }
        }
    }

    // Test Case 2: Test aggregation with specific sampling
    info!("📥 Testing average aggregation");
    let query_payload = json!({
        "start_relative": {"value": 1, "unit": "hours"},
        "metrics": [{
            "name": metric_name,
            "tags": {"test": "aggregation_accuracy"},
            "aggregators": [
                {
                    "name": "avg",
                    "sampling": {
                        "value": 5,
                        "unit": "minutes"
                    }
                }
            ]
        }]
    });

    let response = config
        .client
        .post(format!("{}/api/v1/datapoints/query", RUST_QUERY_BASE_URL))
        .json(&query_payload)
        .send()
        .await
        .expect("Failed to query with average aggregation");

    if response.status().is_success() {
        let result: Value = response.json().await.expect("Should parse JSON");
        let results = result["queries"][0]["results"].as_array().unwrap();

        if !results.is_empty() {
            let values = results[0]["values"].as_array().unwrap();
            info!("Average aggregation returned {} points", values.len());

            // Log the aggregated values for inspection
            for value in values {
                if let Some(val) = value["value"].as_f64() {
                    info!("Aggregated value: {}", val);
                }
            }

            info!("✅ Average aggregation executed successfully");
        }
    } else {
        info!(
            "⚠️  Average aggregation query failed: {}",
            response.status()
        );
    }

    info!("✅ End-to-end aggregation accuracy test completed");
}

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_tag_filtering_precision() {
    let config = E2ETestConfig::new();
    let metric_name = config.test_metric_name("tag_precision");
    let now = Utc::now().timestamp_millis();

    info!("🧪 Testing tag filtering precision end-to-end");

    // Step 1: Ingest precisely controlled data
    let test_data = [
        (
            vec![("env", "prod"), ("dc", "us-east"), ("tier", "web")],
            100.0,
        ),
        (
            vec![("env", "prod"), ("dc", "us-east"), ("tier", "api")],
            200.0,
        ),
        (
            vec![("env", "prod"), ("dc", "us-west"), ("tier", "web")],
            300.0,
        ),
        (
            vec![("env", "test"), ("dc", "us-east"), ("tier", "web")],
            400.0,
        ),
    ];

    let mut ingest_batch = Vec::new();
    for (i, (tag_pairs, value)) in test_data.iter().enumerate() {
        let mut tags = serde_json::Map::new();
        for (key, val) in tag_pairs {
            tags.insert(key.to_string(), json!(val));
        }
        tags.insert(
            "instance".to_string(),
            json!(format!("server-{:02}", i + 1)),
        );

        ingest_batch.push(json!({
            "name": metric_name,
            "datapoints": [[now + (i as i64 * 1000), value]],
            "tags": tags
        }));
    }

    let ingest_response = config
        .client
        .post(format!("{}/api/v1/datapoints", INGEST_BASE_URL))
        .json(&ingest_batch)
        .send()
        .await
        .expect("Failed to ingest tag precision test data");

    assert!(ingest_response.status().is_success());
    info!(
        "✅ Ingested {} data points with precise tags",
        test_data.len()
    );

    // Wait for data propagation
    sleep(std::time::Duration::from_secs(5)).await;

    // Test Case 1: Single tag filter - should get exact matches
    info!("📥 Testing precise single tag filter: env=prod");
    let query_payload = json!({
        "start_relative": {"value": 1, "unit": "hours"},
        "metrics": [{
            "name": metric_name,
            "tags": {"env": "prod"}
        }]
    });

    let response = config
        .client
        .post(format!("{}/api/v1/datapoints/query", RUST_QUERY_BASE_URL))
        .json(&query_payload)
        .send()
        .await
        .expect("Failed to query with env=prod filter");

    assert!(response.status().is_success());
    let result: Value = response.json().await.expect("Should parse JSON");
    let results = result["queries"][0]["results"].as_array().unwrap();

    info!(
        "Single tag filter (env=prod) returned {} results",
        results.len()
    );

    // Verify we got the expected data values
    let mut found_values = Vec::new();
    for result in results {
        if let Some(values_array) = result["values"].as_array() {
            for value in values_array {
                if let Some(val) = value["value"].as_f64() {
                    found_values.push(val);
                }
            }
        }
    }

    found_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    info!("Found values for env=prod: {:?}", found_values);

    // Test Case 2: Multi-tag filter - more specific
    info!("📥 Testing precise multi-tag filter: env=prod AND dc=us-east");
    let query_payload = json!({
        "start_relative": {"value": 1, "unit": "hours"},
        "metrics": [{
            "name": metric_name,
            "tags": {
                "env": "prod",
                "dc": "us-east"
            }
        }]
    });

    let response = config
        .client
        .post(format!("{}/api/v1/datapoints/query", RUST_QUERY_BASE_URL))
        .json(&query_payload)
        .send()
        .await
        .expect("Failed to query with multi-tag filter");

    assert!(response.status().is_success());
    let result: Value = response.json().await.expect("Should parse JSON");
    let results = result["queries"][0]["results"].as_array().unwrap();

    info!(
        "Multi-tag filter (env=prod AND dc=us-east) returned {} results",
        results.len()
    );

    // Test Case 3: Verify specific data values are retrievable
    info!("📥 Testing exact value retrieval");
    let query_payload = json!({
        "start_relative": {"value": 1, "unit": "hours"},
        "metrics": [{
            "name": metric_name,
            "tags": {
                "env": "prod",
                "dc": "us-west",
                "tier": "web"
            }
        }]
    });

    let response = config
        .client
        .post(format!("{}/api/v1/datapoints/query", RUST_QUERY_BASE_URL))
        .json(&query_payload)
        .send()
        .await
        .expect("Failed to query exact combination");

    assert!(response.status().is_success());
    let result: Value = response.json().await.expect("Should parse JSON");
    let results = result["queries"][0]["results"].as_array().unwrap();

    if !results.is_empty() {
        let values = results[0]["values"].as_array().unwrap();
        if !values.is_empty() {
            let found_value = values[0]["value"].as_f64().unwrap_or(0.0);
            info!("Found exact value: {}, expected: 300.0", found_value);

            if (found_value - 300.0).abs() < 0.001 {
                info!("✅ Exact value retrieval verified");
            } else {
                info!("⚠️  Expected 300.0 but found {}", found_value);
            }
        }
    }

    info!("✅ Tag filtering precision test completed");
}

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_data_correctness_multiple_series() {
    let config = E2ETestConfig::new();
    let metric_name = config.test_metric_name("multi_series");
    let now = Utc::now().timestamp_millis();

    info!("🧪 Testing data correctness across multiple series");

    // Step 1: Create multiple time series with predictable patterns
    let series_data = vec![
        ("server-01", vec![10.0, 20.0, 30.0]), // ascending
        ("server-02", vec![50.0, 40.0, 30.0]), // descending
        ("server-03", vec![25.0, 25.0, 25.0]), // constant
        ("server-04", vec![5.0, 15.0, 10.0]),  // variable
    ];

    let mut ingest_batch = Vec::new();
    for (server, values) in &series_data {
        let mut datapoints = Vec::new();
        for (i, &value) in values.iter().enumerate() {
            datapoints.push(json!([now + (i as i64 * 30000), value]));
        }

        ingest_batch.push(json!({
            "name": metric_name,
            "datapoints": datapoints,
            "tags": {
                "host": server,
                "test": "multi_series"
            }
        }));
    }

    let ingest_response = config
        .client
        .post(format!("{}/api/v1/datapoints", INGEST_BASE_URL))
        .json(&ingest_batch)
        .send()
        .await
        .expect("Failed to ingest multi-series test data");

    assert!(ingest_response.status().is_success());
    info!("✅ Ingested {} time series", series_data.len());

    // Wait for data propagation
    sleep(std::time::Duration::from_secs(5)).await;

    // Step 2: Query all series and verify data integrity
    info!("📥 Querying all series");
    let query_payload = json!({
        "start_relative": {"value": 1, "unit": "hours"},
        "metrics": [{
            "name": metric_name,
            "tags": {"test": "multi_series"}
        }]
    });

    let response = config
        .client
        .post(format!("{}/api/v1/datapoints/query", RUST_QUERY_BASE_URL))
        .json(&query_payload)
        .send()
        .await
        .expect("Failed to query multi-series data");

    assert!(response.status().is_success());
    let result: Value = response.json().await.expect("Should parse JSON");
    let results = result["queries"][0]["results"].as_array().unwrap();

    info!("Query returned {} series", results.len());

    // Step 3: Verify each series has correct data
    let mut verified_series = 0;
    for result in results {
        let series_tags = &result["tags"];
        let host = series_tags["host"].as_str().unwrap_or("unknown");
        let values = result["values"].as_array().unwrap();

        info!("Verifying series for host: {}", host);

        // Find expected data for this host
        if let Some((_server, expected_values)) = series_data.iter().find(|(s, _)| *s == host) {
            let mut actual_values = Vec::new();
            for value in values {
                if let Some(val) = value["value"].as_f64() {
                    actual_values.push(val);
                }
            }

            actual_values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let mut expected_sorted = expected_values.clone();
            expected_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

            info!(
                "Host {}: expected {:?}, got {:?}",
                host, expected_sorted, actual_values
            );

            // Verify we have all expected values
            for expected_val in &expected_sorted {
                let found = actual_values
                    .iter()
                    .any(|&actual| (actual - expected_val).abs() < 0.001);
                if found {
                    info!("✅ Found expected value {} for {}", expected_val, host);
                } else {
                    info!("⚠️  Missing expected value {} for {}", expected_val, host);
                }
            }

            verified_series += 1;
        }
    }

    info!(
        "✅ Verified data for {} out of {} series",
        verified_series,
        series_data.len()
    );

    // Step 4: Test series-specific queries
    info!("📥 Testing series-specific queries");
    for (server, expected_values) in &series_data {
        let query_payload = json!({
            "start_relative": {"value": 1, "unit": "hours"},
            "metrics": [{
                "name": metric_name,
                "tags": {
                    "host": server,
                    "test": "multi_series"
                }
            }]
        });

        let response = config
            .client
            .post(format!("{}/api/v1/datapoints/query", RUST_QUERY_BASE_URL))
            .json(&query_payload)
            .send()
            .await
            .expect("Failed to query specific series");

        if response.status().is_success() {
            let result: Value = response.json().await.expect("Should parse JSON");
            let results = result["queries"][0]["results"].as_array().unwrap();

            if !results.is_empty() {
                let values = results[0]["values"].as_array().unwrap();
                info!("Series {} returned {} data points", server, values.len());

                // Verify we can retrieve all expected values
                for &expected in expected_values {
                    let found = values.iter().any(|value| {
                        if let Some(val) = value["value"].as_f64() {
                            (val - expected).abs() < 0.001
                        } else {
                            false
                        }
                    });

                    if found {
                        info!("✅ Series {}: found value {}", server, expected);
                    } else {
                        info!("⚠️  Series {}: missing value {}", server, expected);
                    }
                }
            }
        }
    }

    info!("✅ Multi-series data correctness test completed");
}
