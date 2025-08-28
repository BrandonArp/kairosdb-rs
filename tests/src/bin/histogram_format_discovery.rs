//! Format discovery script - send histogram to Rust ingest, read back from Java KairosDB
//! This will show us the exact format Java KairosDB uses for histogram data.

use reqwest::Client;
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let client = Client::new();
    let rust_ingest_url = "http://localhost:8081/api/v1/datapoints";
    let java_query_url = "http://localhost:8080/api/v1/datapoints/query";

    let metric_name = "test.histogram.format.discovery";
    let timestamp = chrono::Utc::now().timestamp_millis();

    // Step 1: Send histogram to Rust ingest service using working format
    info!("Step 1: Sending histogram to Rust ingest service...");
    let histogram_payload = json!([{
        "name": metric_name,
        "datapoints": [[timestamp, {
            "boundaries": [0.1, 0.5, 1.0, 5.0, 10.0],
            "counts": [10, 25, 15, 8, 2],
            "total_count": 60,
            "sum": 95.5,
            "min": 0.05,
            "max": 9.8
        }]],
        "tags": {
            "test": "format_discovery"
        }
    }]);

    info!(
        "Sending to Rust: {}",
        serde_json::to_string_pretty(&histogram_payload)?
    );

    let response = client
        .post(rust_ingest_url)
        .header("Content-Type", "application/json")
        .timeout(Duration::from_secs(10))
        .json(&histogram_payload)
        .send()
        .await?;

    if !response.status().is_success() {
        error!(
            "Failed to send to Rust ingest: {} - {}",
            response.status(),
            response.text().await?
        );
        return Ok(());
    }

    info!("✅ Successfully sent histogram to Rust ingest service");

    // Step 2: Wait for data to propagate to Cassandra
    info!("Step 2: Waiting 5 seconds for data to propagate to Cassandra...");
    sleep(Duration::from_secs(5)).await;

    // Step 3: Query histogram data from Java KairosDB
    info!("Step 3: Querying histogram from Java KairosDB...");
    let query_payload = json!({
        "start_relative": {"value": "1", "unit": "hours"},
        "metrics": [{
            "name": metric_name,
            "tags": {
                "test": ["format_discovery"]
            }
        }]
    });

    info!(
        "Query payload: {}",
        serde_json::to_string_pretty(&query_payload)?
    );

    let response = client
        .post(java_query_url)
        .header("Content-Type", "application/json")
        .timeout(Duration::from_secs(10))
        .json(&query_payload)
        .send()
        .await?;

    let status = response.status();
    let response_text = response.text().await?;

    if !status.is_success() {
        error!(
            "Failed to query from Java KairosDB: {} - {}",
            status, response_text
        );
        return Ok(());
    }

    info!("✅ Successfully queried histogram from Java KairosDB");

    // Step 4: Parse and display the histogram format Java KairosDB returns
    let query_result: serde_json::Value = serde_json::from_str(&response_text)?;
    info!(
        "Full response: {}",
        serde_json::to_string_pretty(&query_result)?
    );

    // Extract and analyze the histogram data format
    if let Some(queries) = query_result["queries"].as_array() {
        if let Some(first_query) = queries.first() {
            if let Some(results) = first_query["results"].as_array() {
                if let Some(first_result) = results.first() {
                    if let Some(values) = first_result["values"].as_array() {
                        info!("🔍 Histogram values format from Java KairosDB:");
                        for value in values {
                            if let Some(value_array) = value.as_array() {
                                if value_array.len() >= 2 {
                                    info!("  Timestamp: {}", value_array[0]);
                                    info!(
                                        "  Histogram data: {}",
                                        serde_json::to_string_pretty(&value_array[1])?
                                    );
                                    info!("  Histogram data type: {:?}", value_array[1]);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
