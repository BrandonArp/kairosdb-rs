//! Debug script for testing histogram formats with Java KairosDB
//! 
//! This script generates individual histogram payloads and tests them
//! against Java KairosDB to identify format compatibility issues.

use reqwest::Client;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;
use tokio;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let client = Client::new();
    let java_url = "http://localhost:8080/api/v1/datapoints";

    // Test different histogram formats
    test_kairosdb_bins_format(&client, java_url).await?;
    test_prometheus_format(&client, java_url).await?;
    test_direct_format(&client, java_url).await?;
    test_simple_numeric(&client, java_url).await?;

    Ok(())
}

async fn test_kairosdb_bins_format(client: &Client, url: &str) -> Result<(), Box<dyn std::error::Error>> {
    info!("Testing KairosDB bins format...");
    
    // Create bins object in KairosDB format: {"0.1": 10, "1.0": 20, ...}
    let mut bins_obj = serde_json::Map::new();
    bins_obj.insert("0.1".to_string(), json!(5));
    bins_obj.insert("0.5".to_string(), json!(10));
    bins_obj.insert("1.0".to_string(), json!(8));
    bins_obj.insert("5.0".to_string(), json!(2));

    let histogram_value = json!({
        "bins": bins_obj,
        "sum": 15.5,
        "min": 0.05,
        "max": 4.8,
        "mean": 0.62,
        "precision": 7
    });

    let payload = json!([{
        "name": "test.histogram.bins",
        "datapoints": [[chrono::Utc::now().timestamp_millis(), histogram_value]],
        "tags": {"test": "bins_format"}
    }]);

    test_payload(client, url, &payload, "KairosDB bins format").await
}

async fn test_prometheus_format(client: &Client, url: &str) -> Result<(), Box<dyn std::error::Error>> {
    info!("Testing Prometheus buckets format...");
    
    let histogram_value = json!({
        "buckets": [
            {"le": 0.1, "count": 5},
            {"le": 0.5, "count": 15}, // Cumulative
            {"le": 1.0, "count": 23}, // Cumulative
            {"le": 5.0, "count": 25}  // Cumulative
        ],
        "count": 25,
        "sum": 15.5
    });

    let payload = json!([{
        "name": "test.histogram.prometheus",
        "datapoints": [[chrono::Utc::now().timestamp_millis(), histogram_value]],
        "tags": {"test": "prometheus_format"}
    }]);

    test_payload(client, url, &payload, "Prometheus buckets format").await
}

async fn test_direct_format(client: &Client, url: &str) -> Result<(), Box<dyn std::error::Error>> {
    info!("Testing direct boundaries format...");
    
    let histogram_value = json!({
        "boundaries": [0.1, 0.5, 1.0, 5.0],
        "counts": [5, 10, 8, 2],
        "total_count": 25,
        "sum": 15.5,
        "min": 0.05,
        "max": 4.8
    });

    let payload = json!([{
        "name": "test.histogram.direct",
        "datapoints": [[chrono::Utc::now().timestamp_millis(), histogram_value]],
        "tags": {"test": "direct_format"}
    }]);

    test_payload(client, url, &payload, "Direct boundaries format").await
}

async fn test_simple_numeric(client: &Client, url: &str) -> Result<(), Box<dyn std::error::Error>> {
    info!("Testing simple numeric value...");
    
    let payload = json!([{
        "name": "test.simple.numeric",
        "datapoints": [[chrono::Utc::now().timestamp_millis(), 42.5]],
        "tags": {"test": "simple_numeric"}
    }]);

    test_payload(client, url, &payload, "Simple numeric value").await
}

async fn test_payload(
    client: &Client, 
    url: &str, 
    payload: &Value, 
    description: &str
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Sending payload: {}", description);
    info!("JSON: {}", serde_json::to_string_pretty(payload)?);
    
    let response = client
        .post(url)
        .header("Content-Type", "application/json")
        .timeout(Duration::from_secs(5))
        .json(payload)
        .send()
        .await?;
    
    let status = response.status();
    let response_text = response.text().await?;
    
    if status.is_success() {
        info!("✅ {}: SUCCESS ({})", description, status);
    } else {
        error!("❌ {}: FAILED ({})", description, status);
        error!("Response: {}", response_text);
    }
    
    println!("---");
    Ok(())
}