//! Discover available Java KairosDB metrics to find queue-related ones

use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let client = Client::new();
    let java_url = "http://localhost:8080";
    
    info!("🔍 Discovering Java KairosDB metrics...");
    
    // Query for all metrics that might be queue-related
    let search_terms = vec![
        "queue", "write", "batch", "datastore", "kairosdb", "ingest", "buffer"
    ];
    
    for search_term in search_terms {
        info!("🔍 Searching for metrics containing '{}'...", search_term);
        
        let query_payload = serde_json::json!({
            "start_relative": {"value": "1", "unit": "hours"},
            "metrics": [{
                "name": format!(".*{}.*", search_term),
                "aggregators": [{
                    "name": "last",
                    "sampling": {"value": "10", "unit": "minutes"}
                }]
            }]
        });
        
        match client
            .post(&format!("{}/api/v1/datapoints/query", java_url))
            .header("Content-Type", "application/json")
            .timeout(Duration::from_secs(10))
            .json(&query_payload)
            .send()
            .await
        {
            Ok(response) => {
                if response.status().is_success() {
                    if let Ok(result) = response.json::<Value>().await {
                        extract_metric_names(&result, search_term);
                    }
                } else {
                    info!("⚠️  Search for '{}' returned {}", search_term, response.status());
                }
            }
            Err(e) => {
                info!("⚠️  Search for '{}' failed: {}", search_term, e);
            }
        }
    }
    
    // Also try to list all metric names if Java KairosDB supports it
    info!("🔍 Trying to list all metric names...");
    match client
        .get(&format!("{}/api/v1/metricnames", java_url))
        .timeout(Duration::from_secs(10))
        .send()
        .await
    {
        Ok(response) => {
            if response.status().is_success() {
                if let Ok(result) = response.json::<Value>().await {
                    if let Some(results) = result["results"].as_array() {
                        info!("📋 Found {} metric names total", results.len());
                        for result in results {
                            if let Some(name) = result.as_str() {
                                if name.to_lowercase().contains("queue") || 
                                   name.to_lowercase().contains("write") ||
                                   name.to_lowercase().contains("batch") ||
                                   name.to_lowercase().contains("buffer") {
                                    info!("📊 Queue-related metric: {}", name);
                                }
                            }
                        }
                    }
                }
            } else {
                info!("⚠️  Metric names query returned {}", response.status());
            }
        }
        Err(e) => {
            info!("⚠️  Metric names query failed: {}", e);
        }
    }

    Ok(())
}

fn extract_metric_names(result: &Value, search_term: &str) {
    if let Some(queries) = result["queries"].as_array() {
        for query in queries {
            if let Some(results) = query["results"].as_array() {
                for result in results {
                    if let Some(name) = result["name"].as_str() {
                        info!("📊 Found metric ({}): {}", search_term, name);
                        
                        // Also show latest value if available
                        if let Some(values) = result["values"].as_array() {
                            if let Some(last_value) = values.last() {
                                if let Some(value_array) = last_value.as_array() {
                                    if value_array.len() >= 2 {
                                        info!("  └─ Latest value: {}", value_array[1]);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}