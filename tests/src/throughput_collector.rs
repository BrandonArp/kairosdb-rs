//! Throughput metrics collector for comparing Rust vs Java KairosDB performance
//!
//! Collects real-time metrics from both services during performance tests to measure:
//! 1. Ingest rate (HTTP requests -> disk queue) 
//! 2. End-to-end throughput (HTTP requests -> Cassandra)

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::interval;
use tracing::{debug, error, warn};

/// Throughput measurement results for both ingest and end-to-end processing  
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputMetrics {
    pub timestamp: u64,
    pub service_name: String,
    
    // Ingest throughput (HTTP -> Queue)
    pub ingest_rate_datapoints_per_sec: f64,
    pub ingest_rate_requests_per_sec: f64,
    pub ingest_requests_total: u64,
    pub ingest_datapoints_total: u64,
    
    // End-to-end throughput (HTTP -> Cassandra)
    pub end_to_end_rate_datapoints_per_sec: f64,
    pub cassandra_writes_total: u64,
    pub cassandra_write_errors: u64,
    
    // Queue metrics (shows processing lag)
    pub queue_size: u64,
    pub queue_oldest_entry_age_seconds: f64,
    
    // Service health
    pub success_rate_percent: f64,
    pub error_count: u64,
}

/// Configuration for throughput collection
#[derive(Debug, Clone)]
pub struct ThroughputCollectorConfig {
    pub rust_service_url: String,
    pub java_service_url: String,
    pub collection_interval: Duration,
    pub collection_timeout: Duration,
}

impl Default for ThroughputCollectorConfig {
    fn default() -> Self {
        Self {
            rust_service_url: "http://localhost:8081".to_string(),
            java_service_url: "http://localhost:8080".to_string(),
            collection_interval: Duration::from_secs(5), // Collect every 5 seconds
            collection_timeout: Duration::from_secs(3),   // 3 second timeout per request
        }
    }
}

/// Collects throughput metrics from both Rust and Java KairosDB services
#[derive(Clone)]
pub struct ThroughputCollector {
    client: Client,
    config: ThroughputCollectorConfig,
    
    // Cached previous values for rate calculation
    rust_previous: Option<RustRawMetrics>,
    java_previous: Option<JavaRawMetrics>,
    
    last_collection_time: Option<Instant>,
}

/// Raw metrics from Rust service (via JSON endpoint)
#[derive(Debug, Clone, Deserialize)]
pub struct RustRawMetrics {
    pub datapoints_ingested: u64,
    pub batches_processed: u64,
    pub cassandra_errors: u64,
    pub queue_size: usize,
    pub avg_batch_time_ms: u64,
    pub ingestion_errors: u64,
}

/// Raw metrics from Java service (via KairosDB query API)
#[derive(Debug, Clone, Default)]
pub struct JavaRawMetrics {
    pub ingest_count: u64,
    pub ingest_time_total: u64,
    pub cassandra_write_count: u64,
    pub cassandra_write_batch_size_total: u64,
}

#[derive(Debug, Deserialize)]
struct KairosDbQueryResponse {
    queries: Vec<KairosDbQuery>,
}

#[derive(Debug, Deserialize)]
struct KairosDbQuery {
    results: Vec<KairosDbMetric>,
}

#[derive(Debug, Deserialize)]
struct KairosDbMetric {
    name: String,
    values: Vec<(u64, f64)>, // [(timestamp, value), ...]
}

impl ThroughputCollector {
    pub fn new(config: ThroughputCollectorConfig) -> Self {
        let client = Client::builder()
            .timeout(config.collection_timeout)
            .build()
            .unwrap();
            
        Self {
            client,
            config,
            rust_previous: None,
            java_previous: None,
            last_collection_time: None,
        }
    }
    
    /// Start collecting metrics at configured intervals until stopped
    pub async fn start_background_collection(
        &mut self,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<Vec<ThroughputMetrics>> {
        let mut metrics_history = Vec::new();
        let mut collection_interval = interval(self.config.collection_interval);
        collection_interval.tick().await; // Skip first tick
        
        loop {
            tokio::select! {
                _ = collection_interval.tick() => {
                    match self.collect_current_metrics().await {
                        Ok(mut metrics) => {
                            metrics_history.append(&mut metrics);
                        }
                        Err(e) => {
                            error!("Failed to collect throughput metrics: {}", e);
                        }
                    }
                }
                _ = &mut shutdown_rx => {
                    debug!("Throughput collection shutdown requested");
                    break;
                }
            }
        }
        
        Ok(metrics_history)
    }
    
    /// Collect current metrics from both services
    pub async fn collect_current_metrics(&mut self) -> Result<Vec<ThroughputMetrics>> {
        let now = Instant::now();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Collect from both services in parallel
        let (rust_result, java_result) = tokio::join!(
            self.collect_rust_metrics(),
            self.collect_java_metrics()
        );
        
        let mut metrics = Vec::new();
        
        // Process Rust metrics
        match rust_result {
            Ok(rust_raw) => {
                let rust_throughput = self.calculate_rust_throughput(&rust_raw, timestamp, now);
                metrics.push(rust_throughput);
                self.rust_previous = Some(rust_raw);
            }
            Err(e) => {
                warn!("Failed to collect Rust metrics: {}", e);
            }
        }
        
        // Process Java metrics  
        match java_result {
            Ok(java_raw) => {
                let java_throughput = self.calculate_java_throughput(&java_raw, timestamp, now);
                metrics.push(java_throughput);
                self.java_previous = Some(java_raw);
            }
            Err(e) => {
                warn!("Failed to collect Java metrics: {}", e);
            }
        }
        
        self.last_collection_time = Some(now);
        Ok(metrics)
    }
    
    /// Collect raw metrics from Rust service
    async fn collect_rust_metrics(&self) -> Result<RustRawMetrics> {
        let url = format!("{}/api/v1/metrics", self.config.rust_service_url);
        let response = self.client
            .get(&url)
            .send()
            .await
            .context("Failed to request Rust metrics")?;
            
        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Rust metrics returned {}", response.status()));
        }
        
        let metrics: RustRawMetrics = response
            .json()
            .await
            .context("Failed to parse Rust metrics JSON")?;
            
        debug!("Collected Rust metrics: queue_size={}, datapoints_ingested={}", 
               metrics.queue_size, metrics.datapoints_ingested);
               
        Ok(metrics)
    }
    
    /// Collect raw metrics from Java service via KairosDB query API
    async fn collect_java_metrics(&self) -> Result<JavaRawMetrics> {
        let mut java_metrics = JavaRawMetrics::default();
        
        // Query for http/ingest_count.count (total ingest requests)
        if let Ok(value) = self.query_java_metric("http/ingest_count.count").await {
            java_metrics.ingest_count = value as u64;
        }
        
        // Query for cassandra write batch size count (approximation of writes)
        if let Ok(value) = self.query_java_metric("datastore/cassandra/data_points/write_batch_size.count").await {
            java_metrics.cassandra_write_count = value as u64;
        }
        
        // Query for total write batch size (sum of all batch sizes)
        if let Ok(value) = self.query_java_metric("datastore/cassandra/data_points/write_batch_size.sum").await {
            java_metrics.cassandra_write_batch_size_total = value as u64;
        }
        
        debug!("Collected Java metrics: ingest_count={}, cassandra_write_count={}", 
               java_metrics.ingest_count, java_metrics.cassandra_write_count);
               
        Ok(java_metrics)
    }
    
    /// Query a specific metric from Java KairosDB
    async fn query_java_metric(&self, metric_name: &str) -> Result<f64> {
        let query = format!(r#"{{
            "start_relative": {{"value": "1", "unit": "hours"}},
            "metrics": [{{
                "name": "{}",
                "aggregators": [{{
                    "name": "max",
                    "sampling": {{"value": 1, "unit": "hours"}}
                }}]
            }}]
        }}"#, metric_name);
        
        let url = format!("{}/api/v1/datapoints/query", self.config.java_service_url);
        let response = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(query)
            .send()
            .await
            .context("Failed to query Java metrics")?;
            
        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Java query returned {}", response.status()));
        }
        
        let query_response: KairosDbQueryResponse = response
            .json()
            .await
            .context("Failed to parse Java query response")?;
            
        // Extract latest value from response
        for query in query_response.queries {
            for metric in query.results {
                if let Some((_, value)) = metric.values.into_iter().last() {
                    return Ok(value);
                }
            }
        }
        
        Ok(0.0) // Default to 0 if no data found
    }
    
    /// Calculate throughput metrics for Rust service
    fn calculate_rust_throughput(
        &self,
        current: &RustRawMetrics,
        timestamp: u64,
        now: Instant,
    ) -> ThroughputMetrics {
        let (ingest_rate_dps, ingest_rate_rps, e2e_rate_dps) = if let (Some(previous), Some(last_time)) = 
            (&self.rust_previous, self.last_collection_time) {
            
            let elapsed_secs = now.duration_since(last_time).as_secs_f64();
            if elapsed_secs > 0.0 {
                let datapoints_delta = current.datapoints_ingested.saturating_sub(previous.datapoints_ingested);
                let batches_delta = current.batches_processed.saturating_sub(previous.batches_processed);
                
                let ingest_rate_dps = datapoints_delta as f64 / elapsed_secs;
                let ingest_rate_rps = batches_delta as f64 / elapsed_secs;
                let e2e_rate_dps = ingest_rate_dps; // For Rust, assume processing keeps up
                
                (ingest_rate_dps, ingest_rate_rps, e2e_rate_dps)
            } else {
                (0.0, 0.0, 0.0)
            }
        } else {
            (0.0, 0.0, 0.0)
        };
        
        let success_rate = if current.batches_processed > 0 {
            ((current.batches_processed - current.ingestion_errors) as f64 / current.batches_processed as f64) * 100.0
        } else {
            100.0
        };
        
        ThroughputMetrics {
            timestamp,
            service_name: "Rust".to_string(),
            ingest_rate_datapoints_per_sec: ingest_rate_dps,
            ingest_rate_requests_per_sec: ingest_rate_rps,
            ingest_requests_total: current.batches_processed,
            ingest_datapoints_total: current.datapoints_ingested,
            end_to_end_rate_datapoints_per_sec: e2e_rate_dps,
            cassandra_writes_total: current.datapoints_ingested, // Approximation
            cassandra_write_errors: current.cassandra_errors,
            queue_size: current.queue_size as u64,
            queue_oldest_entry_age_seconds: 0.0, // TODO: Extract from prometheus metrics
            success_rate_percent: success_rate,
            error_count: current.ingestion_errors + current.cassandra_errors,
        }
    }
    
    /// Calculate throughput metrics for Java service
    fn calculate_java_throughput(
        &self,
        current: &JavaRawMetrics,
        timestamp: u64,
        now: Instant,
    ) -> ThroughputMetrics {
        let (ingest_rate_rps, ingest_rate_dps, e2e_rate_dps) = if let (Some(previous), Some(last_time)) = 
            (&self.java_previous, self.last_collection_time) {
            
            let elapsed_secs = now.duration_since(last_time).as_secs_f64();
            if elapsed_secs > 0.0 {
                let ingest_delta = current.ingest_count.saturating_sub(previous.ingest_count);
                let write_delta = current.cassandra_write_batch_size_total.saturating_sub(previous.cassandra_write_batch_size_total);
                
                let ingest_rate_rps = ingest_delta as f64 / elapsed_secs;
                let ingest_rate_dps = write_delta as f64 / elapsed_secs; // Use cassandra write total as datapoints ingested
                let e2e_rate_dps = write_delta as f64 / elapsed_secs;
                
                (ingest_rate_rps, ingest_rate_dps, e2e_rate_dps)
            } else {
                (0.0, 0.0, 0.0)
            }
        } else {
            (0.0, 0.0, 0.0)
        };
        
        ThroughputMetrics {
            timestamp,
            service_name: "Java".to_string(),
            ingest_rate_datapoints_per_sec: ingest_rate_dps,
            ingest_rate_requests_per_sec: ingest_rate_rps,
            ingest_requests_total: current.ingest_count,
            ingest_datapoints_total: current.cassandra_write_batch_size_total,
            end_to_end_rate_datapoints_per_sec: e2e_rate_dps,
            cassandra_writes_total: current.cassandra_write_count,
            cassandra_write_errors: 0, // TODO: Query error metrics
            queue_size: 0, // Java queue metrics not easily accessible
            queue_oldest_entry_age_seconds: 0.0,
            success_rate_percent: 100.0, // TODO: Calculate from error metrics
            error_count: 0,
        }
    }
    
    /// Get a one-shot metrics sample from both services
    pub async fn collect_snapshot(&mut self) -> Result<Vec<ThroughputMetrics>> {
        self.collect_current_metrics().await
    }
}

/// Helper to format throughput metrics for display
impl ThroughputMetrics {
    pub fn format_summary(&self) -> String {
        format!(
            "{} Service: {:.1} req/s, {:.1} dps/s ingested, {:.1} dps/s to Cassandra, Queue: {}",
            self.service_name,
            self.ingest_rate_requests_per_sec,
            self.ingest_rate_datapoints_per_sec,
            self.end_to_end_rate_datapoints_per_sec,
            self.queue_size
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_throughput_metrics_creation() {
        let metrics = ThroughputMetrics {
            timestamp: 1000,
            service_name: "Test".to_string(),
            ingest_rate_datapoints_per_sec: 100.0,
            ingest_rate_requests_per_sec: 10.0,
            ingest_requests_total: 1000,
            ingest_datapoints_total: 10000,
            end_to_end_rate_datapoints_per_sec: 95.0,
            cassandra_writes_total: 9500,
            cassandra_write_errors: 5,
            queue_size: 50,
            queue_oldest_entry_age_seconds: 2.5,
            success_rate_percent: 99.5,
            error_count: 5,
        };
        
        assert_eq!(metrics.service_name, "Test");
        assert_eq!(metrics.ingest_rate_datapoints_per_sec, 100.0);
    }
    
    #[tokio::test]
    async fn test_collector_creation() {
        let config = ThroughputCollectorConfig::default();
        let collector = ThroughputCollector::new(config);
        assert_eq!(collector.config.rust_service_url, "http://localhost:8081");
    }
}