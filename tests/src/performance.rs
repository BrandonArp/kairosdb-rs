use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::info;

pub mod generator;
pub mod scenarios;
pub mod reporter;

pub use generator::*;
pub use scenarios::*;
pub use reporter::*;

/// Performance test configuration
#[derive(Debug, Clone, serde::Serialize)]
pub struct PerfTestConfig {
    pub ingest_url: String,
    pub metrics_count: usize,
    pub tag_combinations_per_metric: usize,
    pub histogram_samples_per_datapoint: (usize, usize), // (min, max) samples
    pub batch_size: usize,
    pub concurrent_batches: usize,
    pub duration_seconds: u64,
    pub tag_cardinality_limit: usize,
    pub warmup_seconds: u64,
}

impl Default for PerfTestConfig {
    fn default() -> Self {
        Self {
            ingest_url: "http://localhost:8081".to_string(),
            metrics_count: 1000,
            tag_combinations_per_metric: 50,
            histogram_samples_per_datapoint: (10, 1000),
            batch_size: 100,
            concurrent_batches: 10,
            duration_seconds: 60,
            tag_cardinality_limit: 20,
            warmup_seconds: 10,
        }
    }
}

/// Performance test results
#[derive(Debug, Clone, serde::Serialize)]
pub struct PerfTestResults {
    pub total_datapoints_sent: u64,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub test_duration: Duration,
    pub throughput_datapoints_per_sec: f64,
    pub throughput_requests_per_sec: f64,
    pub latency_stats: LatencyStats,
    pub error_details: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LatencyStats {
    pub mean_ms: f64,
    pub median_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub min_ms: f64,
    pub max_ms: f64,
}

/// Main performance test runner
pub struct PerfTestRunner {
    config: PerfTestConfig,
    client: Client,
    generator: MetricDataGenerator,
    semaphore: Arc<Semaphore>,
}

impl PerfTestRunner {
    pub fn new(config: PerfTestConfig) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");
        
        let generator = MetricDataGenerator::new(GeneratorConfig {
            metrics_count: config.metrics_count,
            tag_combinations_per_metric: config.tag_combinations_per_metric,
            histogram_samples_range: config.histogram_samples_per_datapoint,
            tag_cardinality_limit: config.tag_cardinality_limit,
        });

        let semaphore = Arc::new(Semaphore::new(config.concurrent_batches));

        Self {
            config,
            client,
            generator,
            semaphore,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<PerfTestResults> {
        info!("Starting performance test with config: {:?}", self.config);
        
        // Warmup phase
        if self.config.warmup_seconds > 0 {
            info!("Warming up for {} seconds...", self.config.warmup_seconds);
            self.warmup().await?;
        }

        // Main test phase
        info!("Starting main test phase for {} seconds", self.config.duration_seconds);
        let results = self.run_main_test().await?;
        
        info!("Performance test completed. Results: {:?}", results);
        Ok(results)
    }

    async fn warmup(&self) -> anyhow::Result<()> {
        let warmup_duration = Duration::from_secs(self.config.warmup_seconds);
        let start = Instant::now();
        
        while start.elapsed() < warmup_duration {
            let batch = self.generator.generate_batch(self.config.batch_size / 4).await;
            let _ = self.send_batch(&batch).await;
            sleep(Duration::from_millis(100)).await;
        }
        
        Ok(())
    }

    async fn run_main_test(&self) -> anyhow::Result<PerfTestResults> {
        let mut results = PerfTestResults {
            total_datapoints_sent: 0,
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            test_duration: Duration::default(),
            throughput_datapoints_per_sec: 0.0,
            throughput_requests_per_sec: 0.0,
            latency_stats: LatencyStats {
                mean_ms: 0.0,
                median_ms: 0.0,
                p95_ms: 0.0,
                p99_ms: 0.0,
                min_ms: 0.0,
                max_ms: 0.0,
            },
            error_details: Vec::new(),
        };

        let test_duration = Duration::from_secs(self.config.duration_seconds);
        let start = Instant::now();
        let mut latencies = Vec::new();
        let mut handles = Vec::new();

        while start.elapsed() < test_duration {
            let batch = self.generator.generate_batch(self.config.batch_size).await;
            let datapoint_count = batch.iter().map(|dp| dp.len()).sum::<usize>() as u64;
            
            let semaphore = self.semaphore.clone();
            let client = self.client.clone();
            let ingest_url = self.config.ingest_url.clone();
            
            let handle = tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                let req_start = Instant::now();
                
                match Self::send_batch_internal(&client, &ingest_url, &batch).await {
                    Ok(_) => (true, req_start.elapsed(), datapoint_count, None),
                    Err(e) => (false, req_start.elapsed(), datapoint_count, Some(e.to_string())),
                }
            });
            
            handles.push(handle);
            
            // Prevent overwhelming the system
            sleep(Duration::from_millis(10)).await;
        }

        // Collect all results
        for handle in handles {
            if let Ok((success, latency, dp_count, error)) = handle.await {
                results.total_requests += 1;
                results.total_datapoints_sent += dp_count;
                latencies.push(latency.as_secs_f64() * 1000.0);
                
                if success {
                    results.successful_requests += 1;
                } else {
                    results.failed_requests += 1;
                    if let Some(err) = error {
                        results.error_details.push(err);
                    }
                }
            }
        }

        results.test_duration = start.elapsed();
        results.throughput_datapoints_per_sec = 
            results.total_datapoints_sent as f64 / results.test_duration.as_secs_f64();
        results.throughput_requests_per_sec = 
            results.total_requests as f64 / results.test_duration.as_secs_f64();
        
        // Calculate latency statistics
        if !latencies.is_empty() {
            latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
            results.latency_stats = LatencyStats {
                mean_ms: latencies.iter().sum::<f64>() / latencies.len() as f64,
                median_ms: latencies[latencies.len() / 2],
                p95_ms: latencies[(latencies.len() as f64 * 0.95) as usize],
                p99_ms: latencies[(latencies.len() as f64 * 0.99) as usize],
                min_ms: latencies[0],
                max_ms: latencies[latencies.len() - 1],
            };
        }

        Ok(results)
    }

    async fn send_batch(&self, batch: &[Vec<Value>]) -> anyhow::Result<()> {
        Self::send_batch_internal(&self.client, &self.config.ingest_url, batch).await
    }

    async fn send_batch_internal(
        client: &Client,
        ingest_url: &str,
        batch: &[Vec<Value>],
    ) -> anyhow::Result<()> {
        let payload: Vec<Value> = batch.iter().flatten().cloned().collect();
        
        let response = client
            .post(&format!("{}/api/v1/datapoints", ingest_url))
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Ingest request failed: {} - {}", status, body);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_perf_config_default() {
        let config = PerfTestConfig::default();
        assert_eq!(config.metrics_count, 1000);
        assert_eq!(config.batch_size, 100);
    }
}