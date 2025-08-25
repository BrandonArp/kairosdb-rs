use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::sleep;
use tracing::info;

pub mod generator;
pub mod reporter;
pub mod scenarios;

pub use generator::*;
pub use reporter::*;
pub use scenarios::*;

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
    pub performance_mode: Option<String>, // no_parse, parse_only, or parse_and_store
    pub producer_tasks: usize,            // Number of producer tasks for batch generation
}

impl Default for PerfTestConfig {
    fn default() -> Self {
        Self {
            ingest_url: "http://localhost:8081".to_string(),
            metrics_count: 1000,
            tag_combinations_per_metric: 50,
            histogram_samples_per_datapoint: (10, 1000),
            batch_size: 100,
            concurrent_batches: 50, // Default to 50 concurrent requests
            duration_seconds: 60,
            tag_cardinality_limit: 20,
            warmup_seconds: 10,
            performance_mode: None, // Use server default
            producer_tasks: 4,      // Multiple producers to avoid generation bottleneck
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

// Type aliases for complex types to satisfy clippy
type BatchReceiver = Arc<tokio::sync::Mutex<mpsc::Receiver<(Vec<Vec<Value>>, u64)>>>;
type ResultsState = Arc<tokio::sync::Mutex<(u64, u64, u64, Vec<String>)>>;

/// Main performance test runner
#[allow(dead_code)]
pub struct PerfTestRunner {
    config: PerfTestConfig,
    client: Client,
    generator: MetricDataGenerator,
    semaphore: Arc<Semaphore>,
}

impl PerfTestRunner {
    pub fn new(config: PerfTestConfig) -> Self {
        // Validate configuration
        if config.concurrent_batches == 0 {
            panic!("concurrent_batches must be greater than 0");
        }
        if config.concurrent_batches > 1000 {
            tracing::warn!(
                "Very high concurrency setting ({}), this may overwhelm the system",
                config.concurrent_batches
            );
        }

        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(config.concurrent_batches) // Allow more idle connections per host
            .pool_idle_timeout(Duration::from_secs(90)) // Keep connections alive longer
            .http2_keep_alive_interval(Some(Duration::from_secs(30))) // HTTP/2 keepalive
            .http2_keep_alive_timeout(Duration::from_secs(10))
            .tcp_keepalive(Some(Duration::from_secs(60))) // TCP keepalive
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
        info!(
            "Starting main test phase for {} seconds",
            self.config.duration_seconds
        );
        let results = self.run_main_test().await?;

        info!("Performance test completed. Results: {:?}", results);
        Ok(results)
    }

    async fn warmup(&self) -> anyhow::Result<()> {
        let warmup_duration = Duration::from_secs(self.config.warmup_seconds);
        let start = Instant::now();

        while start.elapsed() < warmup_duration {
            let batch = self
                .generator
                .generate_batch(self.config.batch_size / 4)
                .await;
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

        // Create bounded channel for request batches
        let channel_capacity = 5000; // Buffer up to 5k batches
        let (batch_sender, batch_receiver) = mpsc::channel(channel_capacity);

        // Shared state for collecting results
        let latencies = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let results_state = Arc::new(tokio::sync::Mutex::new((
            0u64,
            0u64,
            0u64,
            Vec::<String>::new(),
        ))); // (successful, failed, total_datapoints, errors)

        info!("Starting producer-consumer test with {}s duration, {} producers, {} concurrent workers, {}k channel buffer", 
              test_duration.as_secs(), self.config.producer_tasks, self.config.concurrent_batches, channel_capacity / 1000);

        // Start consumer workers - they'll all compete for messages from the same receiver
        let batch_receiver = Arc::new(tokio::sync::Mutex::new(batch_receiver));
        let mut worker_handles = Vec::new();
        for worker_id in 0..self.config.concurrent_batches {
            let receiver = batch_receiver.clone();
            let client = self.client.clone();
            let ingest_url = self.config.ingest_url.clone();
            let performance_mode = self.config.performance_mode.clone();
            let latencies_clone = latencies.clone();
            let results_clone = results_state.clone();

            let handle = tokio::spawn(async move {
                Self::consumer_worker(
                    worker_id,
                    receiver,
                    client,
                    ingest_url,
                    performance_mode,
                    latencies_clone,
                    results_clone,
                )
                .await;
            });
            worker_handles.push(handle);
        }

        // Start multiple producer tasks
        let mut producer_handles = Vec::new();
        let batch_size_per_producer = self.config.batch_size / self.config.producer_tasks.max(1);

        for producer_id in 0..self.config.producer_tasks {
            let generator_config = self.generator.get_config().clone();
            let sender = batch_sender.clone();
            let producer_batch_size = if producer_id == 0 {
                // First producer takes remainder if not evenly divisible
                batch_size_per_producer + (self.config.batch_size % self.config.producer_tasks)
            } else {
                batch_size_per_producer
            };

            let handle = tokio::spawn(async move {
                let generator =
                    crate::performance::generator::MetricDataGenerator::new(generator_config);
                Self::producer_worker(
                    producer_id,
                    generator,
                    producer_batch_size,
                    test_duration,
                    sender,
                )
                .await
            });
            producer_handles.push(handle);
        }

        // Drop the original sender so channel closes when all producers finish
        drop(batch_sender);

        // Wait for all producers to complete
        let mut total_batches = 0;
        for (i, handle) in producer_handles.into_iter().enumerate() {
            match handle.await {
                Ok(batches) => {
                    total_batches += batches;
                    info!("Producer {} completed: generated {} batches", i, batches);
                }
                Err(e) => {
                    tracing::error!("Producer {} failed: {}", i, e);
                }
            }
        }

        info!(
            "All producers completed: generated {} total batches",
            total_batches
        );

        // Give workers a bit of time to finish processing remaining batches
        let _collection_timeout = Duration::from_secs(30);
        tokio::time::sleep(Duration::from_secs(2)).await; // Brief pause to let workers catch up

        // Cancel remaining workers
        for handle in worker_handles {
            handle.abort();
        }

        // Collect final results
        let final_latencies = latencies.lock().await.clone();
        let (successful, failed, total_datapoints, errors) = {
            let state = results_state.lock().await;
            (state.0, state.1, state.2, state.3.clone())
        };

        results.successful_requests = successful;
        results.failed_requests = failed;
        results.total_requests = successful + failed;
        results.total_datapoints_sent = total_datapoints;
        results.error_details = errors;
        results.test_duration = test_duration;

        // Calculate throughput based on actual test duration
        results.throughput_datapoints_per_sec =
            results.total_datapoints_sent as f64 / test_duration.as_secs_f64();
        results.throughput_requests_per_sec =
            results.total_requests as f64 / test_duration.as_secs_f64();

        let total_elapsed = start.elapsed();
        info!(
            "Test completed: {}s test duration, {}s total elapsed",
            test_duration.as_secs(),
            total_elapsed.as_secs()
        );

        // Calculate latency statistics
        if !final_latencies.is_empty() {
            let mut latencies_sorted = final_latencies;
            latencies_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
            results.latency_stats = LatencyStats {
                mean_ms: latencies_sorted.iter().sum::<f64>() / latencies_sorted.len() as f64,
                median_ms: latencies_sorted[latencies_sorted.len() / 2],
                p95_ms: latencies_sorted[(latencies_sorted.len() as f64 * 0.95) as usize],
                p99_ms: latencies_sorted[(latencies_sorted.len() as f64 * 0.99) as usize],
                min_ms: latencies_sorted[0],
                max_ms: latencies_sorted[latencies_sorted.len() - 1],
            };
        }

        Ok(results)
    }

    /// Producer worker - generates batches and sends them through channel (synchronous version)
    #[allow(dead_code)]
    async fn producer_worker_sync(
        generator: crate::performance::generator::MetricDataGenerator,
        batch_size: usize,
        test_duration: Duration,
        batch_sender: mpsc::Sender<(Vec<Vec<Value>>, u64)>, // (batch, datapoint_count)
    ) -> u64 {
        let start = Instant::now();
        let mut batches_generated = 0;
        let mut last_progress = Instant::now();

        while start.elapsed() < test_duration {
            let batch = generator.generate_batch(batch_size).await;
            let datapoint_count = batch.iter().map(|dp| dp.len()).sum::<usize>() as u64;

            // Try to send batch; if channel is full, this will block (natural backpressure)
            match batch_sender.send((batch, datapoint_count)).await {
                Ok(_) => {
                    batches_generated += 1;

                    // Progress reporting every 10 seconds
                    if last_progress.elapsed() >= Duration::from_secs(10) {
                        let elapsed = start.elapsed().as_secs();
                        let total = test_duration.as_secs();
                        info!(
                            "Producer progress: {}s / {}s ({:.1}%) - {} batches generated",
                            elapsed,
                            total,
                            (elapsed as f64 / total as f64) * 100.0,
                            batches_generated
                        );
                        last_progress = Instant::now();
                    }
                }
                Err(_) => {
                    // Channel closed, test is ending
                    break;
                }
            }
        }

        // Close the channel to signal workers to stop
        drop(batch_sender);
        batches_generated
    }

    /// Producer worker - spawnable version for multiple producers
    async fn producer_worker(
        producer_id: usize,
        generator: crate::performance::generator::MetricDataGenerator,
        batch_size: usize,
        test_duration: Duration,
        batch_sender: mpsc::Sender<(Vec<Vec<Value>>, u64)>, // (batch, datapoint_count)
    ) -> u64 {
        let start = Instant::now();
        let mut batches_generated = 0;
        let mut last_progress = Instant::now();

        while start.elapsed() < test_duration {
            let batch = generator.generate_batch(batch_size).await;
            let datapoint_count = batch.iter().map(|dp| dp.len()).sum::<usize>() as u64;

            // Try to send batch; if channel is full, this will block (natural backpressure)
            match batch_sender.send((batch, datapoint_count)).await {
                Ok(_) => {
                    batches_generated += 1;

                    // Progress reporting every 30 seconds (less frequent for multiple producers)
                    if last_progress.elapsed() >= Duration::from_secs(30) {
                        let elapsed = start.elapsed().as_secs();
                        let total = test_duration.as_secs();
                        info!(
                            "Producer {} progress: {}s / {}s ({:.1}%) - {} batches generated",
                            producer_id,
                            elapsed,
                            total,
                            (elapsed as f64 / total as f64) * 100.0,
                            batches_generated
                        );
                        last_progress = Instant::now();
                    }
                }
                Err(_) => {
                    // Channel closed, test is ending
                    break;
                }
            }
        }

        // Channel will be dropped when this function exits
        batches_generated
    }

    /// Consumer worker - processes batches from channel
    async fn consumer_worker(
        worker_id: usize,
        batch_receiver: BatchReceiver,
        client: Client,
        ingest_url: String,
        performance_mode: Option<String>,
        latencies: Arc<tokio::sync::Mutex<Vec<f64>>>,
        results_state: ResultsState, // (successful, failed, total_datapoints, errors)
    ) {
        let mut requests_processed = 0;

        loop {
            let message = {
                let mut receiver = batch_receiver.lock().await;
                receiver.recv().await
            };

            match message {
                Some((batch, datapoint_count)) => {
                    let req_start = Instant::now();

                    match Self::send_batch_internal(
                        &client,
                        &ingest_url,
                        &batch,
                        performance_mode.as_deref(),
                    )
                    .await
                    {
                        Ok(_) => {
                            let latency_ms = req_start.elapsed().as_secs_f64() * 1000.0;

                            // Update shared state
                            {
                                let mut state = results_state.lock().await;
                                state.0 += 1; // successful
                                state.2 += datapoint_count; // total_datapoints
                            }

                            // Record latency
                            latencies.lock().await.push(latency_ms);

                            requests_processed += 1;
                        }
                        Err(e) => {
                            // Update shared state
                            {
                                let mut state = results_state.lock().await;
                                state.1 += 1; // failed
                                state.2 += datapoint_count; // total_datapoints (still count them)
                                state.3.push(e.to_string()); // errors
                            }

                            requests_processed += 1;
                        }
                    }
                }
                None => {
                    // Channel closed, producer is done
                    break;
                }
            }
        }

        info!(
            "Worker {} finished: processed {} requests",
            worker_id, requests_processed
        );
    }

    async fn send_batch(&self, batch: &[Vec<Value>]) -> anyhow::Result<()> {
        Self::send_batch_internal(
            &self.client,
            &self.config.ingest_url,
            batch,
            self.config.performance_mode.as_deref(),
        )
        .await
    }

    async fn send_batch_internal(
        client: &Client,
        ingest_url: &str,
        batch: &[Vec<Value>],
        performance_mode: Option<&str>,
    ) -> anyhow::Result<()> {
        let payload: Vec<Value> = batch.iter().flatten().cloned().collect();

        // Build URL with optional performance mode query parameter
        let url = if let Some(mode) = performance_mode {
            format!("{}/api/v1/datapoints?perf_mode={}", ingest_url, mode)
        } else {
            format!("{}/api/v1/datapoints", ingest_url)
        };

        let response = client
            .post(&url)
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
