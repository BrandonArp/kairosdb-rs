use super::*;

/// Predefined performance test scenarios
pub struct TestScenarios;

impl TestScenarios {
    /// Small scale test - for local development and quick validation
    pub fn small_scale() -> PerfTestConfig {
        PerfTestConfig {
            ingest_url: "http://localhost:8081".to_string(),
            metrics_count: 50,
            tag_combinations_per_metric: 10,
            histogram_samples_per_datapoint: (10, 100),
            batch_size: 25,
            concurrent_batches: 25, // Higher concurrency for small scale
            duration_seconds: 30,
            tag_cardinality_limit: 5,
            warmup_seconds: 5,
            performance_mode: None, // Use server default
            producer_tasks: 8,      // Multiple producers for better generation throughput
        }
    }

    /// Medium scale test - for CI and staging environments
    pub fn medium_scale() -> PerfTestConfig {
        PerfTestConfig {
            ingest_url: "http://localhost:8081".to_string(),
            metrics_count: 500,
            tag_combinations_per_metric: 25,
            histogram_samples_per_datapoint: (50, 500),
            batch_size: 100,
            concurrent_batches: 50, // Default high concurrency
            duration_seconds: 120,
            tag_cardinality_limit: 15,
            warmup_seconds: 10,
            performance_mode: None, // Use server default
            producer_tasks: 8,      // Multiple producers for better generation throughput
        }
    }

    /// Large scale test - for production-like load testing
    pub fn large_scale() -> PerfTestConfig {
        PerfTestConfig {
            ingest_url: "http://localhost:8081".to_string(),
            metrics_count: 2000,
            tag_combinations_per_metric: 50,
            histogram_samples_per_datapoint: (100, 1000),
            batch_size: 200,
            concurrent_batches: 75, // Higher load for large scale
            duration_seconds: 120,
            tag_cardinality_limit: 25,
            warmup_seconds: 15,
            performance_mode: None, // Use server default
            producer_tasks: 8,      // Multiple producers for better generation throughput
        }
    }

    /// Stress test - designed to find breaking points
    pub fn stress_test() -> PerfTestConfig {
        PerfTestConfig {
            ingest_url: "http://localhost:8081".to_string(),
            metrics_count: 5000,
            tag_combinations_per_metric: 100,
            histogram_samples_per_datapoint: (500, 2000),
            batch_size: 500,
            concurrent_batches: 100, // Maximum load for stress test
            duration_seconds: 600,
            tag_cardinality_limit: 50,
            warmup_seconds: 30,
            performance_mode: None, // Use server default
            producer_tasks: 8,      // Multiple producers for better generation throughput
        }
    }

    /// High cardinality test - tests tag explosion scenarios
    pub fn high_cardinality() -> PerfTestConfig {
        PerfTestConfig {
            ingest_url: "http://localhost:8081".to_string(),
            metrics_count: 100,
            tag_combinations_per_metric: 1000,
            histogram_samples_per_datapoint: (10, 100),
            batch_size: 50,
            concurrent_batches: 30, // Moderate concurrency for high cardinality
            duration_seconds: 180,
            tag_cardinality_limit: 100,
            warmup_seconds: 10,
            performance_mode: None, // Use server default
            producer_tasks: 8,      // Multiple producers for better generation throughput
        }
    }

    /// High frequency test - many small batches
    pub fn high_frequency() -> PerfTestConfig {
        PerfTestConfig {
            ingest_url: "http://localhost:8081".to_string(),
            metrics_count: 200,
            tag_combinations_per_metric: 10,
            histogram_samples_per_datapoint: (5, 50),
            batch_size: 10,
            concurrent_batches: 75, // High frequency needs high concurrency
            duration_seconds: 240,
            tag_cardinality_limit: 8,
            warmup_seconds: 10,
            performance_mode: None, // Use server default
            producer_tasks: 8,      // Multiple producers for better generation throughput
        }
    }

    /// Large batch test - fewer large batches
    pub fn large_batch() -> PerfTestConfig {
        PerfTestConfig {
            ingest_url: "http://localhost:8081".to_string(),
            metrics_count: 1000,
            tag_combinations_per_metric: 20,
            histogram_samples_per_datapoint: (100, 1000),
            batch_size: 1000,
            concurrent_batches: 20, // Lower concurrency for large batches
            duration_seconds: 180,
            tag_cardinality_limit: 15,
            warmup_seconds: 15,
            performance_mode: None, // Use server default
            producer_tasks: 8,      // Multiple producers for better generation throughput
        }
    }

    /// Memory pressure test - large histograms
    pub fn memory_pressure() -> PerfTestConfig {
        PerfTestConfig {
            ingest_url: "http://localhost:8081".to_string(),
            metrics_count: 500,
            tag_combinations_per_metric: 30,
            histogram_samples_per_datapoint: (1000, 5000),
            batch_size: 100,
            concurrent_batches: 40, // Moderate concurrency for memory pressure
            duration_seconds: 300,
            tag_cardinality_limit: 20,
            warmup_seconds: 20,
            performance_mode: None, // Use server default
            producer_tasks: 8,      // Multiple producers for better generation throughput
        }
    }

    /// Get all predefined scenarios
    pub fn all_scenarios() -> Vec<(&'static str, PerfTestConfig)> {
        vec![
            ("small_scale", Self::small_scale()),
            ("medium_scale", Self::medium_scale()),
            ("large_scale", Self::large_scale()),
            ("stress_test", Self::stress_test()),
            ("high_cardinality", Self::high_cardinality()),
            ("high_frequency", Self::high_frequency()),
            ("large_batch", Self::large_batch()),
            ("memory_pressure", Self::memory_pressure()),
        ]
    }

    /// Get scenario by name
    pub fn by_name(name: &str) -> Option<PerfTestConfig> {
        match name.to_lowercase().as_str() {
            "small" | "small_scale" => Some(Self::small_scale()),
            "medium" | "medium_scale" => Some(Self::medium_scale()),
            "large" | "large_scale" => Some(Self::large_scale()),
            "stress" | "stress_test" => Some(Self::stress_test()),
            "cardinality" | "high_cardinality" => Some(Self::high_cardinality()),
            "frequency" | "high_frequency" => Some(Self::high_frequency()),
            "batch" | "large_batch" => Some(Self::large_batch()),
            "memory" | "memory_pressure" => Some(Self::memory_pressure()),
            _ => None,
        }
    }

    /// List available scenario names
    pub fn list_scenarios() -> Vec<&'static str> {
        vec![
            "small_scale - Quick local test (50 metrics, 30s)",
            "medium_scale - CI/staging test (500 metrics, 2m)",
            "large_scale - Production load test (2k metrics, 5m)",
            "stress_test - Breaking point test (5k metrics, 10m)",
            "high_cardinality - Tag explosion test (100 metrics, 1k tags)",
            "high_frequency - Many small batches test",
            "large_batch - Few large batches test",
            "memory_pressure - Large histogram test",
        ]
    }

    /// Create custom scenario with overrides
    pub fn custom(base_scenario: &str, overrides: ScenarioOverrides) -> Option<PerfTestConfig> {
        let mut config = Self::by_name(base_scenario)?;

        if let Some(url) = overrides.ingest_url {
            config.ingest_url = url;
        }
        if let Some(metrics) = overrides.metrics_count {
            config.metrics_count = metrics;
        }
        if let Some(tags) = overrides.tag_combinations_per_metric {
            config.tag_combinations_per_metric = tags;
        }
        if let Some(samples) = overrides.histogram_samples_per_datapoint {
            config.histogram_samples_per_datapoint = samples;
        }
        if let Some(batch_size) = overrides.batch_size {
            config.batch_size = batch_size;
        }
        if let Some(concurrent) = overrides.concurrent_batches {
            config.concurrent_batches = concurrent;
        }
        if let Some(duration) = overrides.duration_seconds {
            config.duration_seconds = duration;
        }
        if let Some(cardinality) = overrides.tag_cardinality_limit {
            config.tag_cardinality_limit = cardinality;
        }
        if let Some(warmup) = overrides.warmup_seconds {
            config.warmup_seconds = warmup;
        }
        if let Some(perf_mode) = overrides.performance_mode {
            config.performance_mode = Some(perf_mode);
        }

        Some(config)
    }
}

/// Scenario override parameters for customization
#[derive(Debug, Default, Clone)]
pub struct ScenarioOverrides {
    pub ingest_url: Option<String>,
    pub metrics_count: Option<usize>,
    pub tag_combinations_per_metric: Option<usize>,
    pub histogram_samples_per_datapoint: Option<(usize, usize)>,
    pub batch_size: Option<usize>,
    pub concurrent_batches: Option<usize>,
    pub duration_seconds: Option<u64>,
    pub tag_cardinality_limit: Option<usize>,
    pub warmup_seconds: Option<u64>,
    pub performance_mode: Option<String>,
}

/// Wait for Rust service queue to drain after performance test completion
/// Tracks queue processing metrics and resets timeout when progress is observed
async fn wait_for_queue_drain_internal(
    client: &reqwest::Client,
    rust_url: &str,
    ingestion_results: &crate::performance::PerfTestResults,
) -> anyhow::Result<Option<crate::performance::QueueProcessingMetrics>> {
    use std::time::{Duration, Instant};

    let mut attempts_since_progress = 0;
    let max_attempts_without_progress = 60; // 5 minutes max wait without progress
    let mut last_queue_size: Option<u64> = None;
    let mut initial_queue_size: Option<u64> = None;
    let mut max_observed_size: u64 = 0;
    let start_time = Instant::now();
    let mut total_checks = 0;

    loop {
        attempts_since_progress += 1;
        total_checks += 1;

        // Check Rust service metrics
        match client
            .get(format!("{}/api/v1/metrics", rust_url))
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(response) => {
                if let Ok(text) = response.text().await {
                    if let Ok(metrics) = serde_json::from_str::<serde_json::Value>(&text) {
                        if let Some(queue_size) = metrics.get("queue_size").and_then(|v| v.as_u64())
                        {
                            // Track initial and maximum queue sizes
                            if initial_queue_size.is_none() {
                                initial_queue_size = Some(queue_size);
                                tracing::info!(
                                    "📊 Starting queue drain monitoring (initial size: {})",
                                    queue_size
                                );
                            }
                            max_observed_size = max_observed_size.max(queue_size);

                            tracing::info!("📊 Queue size: {}", queue_size);

                            // Check for progress (queue size decreasing)
                            if let Some(last_size) = last_queue_size {
                                if queue_size < last_size {
                                    attempts_since_progress = 0; // Reset timeout on progress
                                }
                            }
                            last_queue_size = Some(queue_size);

                            if queue_size < 100 {
                                let elapsed = start_time.elapsed();
                                let initial_size = initial_queue_size.unwrap_or(0);
                                let processed_items = max_observed_size.saturating_sub(queue_size);
                                let processing_rate = if elapsed.as_secs() > 0 {
                                    processed_items as f64 / elapsed.as_secs() as f64
                                } else {
                                    0.0
                                };

                                // Calculate throughput estimates based on ingestion results
                                let avg_batch_size = if ingestion_results.total_requests > 0 {
                                    ingestion_results.total_datapoints_sent as f64
                                        / ingestion_results.total_requests as f64
                                } else {
                                    0.0
                                };

                                let batches_per_second =
                                    if elapsed.as_secs() > 0 && avg_batch_size > 0.0 {
                                        processed_items as f64
                                            / avg_batch_size
                                            / elapsed.as_secs() as f64
                                    } else {
                                        0.0
                                    };

                                let datapoints_per_second = batches_per_second * avg_batch_size;

                                let metrics = crate::performance::QueueProcessingMetrics {
                                    initial_queue_size: initial_size,
                                    peak_queue_size: max_observed_size,
                                    final_queue_size: queue_size,
                                    total_items_processed: processed_items,
                                    processing_time_seconds: elapsed.as_secs_f64(),
                                    items_per_second: processing_rate,
                                    total_status_checks: total_checks,
                                    estimated_batch_size: avg_batch_size,
                                    estimated_batches_per_second: batches_per_second,
                                    estimated_datapoints_per_second: datapoints_per_second,
                                };

                                tracing::info!(
                                    "✅ Queue drained successfully (size: {} < 100)",
                                    queue_size
                                );
                                return Ok(Some(metrics));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                tracing::info!(
                    "⚠️  Could not check queue status: {} (attempt {} since progress)",
                    e,
                    attempts_since_progress
                );
            }
        }

        if attempts_since_progress >= max_attempts_without_progress {
            let elapsed = start_time.elapsed();
            let initial_size = initial_queue_size.unwrap_or(0);
            let current_size = last_queue_size.unwrap_or(0);
            let processed_items = max_observed_size.saturating_sub(current_size);

            // Calculate partial metrics for timeout case
            let avg_batch_size = if ingestion_results.total_requests > 0 {
                ingestion_results.total_datapoints_sent as f64
                    / ingestion_results.total_requests as f64
            } else {
                0.0
            };

            let processing_rate = if elapsed.as_secs() > 0 {
                processed_items as f64 / elapsed.as_secs() as f64
            } else {
                0.0
            };

            let batches_per_second = if elapsed.as_secs() > 0 && avg_batch_size > 0.0 {
                processed_items as f64 / avg_batch_size / elapsed.as_secs() as f64
            } else {
                0.0
            };

            let metrics = crate::performance::QueueProcessingMetrics {
                initial_queue_size: initial_size,
                peak_queue_size: max_observed_size,
                final_queue_size: current_size,
                total_items_processed: processed_items,
                processing_time_seconds: elapsed.as_secs_f64(),
                items_per_second: processing_rate,
                total_status_checks: total_checks,
                estimated_batch_size: avg_batch_size,
                estimated_batches_per_second: batches_per_second,
                estimated_datapoints_per_second: batches_per_second * avg_batch_size,
            };

            tracing::info!(
                "⚠️  Queue drain timeout after {} attempts without progress",
                max_attempts_without_progress
            );
            return Ok(Some(metrics)); // Return partial metrics
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Performance test suite runner for running multiple scenarios
pub struct PerfTestSuite {
    scenarios: Vec<(String, PerfTestConfig)>,
    output_dir: std::path::PathBuf,
}

impl PerfTestSuite {
    pub fn new(output_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            scenarios: Vec::new(),
            output_dir: output_dir.into(),
        }
    }

    pub fn add_scenario(&mut self, name: String, config: PerfTestConfig) {
        self.scenarios.push((name, config));
    }

    pub fn add_all_scenarios(&mut self) {
        for (name, config) in TestScenarios::all_scenarios() {
            self.scenarios.push((name.to_string(), config));
        }
    }

    pub async fn run_all(&mut self) -> anyhow::Result<Vec<(String, PerfTestResults)>> {
        let mut results = Vec::new();

        for (name, config) in &self.scenarios {
            println!("\n🎯 Running scenario: {}", name);

            let mut runner = PerfTestRunner::new(config.clone());
            let result = runner.run().await?;

            // Generate and save report (no queue monitoring for this function)
            let reporter = PerfTestReporter::new(name.clone(), config.clone());
            reporter.print_results(&result);

            if let Err(e) = reporter.save_to_file(&result, &self.output_dir) {
                tracing::warn!("Failed to save report for {}: {}", name, e);
            }

            // Save to trending CSV
            let csv_path = self.output_dir.join("performance_trends.csv");
            if let Err(e) = reporter.save_csv_summary(&result, &csv_path) {
                tracing::warn!("Failed to save CSV summary: {}", e);
            }

            results.push((name.clone(), result));

            // Brief pause between scenarios
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }

        Ok(results)
    }

    /// Run all scenarios in the suite with queue monitoring after each test
    pub async fn run_all_with_queue_monitoring(
        &mut self,
        service_url: &str,
    ) -> anyhow::Result<Vec<(String, PerfTestResults)>> {
        use reqwest::Client;

        let client = Client::new();
        let mut results = Vec::new();

        for (name, config) in &self.scenarios {
            println!("\n🎯 Running scenario: {}", name);

            let mut runner = PerfTestRunner::new(config.clone());
            let result = runner.run().await?;

            // Wait for queue to drain for accurate test completion timing
            tracing::info!("⏳ Waiting for queue to drain after scenario '{}'...", name);
            let queue_metrics =
                match wait_for_queue_drain_internal(&client, service_url, &result).await {
                    Ok(metrics) => metrics,
                    Err(e) => {
                        tracing::warn!("Queue drain failed for scenario '{}': {}", name, e);
                        None
                    }
                };

            // Add queue metrics to the result
            let mut final_result = result;
            final_result.queue_processing_metrics = queue_metrics;

            // Generate and save report
            let reporter = PerfTestReporter::new(name.clone(), config.clone());
            reporter.print_results(&final_result);

            if let Err(e) = reporter.save_to_file(&final_result, &self.output_dir) {
                tracing::warn!("Failed to save report for {}: {}", name, e);
            }

            // Save to trending CSV
            let csv_path = self.output_dir.join("performance_trends.csv");
            if let Err(e) = reporter.save_csv_summary(&final_result, &csv_path) {
                tracing::warn!("Failed to save CSV summary: {}", e);
            }

            results.push((name.clone(), final_result));

            // Brief pause between scenarios
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }

        Ok(results)
    }

    pub fn print_suite_summary(&self, results: &[(String, PerfTestResults)]) {
        println!("\n📋 Performance Test Suite Summary");
        println!("═══════════════════════════════════════════════════════");
        println!(
            "{:<20} {:>12} {:>12} {:>12} {:>12}",
            "Scenario", "DP/sec", "Success%", "P95 (ms)", "Score"
        );
        println!("───────────────────────────────────────────────────────");

        for (name, result) in results {
            let success_rate =
                (result.successful_requests as f64 / result.total_requests as f64) * 100.0;
            let reporter = PerfTestReporter::new(name.clone(), PerfTestConfig::default());
            let score = reporter.calculate_efficiency_score(result);

            println!(
                "{:<20} {:>12.0} {:>11.1}% {:>12.1} {:>12.1}",
                name,
                result.throughput_datapoints_per_sec,
                success_rate,
                result.latency_stats.p95_ms,
                score
            );
        }

        println!("───────────────────────────────────────────────────────");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_scenarios_available() {
        let scenarios = TestScenarios::all_scenarios();
        assert!(!scenarios.is_empty());
        assert!(scenarios.len() >= 8);
    }

    #[test]
    fn test_scenario_by_name() {
        let config = TestScenarios::by_name("small_scale");
        assert!(config.is_some());

        let config = config.unwrap();
        assert_eq!(config.metrics_count, 50);
        assert_eq!(config.duration_seconds, 30);
    }

    #[test]
    fn test_custom_scenario_overrides() {
        let overrides = ScenarioOverrides {
            metrics_count: Some(1000),
            duration_seconds: Some(60),
            ..Default::default()
        };

        let config = TestScenarios::custom("small_scale", overrides);
        assert!(config.is_some());

        let config = config.unwrap();
        assert_eq!(config.metrics_count, 1000); // overridden
        assert_eq!(config.duration_seconds, 60); // overridden
        assert_eq!(config.tag_combinations_per_metric, 10); // original
    }

    #[test]
    fn test_scenario_names() {
        let names = TestScenarios::list_scenarios();
        assert!(!names.is_empty());
        assert!(names.iter().any(|n| n.contains("small_scale")));
    }
}
