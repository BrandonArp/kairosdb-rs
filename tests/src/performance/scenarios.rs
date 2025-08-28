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
            duration_seconds: 300,
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

            // Generate and save report
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
