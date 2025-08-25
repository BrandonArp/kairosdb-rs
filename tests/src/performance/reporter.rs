use super::*;
use chrono::{DateTime, Utc};
use std::fs;
use std::io::Write;
use std::path::Path;

/// Performance test reporter that generates detailed reports
pub struct PerfTestReporter {
    test_name: String,
    start_time: DateTime<Utc>,
    config: PerfTestConfig,
}

impl PerfTestReporter {
    pub fn new(test_name: String, config: PerfTestConfig) -> Self {
        Self {
            test_name,
            start_time: Utc::now(),
            config,
        }
    }

    /// Generate comprehensive performance report
    pub fn generate_report(&self, results: &PerfTestResults) -> PerfReport {
        let efficiency_score = self.calculate_efficiency_score(results);
        let bottlenecks = self.identify_bottlenecks(results);
        let recommendations = self.generate_recommendations(results);

        PerfReport {
            test_name: self.test_name.clone(),
            timestamp: self.start_time,
            config: self.config.clone(),
            results: results.clone(),
            efficiency_score,
            bottlenecks,
            recommendations,
            summary: self.generate_summary(results),
        }
    }

    /// Print results to console with nice formatting
    pub fn print_results(&self, results: &PerfTestResults) {
        println!("\n🚀 Performance Test Results: {}", self.test_name);
        println!("═══════════════════════════════════════════════════════");

        // Overview
        println!("\n📊 Overview:");
        println!("  Duration: {:.1}s", results.test_duration.as_secs_f64());
        println!("  Total Requests: {}", results.total_requests);
        println!(
            "  Successful: {} ({:.1}%)",
            results.successful_requests,
            (results.successful_requests as f64 / results.total_requests as f64) * 100.0
        );
        println!(
            "  Failed: {} ({:.1}%)",
            results.failed_requests,
            (results.failed_requests as f64 / results.total_requests as f64) * 100.0
        );

        // Throughput
        println!("\n📈 Throughput:");
        println!(
            "  Datapoints/sec: {:.0}",
            results.throughput_datapoints_per_sec
        );
        println!("  Requests/sec: {:.1}", results.throughput_requests_per_sec);
        println!("  Total Datapoints: {}", results.total_datapoints_sent);

        // Latency
        println!("\n⏱️  Latency (ms):");
        println!("  Mean: {:.1}", results.latency_stats.mean_ms);
        println!("  Median: {:.1}", results.latency_stats.median_ms);
        println!("  95th percentile: {:.1}", results.latency_stats.p95_ms);
        println!("  99th percentile: {:.1}", results.latency_stats.p99_ms);
        println!("  Min: {:.1}", results.latency_stats.min_ms);
        println!("  Max: {:.1}", results.latency_stats.max_ms);

        // Configuration
        println!("\n⚙️  Configuration:");
        println!("  Metrics: {}", self.config.metrics_count);
        println!(
            "  Tag combinations/metric: {}",
            self.config.tag_combinations_per_metric
        );
        println!(
            "  Histogram samples: {}-{}",
            self.config.histogram_samples_per_datapoint.0,
            self.config.histogram_samples_per_datapoint.1
        );
        println!("  Batch size: {}", self.config.batch_size);
        println!("  Concurrent batches: {}", self.config.concurrent_batches);

        // Errors (if any)
        if !results.error_details.is_empty() {
            println!("\n❌ Errors ({} total):", results.error_details.len());
            let unique_errors: std::collections::HashSet<_> =
                results.error_details.iter().collect();
            for (i, error) in unique_errors.iter().take(5).enumerate() {
                println!("  {}: {}", i + 1, error);
            }
            if unique_errors.len() > 5 {
                println!("  ... and {} more", unique_errors.len() - 5);
            }
        }

        // Performance analysis
        let efficiency = self.calculate_efficiency_score(results);
        println!("\n🎯 Performance Analysis:");
        println!("  Efficiency Score: {:.1}/100", efficiency);
        self.print_performance_analysis(results);
    }

    /// Save detailed results to JSON file
    pub fn save_to_file(
        &self,
        results: &PerfTestResults,
        output_dir: &Path,
    ) -> anyhow::Result<String> {
        let report = self.generate_report(results);

        // Ensure output directory exists
        fs::create_dir_all(output_dir)?;

        let filename = format!(
            "perf_{}_{}.json",
            self.test_name.replace(' ', "_"),
            self.start_time.format("%Y%m%d_%H%M%S")
        );

        let file_path = output_dir.join(&filename);
        let json_data = serde_json::to_string_pretty(&report)?;

        fs::write(&file_path, json_data)?;

        Ok(file_path.to_string_lossy().to_string())
    }

    /// Generate CSV summary for trending analysis
    pub fn save_csv_summary(
        &self,
        results: &PerfTestResults,
        output_path: &Path,
    ) -> anyhow::Result<()> {
        let header_exists = output_path.exists();
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(output_path)?;

        // Write header if file is new
        if !header_exists {
            writeln!(file, "timestamp,test_name,duration_s,total_requests,successful_requests,failed_requests,datapoints_per_sec,requests_per_sec,mean_latency_ms,p95_latency_ms,p99_latency_ms,efficiency_score")?;
        }

        let efficiency = self.calculate_efficiency_score(results);
        writeln!(
            file,
            "{},{},{:.1},{},{},{},{:.0},{:.1},{:.1},{:.1},{:.1},{:.1}",
            self.start_time.format("%Y-%m-%d %H:%M:%S"),
            self.test_name,
            results.test_duration.as_secs_f64(),
            results.total_requests,
            results.successful_requests,
            results.failed_requests,
            results.throughput_datapoints_per_sec,
            results.throughput_requests_per_sec,
            results.latency_stats.mean_ms,
            results.latency_stats.p95_ms,
            results.latency_stats.p99_ms,
            efficiency
        )?;

        Ok(())
    }

    pub fn calculate_efficiency_score(&self, results: &PerfTestResults) -> f64 {
        let success_rate = results.successful_requests as f64 / results.total_requests as f64;
        let latency_score = (1000.0 - results.latency_stats.p95_ms.min(1000.0)) / 1000.0;
        let throughput_score = (results.throughput_datapoints_per_sec / 10000.0).min(1.0);

        (success_rate * 0.4 + latency_score * 0.3 + throughput_score * 0.3) * 100.0
    }

    fn identify_bottlenecks(&self, results: &PerfTestResults) -> Vec<String> {
        let mut bottlenecks = Vec::new();

        // High error rate
        let error_rate = results.failed_requests as f64 / results.total_requests as f64;
        if error_rate > 0.01 {
            bottlenecks.push(format!("High error rate: {:.1}%", error_rate * 100.0));
        }

        // High latency
        if results.latency_stats.p95_ms > 500.0 {
            bottlenecks.push("High P95 latency (>500ms)".to_string());
        }

        // Low throughput
        if results.throughput_datapoints_per_sec < 1000.0 {
            bottlenecks.push("Low throughput (<1000 datapoints/sec)".to_string());
        }

        // High latency variance
        let latency_variance = results.latency_stats.p99_ms - results.latency_stats.median_ms;
        if latency_variance > results.latency_stats.median_ms * 2.0 {
            bottlenecks.push("High latency variance".to_string());
        }

        bottlenecks
    }

    fn generate_recommendations(&self, results: &PerfTestResults) -> Vec<String> {
        let mut recommendations = Vec::new();

        let error_rate = results.failed_requests as f64 / results.total_requests as f64;
        if error_rate > 0.05 {
            recommendations
                .push("Investigate high error rate - check logs and system resources".to_string());
        }

        if results.latency_stats.p95_ms > 1000.0 {
            recommendations.push(
                "Consider optimizing database queries or adding connection pooling".to_string(),
            );
        }

        if results.throughput_datapoints_per_sec < 5000.0 {
            recommendations
                .push("Consider increasing batch size or concurrent connections".to_string());
        }

        let cpu_estimate =
            self.config.concurrent_batches as f64 * 100.0 / results.throughput_requests_per_sec;
        if cpu_estimate > 50.0 {
            recommendations
                .push("High CPU utilization estimated - consider scaling horizontally".to_string());
        }

        if recommendations.is_empty() {
            recommendations.push(
                "Performance looks good! Consider stress testing with higher loads".to_string(),
            );
        }

        recommendations
    }

    fn generate_summary(&self, results: &PerfTestResults) -> String {
        format!(
            "Processed {} datapoints in {:.1}s at {:.0} dp/s with {:.1}% success rate and P95 latency of {:.1}ms",
            results.total_datapoints_sent,
            results.test_duration.as_secs_f64(),
            results.throughput_datapoints_per_sec,
            (results.successful_requests as f64 / results.total_requests as f64) * 100.0,
            results.latency_stats.p95_ms
        )
    }

    fn print_performance_analysis(&self, results: &PerfTestResults) {
        // Throughput rating
        let throughput_rating = match results.throughput_datapoints_per_sec as u64 {
            0..=1000 => "🐌 Slow",
            1001..=5000 => "🚶 Moderate",
            5001..=15000 => "🏃 Good",
            15001..=50000 => "🚀 Excellent",
            _ => "⚡ Outstanding",
        };
        println!(
            "  Throughput: {} ({:.0} dp/s)",
            throughput_rating, results.throughput_datapoints_per_sec
        );

        // Latency rating
        let latency_rating = match results.latency_stats.p95_ms as u64 {
            0..=50 => "⚡ Excellent",
            51..=200 => "🚀 Good",
            201..=500 => "🏃 Moderate",
            501..=1000 => "🚶 Slow",
            _ => "🐌 Poor",
        };
        println!(
            "  Latency: {} (P95: {:.1}ms)",
            latency_rating, results.latency_stats.p95_ms
        );

        // Success rate
        let success_rate =
            (results.successful_requests as f64 / results.total_requests as f64) * 100.0;
        let success_rating = match success_rate as u64 {
            99..=100 => "✅ Excellent",
            95..=98 => "👍 Good",
            90..=94 => "⚠️  Moderate",
            _ => "❌ Poor",
        };
        println!("  Reliability: {} ({:.1}%)", success_rating, success_rate);
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PerfReport {
    pub test_name: String,
    pub timestamp: DateTime<Utc>,
    pub config: PerfTestConfig,
    pub results: PerfTestResults,
    pub efficiency_score: f64,
    pub bottlenecks: Vec<String>,
    pub recommendations: Vec<String>,
    pub summary: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn create_test_results() -> PerfTestResults {
        PerfTestResults {
            total_datapoints_sent: 10000,
            total_requests: 100,
            successful_requests: 98,
            failed_requests: 2,
            test_duration: Duration::from_secs(60),
            throughput_datapoints_per_sec: 166.67,
            throughput_requests_per_sec: 1.67,
            latency_stats: LatencyStats {
                mean_ms: 250.0,
                median_ms: 200.0,
                p95_ms: 500.0,
                p99_ms: 800.0,
                min_ms: 50.0,
                max_ms: 1000.0,
            },
            error_details: vec!["Connection timeout".to_string()],
        }
    }

    #[test]
    fn test_efficiency_score_calculation() {
        let config = PerfTestConfig::default();
        let reporter = PerfTestReporter::new("test".to_string(), config);
        let results = create_test_results();

        let score = reporter.calculate_efficiency_score(&results);
        assert!(score > 0.0 && score <= 100.0);
    }

    #[test]
    fn test_bottleneck_identification() {
        let config = PerfTestConfig::default();
        let reporter = PerfTestReporter::new("test".to_string(), config);
        let results = create_test_results();

        let bottlenecks = reporter.identify_bottlenecks(&results);
        assert!(!bottlenecks.is_empty());
    }
}
