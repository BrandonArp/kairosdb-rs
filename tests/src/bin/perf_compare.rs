#![allow(clippy::single_component_path_imports)]
#![allow(clippy::ptr_arg)]

use anyhow::Result;
use clap::{Parser, Subcommand};
use kairosdb_e2e_tests::performance::*;
use kairosdb_e2e_tests::throughput_collector::{ThroughputCollector, ThroughputCollectorConfig, ThroughputMetrics};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tracing::{info, Level};
use tracing_subscriber;
use serde::{Deserialize, Serialize};
use reqwest::Client;

/// Wait for Rust service queue to drain by monitoring metrics
async fn wait_for_queue_drain(rust_url: &str) -> Result<()> {
    let client = Client::new();
    let mut attempts = 0;
    let max_attempts = 60; // 5 minutes max wait
    
    loop {
        attempts += 1;
        
        // Check Rust service metrics
        match client
            .get(&format!("{}/api/v1/metrics", rust_url))
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(response) => {
                if let Ok(text) = response.text().await {
                    if let Ok(metrics) = serde_json::from_str::<serde_json::Value>(&text) {
                        if let Some(queue_size) = metrics.get("queue_size").and_then(|v| v.as_u64()) {
                            info!("📊 Queue size: {}", queue_size);
                            if queue_size < 100 {
                                info!("✅ Queue drained successfully (size: {} < 100)", queue_size);
                                return Ok(());
                            }
                        }
                    }
                }
            }
            Err(e) => {
                info!("⚠️  Could not check queue status: {} (attempt {}/{})", e, attempts, max_attempts);
            }
        }
        
        if attempts >= max_attempts {
            info!("⚠️  Queue drain timeout after {} attempts", max_attempts);
            return Ok(()); // Continue anyway
        }
        
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Wait for Java service queue to drain by querying KairosDB's own queue size metrics
/// Returns the timestamp of the successful metric response that confirmed queue is empty
async fn wait_for_queue_drain_java(java_url: &str) -> Result<u64> {
    info!("⏳ Waiting for Java KairosDB queues to drain...");
    
    let client = Client::new();
    let mut attempts = 0;
    let max_attempts = 360; // 30 minutes max wait (10x original - Java metrics can be very slow during high load)
    
    
    loop {
        attempts += 1;
        
        // Query the single queue metric we care about (get history for interpolation)
        match query_java_queue_metric_history(&client, java_url, "queue/file_queue.size.min").await {
            Ok(data_points) => {
                if data_points.is_empty() {
                    info!("⚠️  No recent data for queue metric - continuing to wait");
                } else {
                    // Sort by timestamp (should already be sorted but ensure it)
                    let mut sorted_points = data_points;
                    sorted_points.sort_by_key(|(_, timestamp)| *timestamp);
                    
                    let latest = sorted_points.last().unwrap();
                    info!("📊 Java queue metric 'queue/file_queue.size.min': {} at timestamp {} ({} total points)", 
                          latest.0, latest.1, sorted_points.len());
                    
                    if latest.0 < 100.0 {
                        // Check if we have enough data to interpolate
                        if sorted_points.len() >= 2 {
                            let interpolated_time = interpolate_drain_time(&sorted_points, 100.0);
                            info!("✅ Java queue drained successfully (size: {} < 100), interpolated drain time: {}", 
                                  latest.0, interpolated_time);
                            return Ok(interpolated_time);
                        } else {
                            info!("✅ Java queue drained successfully (size: {} < 100)", latest.0);
                            return Ok(latest.1);
                        }
                    }
                    
                    // Show trend if we have multiple points
                    if sorted_points.len() >= 2 {
                        let first = &sorted_points[0];
                        let trend = if latest.0 < first.0 { "decreasing" } else { "increasing" };
                        info!("📈 Queue trend: {} -> {} ({})", first.0, latest.0, trend);
                    }
                }
            }
            Err(e) => {
                // If it's a timeout, continue waiting (queue likely still has data)
                if e.to_string().contains("timeout") || e.to_string().contains("Timeout") {
                    info!("⏰ Timeout querying queue metric - continuing to wait");
                } else {
                    info!("⚠️  Could not query queue metric: {} - continuing to wait", e);
                }
            }
        }
        
        if attempts >= max_attempts {
            return Err(anyhow::anyhow!("Java queue drain timeout after {} attempts - cannot get successful metric response", max_attempts));
        }
        
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// Query a specific Java KairosDB queue metric, returning recent data points as (value, timestamp) pairs
async fn query_java_queue_metric_history(
    client: &Client, 
    java_url: &str, 
    metric_name: &str
) -> Result<Vec<(f64, u64)>> {
    let query_payload = serde_json::json!({
        "start_relative": {"value": "10", "unit": "minutes"},
        "metrics": [{
            "name": metric_name
        }]
    });
    
    let response = client
        .post(&format!("{}/api/v1/datapoints/query", java_url))
        .header("Content-Type", "application/json")
        .timeout(Duration::from_secs(10))
        .json(&query_payload)
        .send()
        .await?;
    
    if !response.status().is_success() {
        return Err(anyhow::anyhow!("Java query failed with status {}", response.status()));
    }
    
    let query_result: serde_json::Value = response.json().await?;
    
    // Extract all values from the query response
    let mut data_points = Vec::new();
    
    if let Some(queries) = query_result["queries"].as_array() {
        if let Some(first_query) = queries.first() {
            if let Some(results) = first_query["results"].as_array() {
                if let Some(first_result) = results.first() {
                    if let Some(values) = first_result["values"].as_array() {
                        for value_entry in values {
                            if let Some(value_array) = value_entry.as_array() {
                                if value_array.len() >= 2 {
                                    if let Some(timestamp) = value_array[0].as_u64() {
                                        if let Some(value) = value_array[1].as_f64() {
                                            data_points.push((value, timestamp));
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
    
    Ok(data_points)
}

/// Fallback basic Java service stability check
async fn wait_for_java_service_stability(java_url: &str) -> Result<()> {
    info!("⏳ Checking Java service stability (fallback)...");
    
    let client = Client::new();
    let mut successful_checks = 0;
    let required_successful_checks = 3;
    
    for attempt in 1..=10 {
        match client
            .get(&format!("{}/api/v1/version", java_url))
            .timeout(Duration::from_secs(3))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                successful_checks += 1;
                info!("✅ Java service stability check {}/{}", successful_checks, required_successful_checks);
                
                if successful_checks >= required_successful_checks {
                    info!("✅ Java service stable");
                    return Ok(());
                }
            }
            Ok(response) => {
                info!("⚠️  Java service returned {}", response.status());
                successful_checks = 0;
            }
            Err(e) => {
                info!("⚠️  Java service check failed: {} (attempt {})", e, attempt);
                successful_checks = 0;
            }
        }
        
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    
    info!("✅ Java stability check completed");
    Ok(())
}

/// KairosDB Performance Comparison CLI - Compare Rust vs Java implementations
#[derive(Parser)]
#[command(name = "perf_compare")]
#[command(about = "Compare performance between Rust and Java KairosDB implementations")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Output directory for reports
    #[arg(short, long, default_value = "target/perf_compare_reports")]
    output_dir: PathBuf,

    /// Rust ingest service URL
    #[arg(long, default_value = "http://localhost:8081")]
    rust_url: String,

    /// Java KairosDB service URL  
    #[arg(long, default_value = "http://localhost:8080")]
    java_url: String,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Run performance comparison between Rust and Java implementations
    Run {
        /// Test scenario to run
        #[arg(value_parser = parse_scenario)]
        scenario: String,

        /// Override metrics count
        #[arg(long)]
        metrics: Option<usize>,

        /// Override tag combinations per metric
        #[arg(long)]
        tags: Option<usize>,

        /// Override histogram sample range (min,max)
        #[arg(long, value_parser = parse_sample_range)]
        samples: Option<(usize, usize)>,

        /// Override batch size
        #[arg(long)]
        batch_size: Option<usize>,

        /// Override concurrent batches
        #[arg(long)]
        concurrent: Option<usize>,

        /// Override duration in seconds
        #[arg(long)]
        duration: Option<u64>,

        /// Run tests in parallel instead of sequential (default: sequential)
        #[arg(long)]
        parallel: bool,

        /// Warmup time before each test in seconds
        #[arg(long, default_value = "10")]
        warmup: u64,
    },

    /// Run comparison across multiple scenarios
    Suite {
        /// Skip scenarios (comma-separated list)
        #[arg(long)]
        skip: Option<String>,

        /// Only run specific scenarios (comma-separated list) 
        #[arg(long)]
        only: Option<String>,

        /// Run tests in parallel instead of sequential (default: sequential)
        #[arg(long)]
        parallel: bool,
    },

    /// List available test scenarios
    List,

    /// Validate that both services are accessible and healthy
    Check,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonResults {
    pub scenario_name: String,
    pub test_config: PerfTestConfig,
    pub rust_results: PerfTestResults,
    pub java_results: PerfTestResults,
    pub comparison: ComparisonAnalysis,
    pub timestamp: String,
    
    // Enhanced throughput measurements
    pub throughput_history: Vec<ThroughputMetrics>,
    pub throughput_analysis: ThroughputAnalysis,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonAnalysis {
    pub throughput_improvement_ratio: f64, // Rust / Java
    pub latency_improvement_ratio: f64,    // Java P95 / Rust P95 (higher is better for Rust)
    pub success_rate_rust: f64,
    pub success_rate_java: f64,
    pub winner: String,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputAnalysis {
    // Average rates during test period
    pub rust_avg_ingest_rate_dps: f64,
    pub rust_avg_e2e_rate_dps: f64,
    pub java_avg_ingest_rate_rps: f64,
    pub java_avg_e2e_rate_dps: f64,
    
    // Peak rates observed
    pub rust_peak_ingest_rate_dps: f64,
    pub rust_peak_e2e_rate_dps: f64,
    pub java_peak_ingest_rate_rps: f64,
    pub java_peak_e2e_rate_dps: f64,
    
    // Queue analysis
    pub rust_avg_queue_size: f64,
    pub rust_max_queue_size: u64,
    pub rust_queue_efficiency: f64, // ingest_rate / e2e_rate
    
    // Processing lag analysis
    pub rust_processing_lag_detected: bool,
    pub java_processing_lag_detected: bool,
    
    // Throughput comparison
    pub ingest_rate_improvement_ratio: f64,  // Rust / Java
    pub e2e_rate_improvement_ratio: f64,     // Rust / Java
}

fn parse_scenario(s: &str) -> Result<String, String> {
    if TestScenarios::by_name(s).is_some() {
        Ok(s.to_string())
    } else {
        Err(format!(
            "Unknown scenario: {}. Use 'list' command to see available scenarios.",
            s
        ))
    }
}

/// Interpolate the time when queue size would have reached the threshold
/// Uses linear interpolation between the last two data points where queue crossed the threshold
fn interpolate_drain_time(data_points: &[(f64, u64)], threshold: f64) -> u64 {
    // Find the last point above threshold and first point at/below threshold
    let mut above_threshold: Option<(f64, u64)> = None;
    let mut below_threshold: Option<(f64, u64)> = None;
    
    for &(value, timestamp) in data_points.iter() {
        if value >= threshold {
            above_threshold = Some((value, timestamp));
        } else if above_threshold.is_some() && below_threshold.is_none() {
            below_threshold = Some((value, timestamp));
            break;
        }
    }
    
    // If we have both points, interpolate
    if let (Some((v1, t1)), Some((v2, t2))) = (above_threshold, below_threshold) {
        if v1 != v2 {
            // Linear interpolation: t = t1 + (threshold - v1) * (t2 - t1) / (v2 - v1)
            let ratio = (threshold - v1) / (v2 - v1);
            let interpolated_time = t1 as f64 + ratio * (t2 as f64 - t1 as f64);
            interpolated_time as u64
        } else {
            // Values are the same, use midpoint time
            (t1 + t2) / 2
        }
    } else if let Some((_, timestamp)) = below_threshold {
        // No point above threshold found, just use the below-threshold timestamp
        timestamp
    } else {
        // Fallback to latest timestamp
        data_points.last().map(|(_, t)| *t).unwrap_or(0)
    }
}

/// Create ThroughputMetrics from PerfTestResults for analysis
fn create_throughput_metrics_from_results(results: &PerfTestResults, service_name: &str) -> ThroughputMetrics {
    ThroughputMetrics {
        timestamp: chrono::Utc::now().timestamp() as u64 * 1000, // Convert to milliseconds
        service_name: service_name.to_string(),
        ingest_rate_datapoints_per_sec: results.throughput_datapoints_per_sec,
        ingest_rate_requests_per_sec: results.throughput_requests_per_sec,
        ingest_requests_total: results.total_requests,
        ingest_datapoints_total: results.total_datapoints_sent,
        end_to_end_rate_datapoints_per_sec: results.throughput_datapoints_per_sec, // Same as ingest for test results
        cassandra_writes_total: results.total_datapoints_sent, // Approximation
        cassandra_write_errors: results.failed_requests,
        queue_size: 0, // Not available from test results
        queue_oldest_entry_age_seconds: 0.0, // Not available from test results
        success_rate_percent: if results.total_requests > 0 {
            (results.successful_requests as f64 / results.total_requests as f64) * 100.0
        } else {
            0.0
        },
        error_count: results.failed_requests,
    }
}

fn parse_sample_range(s: &str) -> Result<(usize, usize), String> {
    let parts: Vec<&str> = s.split(',').collect();
    if parts.len() != 2 {
        return Err("Sample range must be in format 'min,max'".to_string());
    }

    let min = parts[0].parse::<usize>().map_err(|_| "Invalid min value")?;
    let max = parts[1].parse::<usize>().map_err(|_| "Invalid max value")?;

    if min >= max {
        return Err("Min must be less than max".to_string());
    }

    Ok((min, max))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Setup logging
    let level = if cli.verbose {
        Level::DEBUG
    } else {
        Level::INFO
    };
    tracing_subscriber::fmt().with_max_level(level).init();

    // Create output directory
    std::fs::create_dir_all(&cli.output_dir)?;

    match cli.command {
        Commands::Run {
            scenario,
            metrics,
            tags,
            samples,
            batch_size,
            concurrent,
            duration,
            parallel,
            warmup,
        } => {
            run_comparison(
                &scenario,
                &cli.rust_url,
                &cli.java_url,
                &cli.output_dir,
                ScenarioOverrides {
                    metrics_count: metrics,
                    tag_combinations_per_metric: tags,
                    histogram_samples_per_datapoint: samples,
                    batch_size,
                    concurrent_batches: concurrent,
                    duration_seconds: duration,
                    warmup_seconds: Some(warmup),
                    ..Default::default()
                },
                !parallel, // Invert: parallel flag -> sequential behavior
            )
            .await
        }

        Commands::Suite {
            skip,
            only,
            parallel,
        } => run_comparison_suite(&cli.rust_url, &cli.java_url, &cli.output_dir, skip, only, !parallel).await,

        Commands::List => list_scenarios(),

        Commands::Check => check_services(&cli.rust_url, &cli.java_url).await,
    }
}

async fn run_comparison(
    scenario_name: &str,
    rust_url: &str,
    java_url: &str,
    output_dir: &PathBuf,
    overrides: ScenarioOverrides,
    sequential: bool,
) -> Result<()> {
    info!("Running performance comparison: Rust vs Java");
    info!("Scenario: {}", scenario_name);
    info!("Rust URL: {}", rust_url);
    info!("Java URL: {}", java_url);
    info!("Mode: {}", if sequential { "Sequential" } else { "Parallel" });

    // Get base config
    let mut rust_config = TestScenarios::custom(scenario_name, overrides.clone())
        .ok_or_else(|| anyhow::anyhow!("Unknown scenario: {}", scenario_name))?;
    rust_config.ingest_url = rust_url.to_string();

    let mut java_config = TestScenarios::custom(scenario_name, overrides)
        .ok_or_else(|| anyhow::anyhow!("Unknown scenario: {}", scenario_name))?;
    java_config.ingest_url = java_url.to_string();

    let test_start = Instant::now();

    // We'll create throughput metrics from actual test results instead of external collection
    
    let (rust_results, java_results) = if sequential {
        info!("Running tests sequentially...");
        
        info!("🦀 Starting Rust test...");
        let mut rust_runner = PerfTestRunner::new(rust_config.clone());
        let rust_results = rust_runner.run().await?;
        
        info!("✅ Rust test completed. {} datapoints in {} requests", 
              rust_results.total_datapoints_sent, rust_results.total_requests);
        
        // Wait for queues to drain before starting Java test
        info!("⏳ Waiting for Rust queues to drain...");
        wait_for_queue_drain(rust_url).await?;
        
        info!("☕ Starting Java test...");
        let mut java_runner = PerfTestRunner::new(java_config.clone());
        let java_results = java_runner.run().await?;
        
        info!("✅ Java test completed. {} datapoints in {} requests", 
              java_results.total_datapoints_sent, java_results.total_requests);
        
        // Wait for Java queues to drain for clean final state
        info!("⏳ Waiting for Java queues to drain...");
        let java_drain_timestamp = wait_for_queue_drain_java(java_url).await?;
        info!("📅 Java queue drain completed at timestamp: {}", java_drain_timestamp);
        
        (rust_results, java_results)
    } else {
        info!("Running tests in parallel...");
        
        let rust_handle = {
            let config = rust_config.clone();
            tokio::spawn(async move {
                let mut runner = PerfTestRunner::new(config);
                runner.run().await
            })
        };

        let java_handle = {
            let config = java_config.clone();
            tokio::spawn(async move {
                let mut runner = PerfTestRunner::new(config);
                runner.run().await
            })
        };

        let (rust_result, java_result) = tokio::try_join!(rust_handle, java_handle)?;
        let rust_results = rust_result?;
        let java_results = java_result?;
        
        info!("✅ Both tests completed in parallel");
        info!("   Rust: {} datapoints in {} requests", 
              rust_results.total_datapoints_sent, rust_results.total_requests);
        info!("   Java: {} datapoints in {} requests", 
              java_results.total_datapoints_sent, java_results.total_requests);
        
        (rust_results, java_results)
    };

    let total_elapsed = test_start.elapsed();
    info!("Comparison completed in {:.1}s", total_elapsed.as_secs_f64());

    // Analyze results (both traditional and throughput)
    let analysis = analyze_results(&rust_results, &java_results);
    
    // Create throughput metrics from actual test results
    let mut throughput_history = Vec::new();
    throughput_history.push(create_throughput_metrics_from_results(&rust_results, "Rust"));
    throughput_history.push(create_throughput_metrics_from_results(&java_results, "Java"));
    
    let throughput_analysis = analyze_throughput(&throughput_history);
    
    let comparison_results = ComparisonResults {
        scenario_name: scenario_name.to_string(),
        test_config: rust_config, // Use rust config as reference
        rust_results,
        java_results,
        comparison: analysis,
        timestamp: chrono::Utc::now().to_rfc3339(),
        throughput_history,
        throughput_analysis,
    };

    // Print results
    print_comparison_results(&comparison_results);

    // Save detailed report
    let report_file = output_dir.join(format!("comparison_{}_detailed.json", scenario_name));
    let json_content = serde_json::to_string_pretty(&comparison_results)?;
    std::fs::write(&report_file, json_content)?;
    println!("\n📄 Detailed comparison report saved to: {}", report_file.display());

    // Save to CSV for trending
    let csv_path = output_dir.join("comparison_trends.csv");
    save_csv_summary(&comparison_results, &csv_path)?;
    println!("📊 CSV summary updated: {}", csv_path.display());

    Ok(())
}

async fn run_comparison_suite(
    rust_url: &str,
    java_url: &str,
    output_dir: &PathBuf,
    skip: Option<String>,
    only: Option<String>,
    sequential: bool,
) -> Result<()> {
    info!("Running comparison test suite");

    let skip_list: Vec<String> = skip
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let only_list: Vec<String> = only
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let _suite_results: Vec<ComparisonResults> = Vec::new();

    for (name, _config) in TestScenarios::all_scenarios() {
        if !skip_list.is_empty() && skip_list.contains(&name.to_string()) {
            info!("Skipping scenario: {}", name);
            continue;
        }

        if !only_list.is_empty() && !only_list.contains(&name.to_string()) {
            info!("Skipping scenario (not in only list): {}", name);
            continue;
        }

        println!("\n{}", "=".repeat(60));
        println!("🏃 Running scenario: {}", name);
        println!("{}", "=".repeat(60));

        match run_comparison(
            &name,
            rust_url,
            java_url,
            output_dir,
            ScenarioOverrides::default(),
            sequential,
        )
        .await
        {
            Ok(()) => {
                info!("✅ Scenario {} completed successfully", name);
            }
            Err(e) => {
                eprintln!("❌ Scenario {} failed: {}", name, e);
            }
        }

        // Brief pause between scenarios in suite mode
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    println!("\n{}", "=".repeat(60));
    println!("🎯 Comparison Suite Complete");
    println!("{}", "=".repeat(60));
    println!("📄 Individual reports saved to: {}", output_dir.display());
    println!("📊 Trending data: {}/comparison_trends.csv", output_dir.display());

    Ok(())
}

fn analyze_results(rust_results: &PerfTestResults, java_results: &PerfTestResults) -> ComparisonAnalysis {
    let throughput_ratio = rust_results.throughput_datapoints_per_sec / java_results.throughput_datapoints_per_sec;
    let latency_ratio = java_results.latency_stats.p95_ms / rust_results.latency_stats.p95_ms;
    
    let rust_success_rate = rust_results.successful_requests as f64 / rust_results.total_requests as f64 * 100.0;
    let java_success_rate = java_results.successful_requests as f64 / java_results.total_requests as f64 * 100.0;

    // Determine winner based on throughput and success rate
    let winner = if rust_success_rate > 95.0 && java_success_rate > 95.0 {
        // Both have good success rates, compare throughput
        if throughput_ratio > 1.1 {
            "Rust".to_string()
        } else if throughput_ratio < 0.9 {
            "Java".to_string()
        } else {
            "Tie".to_string()
        }
    } else if rust_success_rate > java_success_rate + 5.0 {
        "Rust".to_string()
    } else if java_success_rate > rust_success_rate + 5.0 {
        "Java".to_string()
    } else {
        "Inconclusive".to_string()
    };

    let summary = format!(
        "Rust throughput: {:.1}x Java, Latency: {:.1}x better than Java, Success rates: Rust {:.1}% vs Java {:.1}%",
        throughput_ratio, latency_ratio, rust_success_rate, java_success_rate
    );

    ComparisonAnalysis {
        throughput_improvement_ratio: throughput_ratio,
        latency_improvement_ratio: latency_ratio,
        success_rate_rust: rust_success_rate,
        success_rate_java: java_success_rate,
        winner,
        summary,
    }
}

fn print_comparison_results(results: &ComparisonResults) {
    println!("\n{}", "=".repeat(60));
    println!("🏁 Performance Comparison Results");
    println!("{}", "=".repeat(60));
    
    println!("📋 Scenario: {}", results.scenario_name);
    println!("⏱️  Timestamp: {}", results.timestamp);
    
    println!("\n🦀 Rust Results:");
    print_service_results(&results.rust_results);
    
    println!("\n☕ Java Results:");
    print_service_results(&results.java_results);
    
    println!("\n📊 Traditional Performance Analysis:");
    println!("🏆 Winner: {}", results.comparison.winner);
    println!("📈 Throughput Ratio (Rust/Java): {:.2}x", results.comparison.throughput_improvement_ratio);
    println!("⚡ Latency Improvement (Java P95 / Rust P95): {:.2}x", results.comparison.latency_improvement_ratio);
    println!("✅ Success Rate - Rust: {:.1}%", results.comparison.success_rate_rust);
    println!("✅ Success Rate - Java: {:.1}%", results.comparison.success_rate_java);
    println!("📝 Summary: {}", results.comparison.summary);
    
    // Enhanced throughput analysis
    println!("\n🔍 Detailed Throughput Analysis:");
    print_throughput_analysis(&results.throughput_analysis);
    
    println!("\n{}", "=".repeat(60));
}

fn print_service_results(results: &PerfTestResults) {
    println!("  📊 Total Requests: {} ({} success, {} failed)", 
             results.total_requests, results.successful_requests, results.failed_requests);
    println!("  📈 Throughput: {:.1} datapoints/sec, {:.1} requests/sec",
             results.throughput_datapoints_per_sec, results.throughput_requests_per_sec);
    println!("  ⏱️  Latency - P95: {:.1}ms, P99: {:.1}ms, Mean: {:.1}ms",
             results.latency_stats.p95_ms, results.latency_stats.p99_ms, results.latency_stats.mean_ms);
    println!("  ✅ Success Rate: {:.1}%", 
             (results.successful_requests as f64 / results.total_requests as f64) * 100.0);
}

fn print_throughput_analysis(analysis: &ThroughputAnalysis) {
    println!("  🦀 Rust Service Throughput:");
    println!("    📥 Ingest Rate: {:.1} dps/s avg, {:.1} dps/s peak", 
             analysis.rust_avg_ingest_rate_dps, analysis.rust_peak_ingest_rate_dps);
    println!("    📤 End-to-End Rate: {:.1} dps/s avg, {:.1} dps/s peak", 
             analysis.rust_avg_e2e_rate_dps, analysis.rust_peak_e2e_rate_dps);
    println!("    📊 Queue: {:.0} avg size, {} max size", 
             analysis.rust_avg_queue_size, analysis.rust_max_queue_size);
    println!("    ⚡ Efficiency: {:.2} (ingest/e2e ratio)", analysis.rust_queue_efficiency);
    if analysis.rust_processing_lag_detected {
        println!("    ⚠️  Processing lag detected (queue growing faster than draining)");
    }
    
    println!("  ☕ Java Service Throughput:");
    println!("    📥 Ingest Rate: {:.1} req/s avg, {:.1} req/s peak", 
             analysis.java_avg_ingest_rate_rps, analysis.java_peak_ingest_rate_rps);
    println!("    📤 End-to-End Rate: {:.1} dps/s avg, {:.1} dps/s peak", 
             analysis.java_avg_e2e_rate_dps, analysis.java_peak_e2e_rate_dps);
    if analysis.java_processing_lag_detected {
        println!("    ⚠️  Processing lag detected");
    }
    
    println!("  🔄 Throughput Comparisons:");
    println!("    📈 Ingest Rate Improvement: {:.2}x (Rust vs Java)", 
             analysis.ingest_rate_improvement_ratio);
    println!("    📈 End-to-End Rate Improvement: {:.2}x (Rust vs Java)", 
             analysis.e2e_rate_improvement_ratio);
    
    // Performance insights
    if analysis.ingest_rate_improvement_ratio > 1.5 {
        println!("    ✅ Rust shows significant ingest rate advantage");
    } else if analysis.ingest_rate_improvement_ratio < 0.8 {
        println!("    ⚠️  Java shows better ingest rate performance");
    }
    
    if analysis.e2e_rate_improvement_ratio > 1.2 {
        println!("    ✅ Rust shows better end-to-end throughput");
    } else if analysis.e2e_rate_improvement_ratio < 0.9 {
        println!("    ⚠️  Java shows better end-to-end throughput");
    }
}

fn save_csv_summary(results: &ComparisonResults, csv_path: &PathBuf) -> Result<()> {
    let file_exists = csv_path.exists();
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(!file_exists)
        .from_writer(std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(csv_path)?);

    // Write header if new file
    if !file_exists {
        wtr.write_record(&[
            "timestamp", "scenario", "winner", 
            "rust_throughput_dps", "java_throughput_dps", "throughput_ratio",
            "rust_p95_latency", "java_p95_latency", "latency_ratio",
            "rust_success_rate", "java_success_rate",
            "rust_requests", "java_requests",
            // Enhanced throughput metrics
            "rust_ingest_rate_dps", "rust_e2e_rate_dps", 
            "java_ingest_rate_rps", "java_e2e_rate_dps",
            "ingest_improvement_ratio", "e2e_improvement_ratio",
            "rust_queue_size_avg", "rust_queue_efficiency"
        ])?;
    }

    wtr.write_record(&[
        &results.timestamp,
        &results.scenario_name,
        &results.comparison.winner,
        &results.rust_results.throughput_datapoints_per_sec.to_string(),
        &results.java_results.throughput_datapoints_per_sec.to_string(),
        &results.comparison.throughput_improvement_ratio.to_string(),
        &results.rust_results.latency_stats.p95_ms.to_string(),
        &results.java_results.latency_stats.p95_ms.to_string(),
        &results.comparison.latency_improvement_ratio.to_string(),
        &results.comparison.success_rate_rust.to_string(),
        &results.comparison.success_rate_java.to_string(),
        &results.rust_results.total_requests.to_string(),
        &results.java_results.total_requests.to_string(),
        // Enhanced throughput metrics
        &results.throughput_analysis.rust_avg_ingest_rate_dps.to_string(),
        &results.throughput_analysis.rust_avg_e2e_rate_dps.to_string(),
        &results.throughput_analysis.java_avg_ingest_rate_rps.to_string(),
        &results.throughput_analysis.java_avg_e2e_rate_dps.to_string(),
        &results.throughput_analysis.ingest_rate_improvement_ratio.to_string(),
        &results.throughput_analysis.e2e_rate_improvement_ratio.to_string(),
        &results.throughput_analysis.rust_avg_queue_size.to_string(),
        &results.throughput_analysis.rust_queue_efficiency.to_string(),
    ])?;

    wtr.flush()?;
    Ok(())
}

fn list_scenarios() -> Result<()> {
    println!("📋 Available Performance Test Scenarios for Comparison:\n");

    for (i, scenario) in TestScenarios::list_scenarios().iter().enumerate() {
        println!("{}. {}", i + 1, scenario);
    }

    println!("\n💡 Usage examples:");
    println!("  perf_compare run small_scale                    # Quick comparison (sequential by default)");
    println!("  perf_compare run large_scale --duration 300     # 5-minute load test");
    println!("  perf_compare run stress_test --parallel         # Run tests simultaneously");
    println!("  perf_compare suite --skip stress_test           # Run all except stress test");
    println!("  perf_compare check                              # Validate both services are healthy");

    Ok(())
}

async fn check_services(rust_url: &str, java_url: &str) -> Result<()> {
    info!("Checking service health...");

    let client = reqwest::Client::new();

    // Check Rust service
    print!("🦀 Rust service ({})... ", rust_url);
    match client
        .get(&format!("{}/health", rust_url))
        .timeout(Duration::from_secs(5))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => println!("✅ Healthy"),
        Ok(resp) => println!("❌ Unhealthy (status: {})", resp.status()),
        Err(e) => println!("❌ Unreachable ({})", e),
    }

    // Check Java service
    print!("☕ Java service ({})... ", java_url);
    match client
        .get(&format!("{}/api/v1/health/check", java_url))
        .timeout(Duration::from_secs(5))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => println!("✅ Healthy"),
        Ok(resp) => println!("❌ Unhealthy (status: {})", resp.status()),
        Err(e) => println!("❌ Unreachable ({})", e),
    }

    println!("\n💡 If services are unhealthy, make sure Tilt is running:");
    println!("   tilt up");
    println!("   # Wait for all services to be ready, then try again");

    Ok(())
}

/// Analyze throughput metrics history to provide insights
fn analyze_throughput(throughput_history: &[ThroughputMetrics]) -> ThroughputAnalysis {
    let rust_metrics: Vec<_> = throughput_history.iter()
        .filter(|m| m.service_name == "Rust")
        .collect();
    let java_metrics: Vec<_> = throughput_history.iter()
        .filter(|m| m.service_name == "Java")
        .collect();
    
    // Calculate averages and peaks for Rust
    let (rust_avg_ingest_rate, rust_peak_ingest_rate) = if !rust_metrics.is_empty() {
        let avg = rust_metrics.iter().map(|m| m.ingest_rate_datapoints_per_sec).sum::<f64>() / rust_metrics.len() as f64;
        let peak = rust_metrics.iter().map(|m| m.ingest_rate_datapoints_per_sec).fold(0.0, f64::max);
        (avg, peak)
    } else {
        (0.0, 0.0)
    };
    
    let (rust_avg_e2e_rate, rust_peak_e2e_rate) = if !rust_metrics.is_empty() {
        let avg = rust_metrics.iter().map(|m| m.end_to_end_rate_datapoints_per_sec).sum::<f64>() / rust_metrics.len() as f64;
        let peak = rust_metrics.iter().map(|m| m.end_to_end_rate_datapoints_per_sec).fold(0.0, f64::max);
        (avg, peak)
    } else {
        (0.0, 0.0)
    };
    
    // Calculate averages and peaks for Java
    let (java_avg_ingest_rate, java_peak_ingest_rate) = if !java_metrics.is_empty() {
        let avg = java_metrics.iter().map(|m| m.ingest_rate_requests_per_sec).sum::<f64>() / java_metrics.len() as f64;
        let peak = java_metrics.iter().map(|m| m.ingest_rate_requests_per_sec).fold(0.0, f64::max);
        (avg, peak)
    } else {
        (0.0, 0.0)
    };
    
    let (java_avg_e2e_rate, java_peak_e2e_rate) = if !java_metrics.is_empty() {
        let avg = java_metrics.iter().map(|m| m.end_to_end_rate_datapoints_per_sec).sum::<f64>() / java_metrics.len() as f64;
        let peak = java_metrics.iter().map(|m| m.end_to_end_rate_datapoints_per_sec).fold(0.0, f64::max);
        (avg, peak)
    } else {
        (0.0, 0.0)
    };
    
    // Queue analysis for Rust
    let rust_avg_queue_size = if !rust_metrics.is_empty() {
        rust_metrics.iter().map(|m| m.queue_size as f64).sum::<f64>() / rust_metrics.len() as f64
    } else {
        0.0
    };
    
    let rust_max_queue_size = rust_metrics.iter().map(|m| m.queue_size).max().unwrap_or(0);
    
    let rust_queue_efficiency = if rust_avg_e2e_rate > 0.0 {
        rust_avg_ingest_rate / rust_avg_e2e_rate
    } else {
        1.0
    };
    
    // Processing lag detection
    let rust_processing_lag_detected = rust_queue_efficiency > 1.2; // Ingest significantly faster than processing
    let java_processing_lag_detected = false; // Can't easily detect from Java metrics
    
    // Throughput improvement ratios
    let ingest_rate_improvement_ratio = if java_avg_ingest_rate > 0.0 {
        rust_avg_ingest_rate / java_avg_ingest_rate
    } else {
        1.0
    };
    
    let e2e_rate_improvement_ratio = if java_avg_e2e_rate > 0.0 {
        rust_avg_e2e_rate / java_avg_e2e_rate  
    } else {
        1.0
    };
    
    ThroughputAnalysis {
        rust_avg_ingest_rate_dps: rust_avg_ingest_rate,
        rust_avg_e2e_rate_dps: rust_avg_e2e_rate,
        java_avg_ingest_rate_rps: java_avg_ingest_rate,
        java_avg_e2e_rate_dps: java_avg_e2e_rate,
        rust_peak_ingest_rate_dps: rust_peak_ingest_rate,
        rust_peak_e2e_rate_dps: rust_peak_e2e_rate,
        java_peak_ingest_rate_rps: java_peak_ingest_rate,
        java_peak_e2e_rate_dps: java_peak_e2e_rate,
        rust_avg_queue_size: rust_avg_queue_size,
        rust_max_queue_size,
        rust_queue_efficiency,
        rust_processing_lag_detected,
        java_processing_lag_detected,
        ingest_rate_improvement_ratio,
        e2e_rate_improvement_ratio,
    }
}