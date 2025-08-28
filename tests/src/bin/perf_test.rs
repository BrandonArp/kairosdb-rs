#![allow(clippy::single_component_path_imports)]
#![allow(clippy::ptr_arg)]

use anyhow::Result;
use clap::{Parser, Subcommand};
use kairosdb_e2e_tests::performance::*;
use reqwest::Client;
use std::path::PathBuf;
use std::time::Duration;
use tracing::{info, Level};
use tracing_subscriber;

/// KairosDB Performance Testing CLI
#[derive(Parser)]
#[command(name = "perf_test")]
#[command(about = "KairosDB performance testing and benchmarking tool")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Output directory for reports
    #[arg(short, long, default_value = "target/perf_reports")]
    output_dir: PathBuf,

    /// Ingest service URL
    #[arg(short, long, default_value = "http://localhost:8081")]
    url: String,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,

    /// Set ingestion service performance mode
    #[arg(long, value_parser = ["no_parse", "parse_only", "parse_and_store"])]
    perf_mode: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a single performance test scenario
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

        /// Override performance mode
        #[arg(long, value_parser = ["no_parse", "parse_only", "parse_and_store"])]
        performance_mode: Option<String>,
    },

    /// Run all predefined test scenarios
    Suite {
        /// Skip scenarios (comma-separated list)
        #[arg(long)]
        skip: Option<String>,

        /// Only run specific scenarios (comma-separated list)
        #[arg(long)]
        only: Option<String>,
    },

    /// List available test scenarios
    List,

    /// Generate sample configuration file
    Config,

    /// Run continuous performance monitoring
    Monitor {
        /// Monitoring interval in seconds
        #[arg(long, default_value = "60")]
        interval: u64,

        /// Max number of iterations (0 for infinite)
        #[arg(long, default_value = "0")]
        iterations: u32,

        /// Scenario to run for monitoring
        #[arg(default_value = "small_scale")]
        scenario: String,
    },
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
            performance_mode,
        } => {
            run_single_scenario(
                &scenario,
                &cli.url,
                &cli.output_dir,
                ScenarioOverrides {
                    ingest_url: Some(cli.url.clone()),
                    metrics_count: metrics,
                    tag_combinations_per_metric: tags,
                    histogram_samples_per_datapoint: samples,
                    batch_size,
                    concurrent_batches: concurrent,
                    duration_seconds: duration,
                    performance_mode,
                    ..Default::default()
                },
            )
            .await
        }

        Commands::Suite { skip, only } => {
            run_test_suite(&cli.url, &cli.output_dir, skip, only).await
        }

        Commands::List => list_scenarios(),

        Commands::Config => generate_config_file(),

        Commands::Monitor {
            interval,
            iterations,
            scenario,
        } => run_monitoring(&scenario, &cli.url, &cli.output_dir, interval, iterations).await,
    }
}

/// Wait for Rust service queue to drain after performance test completion
/// Tracks queue processing metrics and resets timeout when progress is observed
async fn wait_for_queue_drain(rust_url: &str) -> Result<()> {
    use std::time::Instant;
    
    let client = Client::new();
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
            .get(&format!("{}/api/v1/metrics", rust_url))
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(response) => {
                if let Ok(text) = response.text().await {
                    if let Ok(metrics) = serde_json::from_str::<serde_json::Value>(&text) {
                        if let Some(queue_size) = metrics.get("queue_size").and_then(|v| v.as_u64()) {
                            // Track initial and maximum queue sizes
                            if initial_queue_size.is_none() {
                                initial_queue_size = Some(queue_size);
                                info!("📊 Starting queue drain monitoring (initial size: {})", queue_size);
                            }
                            max_observed_size = max_observed_size.max(queue_size);
                            
                            info!("📊 Queue size: {}", queue_size);
                            
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
                                
                                info!("✅ Queue drained successfully (size: {} < 100)", queue_size);
                                info!("📈 Queue processing metrics:");
                                info!("   - Initial size: {}", initial_size);
                                info!("   - Peak size: {}", max_observed_size);
                                info!("   - Items processed: {}", processed_items);
                                info!("   - Processing time: {:.1}s", elapsed.as_secs_f64());
                                info!("   - Processing rate: {:.1} items/sec", processing_rate);
                                info!("   - Total checks: {}", total_checks);
                                return Ok(());
                            }
                        }
                    }
                }
            }
            Err(e) => {
                info!("⚠️  Could not check queue status: {} (attempt {} since progress)", 
                      e, attempts_since_progress);
            }
        }
        
        if attempts_since_progress >= max_attempts_without_progress {
            let elapsed = start_time.elapsed();
            let initial_size = initial_queue_size.unwrap_or(0);
            let current_size = last_queue_size.unwrap_or(0);
            let processed_items = max_observed_size.saturating_sub(current_size);
            
            info!("⚠️  Queue drain timeout after {} attempts without progress", max_attempts_without_progress);
            info!("📊 Partial processing metrics:");
            info!("   - Initial size: {}", initial_size);
            info!("   - Peak size: {}", max_observed_size);
            info!("   - Current size: {}", current_size);
            info!("   - Items processed: {}", processed_items);
            info!("   - Processing time: {:.1}s", elapsed.as_secs_f64());
            info!("   - Total checks: {}", total_checks);
            return Ok(()); // Continue anyway
        }
        
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn run_single_scenario(
    scenario_name: &str,
    url: &str,
    output_dir: &PathBuf,
    overrides: ScenarioOverrides,
) -> Result<()> {
    info!("Running performance test scenario: {}", scenario_name);

    let mut config = TestScenarios::custom(scenario_name, overrides)
        .ok_or_else(|| anyhow::anyhow!("Unknown scenario: {}", scenario_name))?;
    
    // Set the ingest URL from parameter
    config.ingest_url = url.to_string();

    let mut runner = PerfTestRunner::new(config.clone());
    let results = runner.run().await?;
    
    // Wait for queue to drain for accurate test completion timing
    info!("⏳ Waiting for queue to drain...");
    wait_for_queue_drain(url).await?;

    let reporter = PerfTestReporter::new(scenario_name.to_string(), config);
    reporter.print_results(&results);

    let report_file = reporter.save_to_file(&results, output_dir)?;
    println!("\n📄 Detailed report saved to: {}", report_file);

    let csv_path = output_dir.join("performance_trends.csv");
    reporter.save_csv_summary(&results, &csv_path)?;
    println!("📊 CSV summary updated: {}", csv_path.display());

    Ok(())
}

async fn run_test_suite(
    url: &str,
    output_dir: &PathBuf,
    skip: Option<String>,
    only: Option<String>,
) -> Result<()> {
    info!("Running performance test suite");

    let mut suite = PerfTestSuite::new(output_dir);

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

    for (name, mut config) in TestScenarios::all_scenarios() {
        if !skip_list.is_empty() && skip_list.contains(&name.to_string()) {
            info!("Skipping scenario: {}", name);
            continue;
        }

        if !only_list.is_empty() && !only_list.contains(&name.to_string()) {
            info!("Skipping scenario (not in only list): {}", name);
            continue;
        }

        config.ingest_url = url.to_string();
        suite.add_scenario(name.to_string(), config);
    }

    let results = suite.run_all_with_queue_monitoring(url).await?;
    suite.print_suite_summary(&results);

    println!("\n📄 Individual reports saved to: {}", output_dir.display());
    println!(
        "📊 Trending data: {}/performance_trends.csv",
        output_dir.display()
    );

    Ok(())
}

fn list_scenarios() -> Result<()> {
    println!("📋 Available Performance Test Scenarios:\n");

    for (i, scenario) in TestScenarios::list_scenarios().iter().enumerate() {
        println!("{}. {}", i + 1, scenario);
    }

    println!("\n💡 Usage examples:");
    println!("  perf_test run small_scale                    # Run quick test");
    println!("  perf_test run large_scale --duration 600     # Run 10-minute load test");
    println!("  perf_test run stress_test --url http://prod-kairosdb:8081");
    println!("  perf_test suite --skip stress_test           # Run all except stress test");
    println!("  perf_test monitor --interval 300 medium_scale # Monitor every 5 minutes");

    Ok(())
}

fn generate_config_file() -> Result<()> {
    let config_example = r#"# KairosDB Performance Test Configuration Example
# Copy this file and modify as needed

[test_config]
ingest_url = "http://localhost:8081"
metrics_count = 1000
tag_combinations_per_metric = 50
histogram_samples_per_datapoint = [100, 1000]
batch_size = 200
concurrent_batches = 10
duration_seconds = 300
tag_cardinality_limit = 25
warmup_seconds = 15

[scenarios.custom_load_test]
metrics_count = 2000
tag_combinations_per_metric = 75
duration_seconds = 600

[scenarios.memory_test]  
histogram_samples_per_datapoint = [1000, 5000]
batch_size = 100
concurrent_batches = 5
"#;

    let config_path = "perf_test_config.toml";
    std::fs::write(config_path, config_example)?;

    println!("📄 Sample configuration file generated: {}", config_path);
    println!("Edit this file and use --config to load custom settings");

    Ok(())
}

async fn run_monitoring(
    scenario_name: &str,
    url: &str,
    output_dir: &PathBuf,
    interval_seconds: u64,
    max_iterations: u32,
) -> Result<()> {
    info!("Starting continuous performance monitoring");
    info!(
        "Scenario: {}, Interval: {}s",
        scenario_name, interval_seconds
    );

    let mut config = TestScenarios::by_name(scenario_name)
        .ok_or_else(|| anyhow::anyhow!("Unknown scenario: {}", scenario_name))?;
    config.ingest_url = url.to_string();

    let mut iteration = 0;

    loop {
        iteration += 1;
        if max_iterations > 0 && iteration > max_iterations {
            break;
        }

        println!(
            "\n🔄 Monitoring iteration {} at {}",
            iteration,
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        );

        let mut runner = PerfTestRunner::new(config.clone());

        match runner.run().await {
            Ok(results) => {
                let reporter = PerfTestReporter::new(
                    format!("{}_monitor_{}", scenario_name, iteration),
                    config.clone(),
                );

                // Quick summary for monitoring
                println!(
                    "✅ Success: {:.1}%, DP/s: {:.0}, P95: {:.1}ms",
                    (results.successful_requests as f64 / results.total_requests as f64) * 100.0,
                    results.throughput_datapoints_per_sec,
                    results.latency_stats.p95_ms
                );

                // Save to trending data
                let csv_path = output_dir.join("monitoring_trends.csv");
                if let Err(e) = reporter.save_csv_summary(&results, &csv_path) {
                    eprintln!("⚠️  Failed to save monitoring data: {}", e);
                }
            }
            Err(e) => {
                eprintln!("❌ Monitoring iteration {} failed: {}", iteration, e);
            }
        }

        if max_iterations == 0 || iteration < max_iterations {
            tokio::time::sleep(tokio::time::Duration::from_secs(interval_seconds)).await;
        }
    }

    println!("\n✅ Monitoring completed after {} iterations", iteration);
    Ok(())
}
