use kairosdb_e2e_tests::performance::*;
use tokio::test;

#[test]
async fn test_small_performance_scenario() {
    let config = TestScenarios::small_scale();

    // Verify config values
    assert_eq!(config.metrics_count, 50);
    assert_eq!(config.duration_seconds, 30);
    assert_eq!(config.batch_size, 25);

    // Test generator creation
    let generator_config = GeneratorConfig {
        metrics_count: config.metrics_count,
        tag_combinations_per_metric: config.tag_combinations_per_metric,
        histogram_samples_range: config.histogram_samples_per_datapoint,
        tag_cardinality_limit: config.tag_cardinality_limit,
    };

    let generator = MetricDataGenerator::new(generator_config);
    let batch = generator.generate_batch(10).await;

    assert!(!batch.is_empty());
    assert!(batch.len() <= 10);
}

#[test]
async fn test_metric_data_generation() {
    let config = GeneratorConfig {
        metrics_count: 5,
        tag_combinations_per_metric: 3,
        histogram_samples_range: (10, 50),
        tag_cardinality_limit: 5,
    };

    let generator = MetricDataGenerator::new(config);
    let batch = generator.generate_batch(15).await;

    // Verify we got histogram data
    for metric_group in &batch {
        for datapoint in metric_group {
            assert!(datapoint.get("name").is_some());
            assert!(datapoint.get("datapoints").is_some());
            assert!(datapoint.get("tags").is_some());
        }
    }
}

#[test]
async fn test_scenario_selection() {
    let small = TestScenarios::by_name("small_scale");
    assert!(small.is_some());

    let medium = TestScenarios::by_name("medium");
    assert!(medium.is_some());

    let invalid = TestScenarios::by_name("invalid");
    assert!(invalid.is_none());
}

#[test]
async fn test_custom_scenario_overrides() {
    let overrides = ScenarioOverrides {
        metrics_count: Some(1000),
        duration_seconds: Some(120),
        ..Default::default()
    };

    let config = TestScenarios::custom("small_scale", overrides);
    assert!(config.is_some());

    let config = config.unwrap();
    assert_eq!(config.metrics_count, 1000);
    assert_eq!(config.duration_seconds, 120);
    assert_eq!(config.tag_combinations_per_metric, 10); // should be original value
}
