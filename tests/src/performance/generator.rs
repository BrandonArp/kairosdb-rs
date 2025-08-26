use rand::prelude::*;
use rand::rng;
use rand_distr::{Distribution, LogNormal, Normal};
use serde_json::{json, Value};
use std::collections::HashMap;

/// Configuration for metric data generation
#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    pub metrics_count: usize,
    pub tag_combinations_per_metric: usize,
    pub histogram_samples_range: (usize, usize),
    pub tag_cardinality_limit: usize,
}

/// Efficient metric data generator optimized for histogram-heavy workloads
pub struct MetricDataGenerator {
    config: GeneratorConfig,
    metric_templates: Vec<MetricTemplate>,
    tag_pools: TagPools,
}

#[derive(Debug, Clone)]
struct MetricTemplate {
    name: String,
    metric_type: MetricType,
    tag_combinations: Vec<HashMap<String, String>>,
}

#[derive(Debug, Clone)]
enum MetricType {
    Histogram { boundaries: Vec<f64> },
    Counter,
    Gauge,
    Timer,
}

/// Pre-generated tag pools for efficient tag combination generation
#[derive(Debug, Clone)]
struct TagPools {
    services: Vec<String>,
    environments: Vec<String>,
    regions: Vec<String>,
    instances: Vec<String>,
    methods: Vec<String>,
    endpoints: Vec<String>,
    status_codes: Vec<String>,
    versions: Vec<String>,
}

impl TagPools {
    fn new() -> Self {
        Self {
            services: vec![
                "user-service",
                "order-service",
                "payment-service",
                "inventory-service",
                "notification-service",
                "auth-service",
                "search-service",
                "recommendation-service",
                "billing-service",
                "analytics-service",
                "logging-service",
                "monitoring-service",
            ]
            .into_iter()
            .map(String::from)
            .collect(),

            environments: vec!["prod", "staging", "dev", "canary", "test"]
                .into_iter()
                .map(String::from)
                .collect(),

            regions: vec![
                "us-east-1",
                "us-west-2",
                "eu-west-1",
                "ap-southeast-1",
                "us-central-1",
            ]
            .into_iter()
            .map(String::from)
            .collect(),

            instances: (0..100).map(|i| format!("instance-{:03}", i)).collect(),

            methods: vec!["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]
                .into_iter()
                .map(String::from)
                .collect(),

            endpoints: vec![
                "/api/v1/users",
                "/api/v1/orders",
                "/api/v1/payments",
                "/api/v1/products",
                "/api/v1/search",
                "/api/v1/recommendations",
                "/api/v1/analytics",
                "/api/v1/health",
                "/api/v2/users",
                "/api/v2/orders",
                "/api/v2/payments",
                "/api/v2/products",
            ]
            .into_iter()
            .map(String::from)
            .collect(),

            status_codes: vec![
                "200", "201", "400", "401", "403", "404", "429", "500", "502", "503",
            ]
            .into_iter()
            .map(String::from)
            .collect(),

            versions: vec!["1.0.0", "1.1.0", "1.2.0", "2.0.0", "2.1.0", "3.0.0"]
                .into_iter()
                .map(String::from)
                .collect(),
        }
    }
}

impl MetricDataGenerator {
    pub fn new(config: GeneratorConfig) -> Self {
        let mut generator = Self {
            config,
            metric_templates: Vec::new(),
            tag_pools: TagPools::new(),
        };

        generator.generate_metric_templates();
        generator
    }

    /// Get the configuration (for cloning to other tasks)
    pub fn get_config(&self) -> &GeneratorConfig {
        &self.config
    }

    fn generate_metric_templates(&mut self) {
        let metric_types = [
            (
                "http_request_duration",
                MetricType::Histogram {
                    boundaries: vec![
                        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
                    ],
                },
            ),
            (
                "http_request_size",
                MetricType::Histogram {
                    boundaries: vec![
                        128.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0, 32768.0,
                        65536.0,
                    ],
                },
            ),
            (
                "database_query_duration",
                MetricType::Histogram {
                    boundaries: vec![
                        0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0,
                    ],
                },
            ),
            (
                "memory_usage",
                MetricType::Histogram {
                    boundaries: vec![
                        10.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0, 10000.0,
                    ],
                },
            ),
            (
                "cpu_utilization",
                MetricType::Histogram {
                    boundaries: vec![5.0, 10.0, 20.0, 30.0, 50.0, 70.0, 85.0, 95.0],
                },
            ),
            (
                "cache_hit_ratio",
                MetricType::Histogram {
                    boundaries: vec![0.1, 0.2, 0.5, 0.7, 0.8, 0.9, 0.95, 0.99, 1.0],
                },
            ),
            (
                "queue_depth",
                MetricType::Histogram {
                    boundaries: vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0],
                },
            ),
            (
                "network_latency",
                MetricType::Histogram {
                    boundaries: vec![1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0],
                },
            ),
            ("request_count", MetricType::Counter),
            ("active_connections", MetricType::Gauge),
            ("response_time", MetricType::Timer),
        ];

        for i in 0..self.config.metrics_count {
            let (base_name, metric_type) = &metric_types[i % metric_types.len()];
            let metric_name = if self.config.metrics_count > metric_types.len() {
                format!("{}_{}", base_name, i / metric_types.len())
            } else {
                base_name.to_string()
            };

            let tag_combinations = self.generate_tag_combinations_for_metric(&metric_name);

            self.metric_templates.push(MetricTemplate {
                name: metric_name,
                metric_type: metric_type.clone(),
                tag_combinations,
            });
        }
    }

    fn generate_tag_combinations_for_metric(
        &mut self,
        metric_name: &str,
    ) -> Vec<HashMap<String, String>> {
        let mut rng = rng();
        let mut combinations = Vec::new();
        let pools = &self.tag_pools;

        // Generate consistent tag combinations based on metric type
        let tag_strategy = match metric_name {
            name if name.contains("http") => TagStrategy::Http,
            name if name.contains("database") => TagStrategy::Database,
            name if name.contains("memory") || name.contains("cpu") => TagStrategy::System,
            name if name.contains("cache") => TagStrategy::Cache,
            name if name.contains("queue") => TagStrategy::Queue,
            name if name.contains("network") => TagStrategy::Network,
            _ => TagStrategy::Generic,
        };

        for _ in 0..self.config.tag_combinations_per_metric {
            let mut tags = HashMap::new();

            match tag_strategy {
                TagStrategy::Http => {
                    tags.insert(
                        "service".to_string(),
                        pools.services.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "environment".to_string(),
                        pools.environments.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "method".to_string(),
                        pools.methods.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "endpoint".to_string(),
                        pools.endpoints.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "status_code".to_string(),
                        pools.status_codes.choose(&mut rng).unwrap().clone(),
                    );
                    if rng.random_bool(0.7) {
                        tags.insert(
                            "region".to_string(),
                            pools.regions.choose(&mut rng).unwrap().clone(),
                        );
                    }
                }
                TagStrategy::Database => {
                    tags.insert(
                        "service".to_string(),
                        pools.services.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "environment".to_string(),
                        pools.environments.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "table".to_string(),
                        format!("table_{}", rng.random_range(1..=20)),
                    );
                    tags.insert(
                        "operation".to_string(),
                        ["SELECT", "INSERT", "UPDATE", "DELETE"]
                            .choose(&mut rng)
                            .unwrap()
                            .to_string(),
                    );
                    if rng.random_bool(0.8) {
                        tags.insert(
                            "region".to_string(),
                            pools.regions.choose(&mut rng).unwrap().clone(),
                        );
                    }
                }
                TagStrategy::System => {
                    tags.insert(
                        "service".to_string(),
                        pools.services.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "environment".to_string(),
                        pools.environments.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "instance".to_string(),
                        pools.instances.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "region".to_string(),
                        pools.regions.choose(&mut rng).unwrap().clone(),
                    );
                    if rng.random_bool(0.6) {
                        tags.insert(
                            "version".to_string(),
                            pools.versions.choose(&mut rng).unwrap().clone(),
                        );
                    }
                }
                TagStrategy::Cache => {
                    tags.insert(
                        "service".to_string(),
                        pools.services.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "environment".to_string(),
                        pools.environments.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "cache_type".to_string(),
                        ["redis", "memcached", "local"]
                            .choose(&mut rng)
                            .unwrap()
                            .to_string(),
                    );
                    tags.insert(
                        "cache_level".to_string(),
                        ["l1", "l2", "l3"].choose(&mut rng).unwrap().to_string(),
                    );
                }
                TagStrategy::Queue => {
                    tags.insert(
                        "service".to_string(),
                        pools.services.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "environment".to_string(),
                        pools.environments.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "queue_name".to_string(),
                        format!("queue_{}", rng.random_range(1..=10)),
                    );
                    tags.insert(
                        "queue_type".to_string(),
                        ["kafka", "rabbitmq", "sqs"]
                            .choose(&mut rng)
                            .unwrap()
                            .to_string(),
                    );
                }
                TagStrategy::Network => {
                    tags.insert(
                        "service".to_string(),
                        pools.services.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "environment".to_string(),
                        pools.environments.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "source_region".to_string(),
                        pools.regions.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "target_region".to_string(),
                        pools.regions.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "protocol".to_string(),
                        ["http", "grpc", "tcp", "udp"]
                            .choose(&mut rng)
                            .unwrap()
                            .to_string(),
                    );
                }
                TagStrategy::Generic => {
                    tags.insert(
                        "service".to_string(),
                        pools.services.choose(&mut rng).unwrap().clone(),
                    );
                    tags.insert(
                        "environment".to_string(),
                        pools.environments.choose(&mut rng).unwrap().clone(),
                    );
                    if rng.random_bool(0.7) {
                        tags.insert(
                            "region".to_string(),
                            pools.regions.choose(&mut rng).unwrap().clone(),
                        );
                    }
                }
            }

            combinations.push(tags);
        }

        combinations
    }

    pub async fn generate_batch(&self, batch_size: usize) -> Vec<Vec<Value>> {
        let mut batch = Vec::with_capacity(batch_size);
        let now = chrono::Utc::now().timestamp_millis() as u64;

        for _ in 0..batch_size {
            let (template, tag_combo) = {
                let mut rng = rng(); // Create fresh RNG for each iteration
                let template =
                    &self.metric_templates[rng.random_range(0..self.metric_templates.len())];
                let tag_combo = &template.tag_combinations
                    [rng.random_range(0..template.tag_combinations.len())];
                (template, tag_combo)
            }; // RNG goes out of scope here

            let datapoint = self.generate_datapoint(template, tag_combo, now).await;
            batch.push(datapoint);
        }

        // Group by metric name for efficiency
        let mut grouped: HashMap<String, Vec<Value>> = HashMap::new();
        for dp in batch {
            for point in dp {
                if let Some(name) = point.get("name").and_then(|n| n.as_str()) {
                    grouped.entry(name.to_string()).or_default().push(point);
                }
            }
        }

        grouped.into_values().collect()
    }

    async fn generate_datapoint(
        &self,
        template: &MetricTemplate,
        tags: &HashMap<String, String>,
        timestamp: u64,
    ) -> Vec<Value> {
        match &template.metric_type {
            MetricType::Histogram { boundaries } => {
                let histogram = self.generate_realistic_histogram(boundaries).await;
                vec![json!({
                    "name": template.name,
                    "datapoints": [[timestamp, histogram]],
                    "tags": tags
                })]
            }
            MetricType::Counter => {
                let value = rng().random_range(1..=100) as u64;
                vec![json!({
                    "name": template.name,
                    "datapoints": [[timestamp, value]],
                    "tags": tags
                })]
            }
            MetricType::Gauge => {
                let sampled_value: f64 = Normal::new(50.0, 15.0).unwrap().sample(&mut rng());
                let value: f64 = sampled_value.max(0.0);
                vec![json!({
                    "name": template.name,
                    "datapoints": [[timestamp, value]],
                    "tags": tags
                })]
            }
            MetricType::Timer => {
                let value: f64 = LogNormal::new(0.0, 1.0).unwrap().sample(&mut rng());
                vec![json!({
                    "name": template.name,
                    "datapoints": [[timestamp, value]],
                    "tags": tags
                })]
            }
        }
    }

    async fn generate_realistic_histogram(&self, boundaries: &[f64]) -> Value {
        let mut rng = rng();
        let sample_count = rng.random_range(
            self.config.histogram_samples_range.0..=self.config.histogram_samples_range.1,
        );

        // Generate realistic distributions based on boundary ranges
        let distribution_type = rng.random_range(0..3);
        let samples = match distribution_type {
            0 => self.generate_normal_samples(boundaries, sample_count),
            1 => self.generate_exponential_samples(boundaries, sample_count),
            _ => self.generate_bimodal_samples(boundaries, sample_count),
        };

        // Count samples in each bucket
        let mut counts = vec![0u64; boundaries.len()];
        let mut total_count = 0u64;
        let mut sum = 0.0f64;
        let mut min_val = f64::INFINITY;
        let mut max_val = f64::NEG_INFINITY;

        for sample in &samples {
            total_count += 1;
            sum += sample;
            min_val = min_val.min(*sample);
            max_val = max_val.max(*sample);

            for (i, &boundary) in boundaries.iter().enumerate() {
                if *sample <= boundary {
                    counts[i] += 1;
                    break;
                }
            }
        }

        // Convert to cumulative counts
        for i in 1..counts.len() {
            counts[i] += counts[i - 1];
        }

        json!({
            "boundaries": boundaries,
            "counts": counts,
            "total_count": total_count,
            "sum": sum,
            "min": min_val,
            "max": max_val
        })
    }

    fn generate_normal_samples(&self, boundaries: &[f64], count: usize) -> Vec<f64> {
        let mean = boundaries[boundaries.len() / 2];
        let std_dev = mean * 0.3;
        let normal = Normal::new(mean, std_dev).unwrap();

        (0..count)
            .map(|_| normal.sample(&mut rng()).max(0.0))
            .collect()
    }

    fn generate_exponential_samples(&self, boundaries: &[f64], count: usize) -> Vec<f64> {
        let scale = boundaries[boundaries.len() / 3];
        let log_normal = LogNormal::new(scale.ln(), 1.0).unwrap();

        (0..count).map(|_| log_normal.sample(&mut rng())).collect()
    }

    fn generate_bimodal_samples(&self, boundaries: &[f64], count: usize) -> Vec<f64> {
        let mean1 = boundaries[boundaries.len() / 4];
        let mean2 = boundaries[3 * boundaries.len() / 4];
        let std_dev = mean1 * 0.2;

        let normal1 = Normal::new(mean1, std_dev).unwrap();
        let normal2 = Normal::new(mean2, std_dev).unwrap();

        (0..count)
            .map(|_| {
                let dist = if rng().random_bool(0.7) {
                    &normal1
                } else {
                    &normal2
                };
                dist.sample(&mut rng()).max(0.0)
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
enum TagStrategy {
    Http,
    Database,
    System,
    Cache,
    Queue,
    Network,
    Generic,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_generator_creates_histogram_data() {
        let config = GeneratorConfig {
            metrics_count: 10,
            tag_combinations_per_metric: 5,
            histogram_samples_range: (10, 100),
            tag_cardinality_limit: 10,
        };

        let generator = MetricDataGenerator::new(config);
        let batch = generator.generate_batch(50).await;

        assert!(!batch.is_empty());
        assert!(batch.len() <= 50);
    }

    #[test]
    fn test_tag_pools_generation() {
        let pools = TagPools::new();
        assert!(!pools.services.is_empty());
        assert!(!pools.environments.is_empty());
        assert!(!pools.regions.is_empty());
    }
}
