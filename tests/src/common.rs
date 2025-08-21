//! Common utilities for E2E tests

use chrono::Utc;
use reqwest::Client;
use std::time::Duration;

pub const INGEST_BASE_URL: &str = "http://localhost:8081";
pub const JAVA_KAIROSDB_BASE_URL: &str = "http://localhost:8080";
pub const RUST_QUERY_BASE_URL: &str = "http://localhost:8082";

/// Test configuration and utilities
pub struct E2ETestConfig {
    pub client: Client,
    pub test_metric_prefix: String,
}

impl E2ETestConfig {
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to create HTTP client");

        // Use timestamp to ensure unique metric names per test run
        let timestamp = Utc::now().timestamp();
        let test_metric_prefix = format!("e2e_test_{}", timestamp);

        Self {
            client,
            test_metric_prefix,
        }
    }

    pub fn test_metric_name(&self, suffix: &str) -> String {
        format!("{}.{}", self.test_metric_prefix, suffix)
    }
}

impl Default for E2ETestConfig {
    fn default() -> Self {
        Self::new()
    }
}
