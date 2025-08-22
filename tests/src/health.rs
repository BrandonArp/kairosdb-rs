//! Health check tests

#[allow(unused_imports)]
use crate::common::{E2ETestConfig, INGEST_BASE_URL, JAVA_KAIROSDB_BASE_URL, RUST_QUERY_BASE_URL};
use tracing::info;

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_ingest_service_health() {
    let config = E2ETestConfig::new();

    let response = config
        .client
        .get(format!("{}/health", INGEST_BASE_URL))
        .send()
        .await
        .expect("Failed to call ingest health endpoint");

    assert!(
        response.status().is_success(),
        "Ingest service health check failed: {}",
        response.status()
    );
}

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_java_kairosdb_health() {
    let config = E2ETestConfig::new();

    let response = config
        .client
        .get(format!("{}/api/v1/health/check", JAVA_KAIROSDB_BASE_URL))
        .send()
        .await
        .expect("Failed to call Java KairosDB health endpoint");

    assert!(
        response.status().is_success(),
        "Java KairosDB health check failed: {}",
        response.status()
    );
}

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_00_all_services_health() {
    // Renamed with 00_ prefix to ensure this runs first
    let config = E2ETestConfig::new();

    info!("🔍 Checking ingest service health at {}", INGEST_BASE_URL);
    let ingest_health = config
        .client
        .get(format!("{}/health", INGEST_BASE_URL))
        .send()
        .await
        .expect("Failed to connect to ingest service - is Tilt running?");

    if !ingest_health.status().is_success() {
        let status = ingest_health.status();
        let error_body = ingest_health.text().await.unwrap_or_default();
        panic!(
            "Ingest service health check failed: {} - {}",
            status, error_body
        );
    }

    info!(
        "🔍 Checking Java KairosDB health at {}",
        JAVA_KAIROSDB_BASE_URL
    );
    let java_health = config
        .client
        .get(format!("{}/api/v1/health/check", JAVA_KAIROSDB_BASE_URL))
        .send()
        .await
        .expect("Failed to connect to Java KairosDB - is Tilt running?");

    if !java_health.status().is_success() {
        let status = java_health.status();
        let error_body = java_health.text().await.unwrap_or_default();
        panic!(
            "Java KairosDB health check failed: {} - {}",
            status, error_body
        );
    }

    info!(
        "🔍 Checking Rust query service health at {}",
        RUST_QUERY_BASE_URL
    );
    let rust_query_health = config
        .client
        .get(format!("{}/health", RUST_QUERY_BASE_URL))
        .send()
        .await
        .expect("Failed to connect to Rust query service - is Tilt running?");

    if !rust_query_health.status().is_success() {
        let status = rust_query_health.status();
        let error_body = rust_query_health.text().await.unwrap_or_default();
        panic!(
            "Rust query service health check failed: {} - {}",
            status, error_body
        );
    }

    info!("✅ All services are healthy and ready for testing");
}

#[tokio::test]
#[ignore] // Run with --ignored flag, requires Tilt environment
async fn test_rust_query_service_health() {
    let config = E2ETestConfig::new();

    let response = config
        .client
        .get(format!("{}/health", RUST_QUERY_BASE_URL))
        .send()
        .await
        .expect("Failed to call Rust query health endpoint");

    assert!(
        response.status().is_success(),
        "Rust query service health check failed: {}",
        response.status()
    );
}
