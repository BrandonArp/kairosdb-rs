//! Health check tests

#[allow(unused_imports)]
use crate::common::{E2ETestConfig, INGEST_BASE_URL, JAVA_KAIROSDB_BASE_URL, RUST_QUERY_BASE_URL};

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
async fn test_all_services_health() {
    let config = E2ETestConfig::new();

    // Check ingest service health
    let ingest_health = config
        .client
        .get(format!("{}/health", INGEST_BASE_URL))
        .send()
        .await
        .expect("Failed to call ingest health endpoint");

    assert!(
        ingest_health.status().is_success(),
        "Ingest service health check failed: {}",
        ingest_health.status()
    );

    // Check Java KairosDB health
    let java_health = config
        .client
        .get(format!("{}/api/v1/health/check", JAVA_KAIROSDB_BASE_URL))
        .send()
        .await
        .expect("Failed to call Java KairosDB health endpoint");

    assert!(
        java_health.status().is_success(),
        "Java KairosDB health check failed: {}",
        java_health.status()
    );

    // Check Rust query service health
    let rust_query_health = config
        .client
        .get(format!("{}/health", RUST_QUERY_BASE_URL))
        .send()
        .await
        .expect("Failed to call Rust query health endpoint");

    assert!(
        rust_query_health.status().is_success(),
        "Rust query service health check failed: {}",
        rust_query_health.status()
    );

    println!("✅ All services are healthy and ready for testing");
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
