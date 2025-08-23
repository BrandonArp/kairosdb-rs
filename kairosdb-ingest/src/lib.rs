//! KairosDB Ingestion Service Library
//!
//! This library provides the core components for the KairosDB ingestion service,
//! including JSON parsing, data ingestion, and HTTP handlers.

// Core modules
pub mod bloom_manager;
pub mod cassandra;
pub mod cassandra_client;
pub mod config;
pub mod handlers;
pub mod ingestion;
pub mod json_parser;
pub mod metrics;
pub mod mock_client;

// Re-export commonly used types
pub use config::IngestConfig;
pub use ingestion::IngestionService;
pub use json_parser::JsonParser;

/// Application state shared across handlers
#[derive(Clone)]
pub struct AppState {
    pub ingestion_service: std::sync::Arc<IngestionService>,
    pub config: std::sync::Arc<IngestConfig>,
}

/// Create the main application router
pub fn create_router(state: AppState) -> axum::Router {
    use crate::handlers::*;
    use axum::routing::{get, post};
    use tower::ServiceBuilder;
    use tower_http::{
        compression::CompressionLayer, cors::CorsLayer, limit::RequestBodyLimitLayer,
        trace::TraceLayer,
    };

    axum::Router::new()
        // Health and monitoring endpoints
        .route(&state.config.health.health_path, get(health_handler))
        .route(&state.config.health.readiness_path, get(health_handler))
        .route(&state.config.health.liveness_path, get(health_handler))
        .route(&state.config.metrics.metrics_path, get(metrics_handler))
        .route("/api/v1/metrics", get(metrics_json_handler))
        // Data ingestion endpoints (KairosDB compatible)
        .route(
            "/api/v1/datapoints",
            post(ingest_handler).options(cors_preflight_datapoints),
        )
        .route(
            "/api/v1/datapoints/gzip",
            post(ingest_gzip_handler).options(cors_preflight_datapoints),
        )
        // Version endpoint for compatibility
        .route("/api/v1/version", get(version_handler))
        // Apply middleware layers in order (bottom to top)
        .layer(
            ServiceBuilder::new()
                .layer(axum::middleware::from_fn(timing_middleware))
                .layer(axum::middleware::from_fn(request_size_middleware))
                .layer(RequestBodyLimitLayer::new(
                    state.config.ingestion.max_request_size,
                ))
                .layer(if state.config.is_compression_enabled() {
                    CompressionLayer::new()
                } else {
                    CompressionLayer::new().no_br().no_deflate().no_gzip()
                })
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(tower_http::trace::DefaultMakeSpan::new().level(tracing::Level::TRACE))
                        .on_request(tower_http::trace::DefaultOnRequest::new().level(tracing::Level::TRACE))
                        .on_response(tower_http::trace::DefaultOnResponse::new().level(tracing::Level::TRACE))
                )
                .layer(CorsLayer::permissive()),
        )
        .with_state(state)
}

/// Version endpoint for KairosDB compatibility
async fn version_handler() -> impl axum::response::IntoResponse {
    use serde_json::json;

    axum::Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "service": "kairosdb-ingest-rs",
        "build_time": "unknown",
        "git_hash": "unknown",
    }))
}
