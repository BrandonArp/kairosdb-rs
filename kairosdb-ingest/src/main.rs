use anyhow::Result;
use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use std::{
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpListener,
    signal,
    time::timeout,
};
use tower::ServiceBuilder;
use tower_http::{
    compression::CompressionLayer,
    cors::CorsLayer, 
    limit::RequestBodyLimitLayer,
    trace::TraceLayer,
};
use tracing::{error, info, warn};

mod cassandra_client;
mod config;
mod handlers;
mod ingestion;
mod json_parser;
mod metrics;

use config::IngestConfig;
use handlers::{
    cors_preflight_datapoints, health_handler, ingest_gzip_handler, ingest_handler,
    metrics_handler, metrics_json_handler, request_size_middleware, timing_middleware,
};
use ingestion::IngestionService;

/// Shared application state
#[derive(Clone)]
pub struct AppState {
    pub ingestion_service: Arc<IngestionService>,
    pub config: Arc<IngestConfig>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();
    
    info!("Starting KairosDB Ingestion Service");
    
    // Load configuration
    let config = Arc::new(IngestConfig::load()?);
    info!("Configuration loaded successfully");
    
    // Initialize ingestion service  
    let ingestion_service = IngestionService::new(config.clone()).await?;
    info!("Ingestion service initialized");
    
    // Create shared state
    let state = AppState {
        ingestion_service: Arc::new(ingestion_service),
        config: config.clone(),
    };
    
    // Build comprehensive router with middleware
    let app = Router::new()
        // Health and monitoring endpoints
        .route(&config.health.health_path, get(health_handler))
        .route(&config.health.readiness_path, get(health_handler))
        .route(&config.health.liveness_path, get(health_handler))
        .route(&config.metrics.metrics_path, get(metrics_handler))
        .route("/api/v1/metrics", get(metrics_json_handler))
        
        // Data ingestion endpoints (KairosDB compatible)
        .route("/api/v1/datapoints", 
            post(ingest_handler)
                .options(cors_preflight_datapoints))
        .route("/api/v1/datapoints/gzip", 
            post(ingest_gzip_handler)
                .options(cors_preflight_datapoints))
        
        // Version endpoint for compatibility
        .route("/api/v1/version", get(version_handler))
        
        // Apply middleware layers in order (bottom to top)
        .layer(
            ServiceBuilder::new()
                .layer(middleware::from_fn(timing_middleware))
                .layer(middleware::from_fn(request_size_middleware))
                .layer(RequestBodyLimitLayer::new(config.ingestion.max_request_size))
                .layer(if config.is_compression_enabled() {
                    CompressionLayer::new()
                } else {
                    CompressionLayer::new().no_br().no_deflate().no_gzip()
                })
                .layer(TraceLayer::new_for_http())
                .layer(CorsLayer::permissive()),
        )
        .with_state(state);
    
    // Start server with graceful shutdown
    let listener = TcpListener::bind(&config.bind_address).await?;
    let addr = listener.local_addr()?;
    info!("KairosDB Ingest Service listening on {}", addr);
    
    // Graceful shutdown handling
    let graceful = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal());
    
    info!("Service ready to accept connections");
    
    if let Err(e) = graceful.await {
        error!("Server error: {}", e);
        return Err(e.into());
    }
    
    info!("Service shutdown complete");
    Ok(())
}

/// Handle graceful shutdown signals
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C signal, starting graceful shutdown");
        },
        _ = terminate => {
            info!("Received SIGTERM signal, starting graceful shutdown");
        },
    }
}

/// Version endpoint for KairosDB compatibility
async fn version_handler() -> impl axum::response::IntoResponse {
    use serde_json::json;
    
    axum::Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "service": "kairosdb-ingest-rs",
        "build_time": env!("VERGEN_BUILD_TIMESTAMP", "unknown"),
        "git_hash": env!("VERGEN_GIT_SHA", "unknown"),
    }))
}