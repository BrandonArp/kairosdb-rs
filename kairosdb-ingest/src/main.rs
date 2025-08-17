use anyhow::Result;
use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use kairosdb_core::{
    datapoint::{DataPoint, DataPointBatch},
    error::KairosError,
};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{
    cors::CorsLayer,
    trace::TraceLayer,
};
use tracing::{info, warn, error};

mod config;
mod handlers;
mod ingestion;
mod metrics;

use config::IngestConfig;
use handlers::{ingest_handler, health_handler, metrics_handler};
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
    
    // Load configuration
    let config = Arc::new(IngestConfig::load()?);
    info!("Loaded configuration: {:?}", config);
    
    // Initialize ingestion service
    let ingestion_service = Arc::new(IngestionService::new(config.clone()).await?);
    info!("Initialized ingestion service");
    
    // Create shared state
    let state = AppState {
        ingestion_service,
        config: config.clone(),
    };
    
    // Build router
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/api/v1/datapoints", post(ingest_handler))
        .route("/api/v1/datapoints/query/tags", post(ingest_handler)) // KairosDB compatibility
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CorsLayer::permissive()),
        )
        .with_state(state);
    
    // Start server
    let listener = TcpListener::bind(&config.bind_address).await?;
    let addr = listener.local_addr()?;
    info!("KairosDB Ingest Service listening on {}", addr);
    
    axum::serve(listener, app).await?;
    
    Ok(())
}