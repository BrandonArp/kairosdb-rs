use anyhow::Result;
use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use kairosdb_core::{
    error::KairosError,
    query::{MetricNamesQuery, QueryRequest, QueryResponse, TagNamesQuery, TagValuesQuery},
};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{error, info, warn};

mod aggregation;
mod config;
mod handlers;
mod metrics;
mod query_engine;

use config::QueryConfig;
use handlers::{
    health_handler, metric_names_handler, metrics_handler, query_handler, tag_names_handler,
    tag_values_handler,
};
use query_engine::QueryEngine;

/// Shared application state
#[derive(Clone)]
pub struct AppState {
    pub query_engine: Arc<QueryEngine>,
    pub config: Arc<QueryConfig>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Load configuration
    let config = Arc::new(QueryConfig::load()?);
    info!("Loaded configuration: {:?}", config);

    // Initialize query engine
    let query_engine = Arc::new(QueryEngine::new(config.clone()).await?);
    info!("Initialized query engine");

    // Create shared state
    let state = AppState {
        query_engine,
        config: config.clone(),
    };

    // Build router
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/api/v1/datapoints/query", post(query_handler))
        .route("/api/v1/metricnames", get(metric_names_handler))
        .route("/api/v1/tagnames", get(tag_names_handler))
        .route("/api/v1/tagvalues", get(tag_values_handler))
        // KairosDB compatibility routes
        .route("/api/v1/datapoints/query/tags", get(tag_names_handler))
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(CorsLayer::permissive()),
        )
        .with_state(state);

    // Start server
    let listener = TcpListener::bind(&config.bind_address).await?;
    let addr = listener.local_addr()?;
    info!("KairosDB Query Service listening on {}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}
