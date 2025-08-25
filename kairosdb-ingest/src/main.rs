use anyhow::Result;
use std::sync::Arc;
use tokio::{net::TcpListener, signal};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

// Use the library modules
use kairosdb_ingest::{create_router, AppState, IngestConfig, IngestionService};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing with development-friendly logging levels
    // Default to INFO for development, WARN for performance testing
    // Can be overridden with RUST_LOG environment variable
    let log_level = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "info".to_string());
    
    tracing_subscriber::fmt()
        .with_env_filter(log_level)
        .with_target(false)  // Remove module paths from logs for cleaner output
        .init();

    info!("Starting KairosDB Ingestion Service");

    // Load configuration
    let config = Arc::new(IngestConfig::load()?);
    info!("Configuration loaded successfully");

    // Create cancellation token for coordinated shutdown
    let shutdown_token = CancellationToken::new();

    // Initialize ingestion service
    let ingestion_service = IngestionService::new(config.clone(), shutdown_token.clone()).await?;
    info!("Ingestion service initialized");

    // Initialize HTTP metrics
    let http_metrics = Arc::new(kairosdb_ingest::http_metrics::HttpMetrics::new()?);
    info!("HTTP metrics initialized");

    // Start background tasks that need coordinated shutdown
    let queue_processor_handle = ingestion_service.start_queue_processor(shutdown_token.clone()).await;

    // Create shared state
    let state = AppState {
        ingestion_service: Arc::new(ingestion_service),
        config: config.clone(),
        http_metrics,
    };

    // Build router using the library function
    let app = create_router(state);

    // Start server with graceful shutdown
    let listener = TcpListener::bind(&config.bind_address).await?;
    let addr = listener.local_addr()?;
    info!("KairosDB Ingest Service listening on {}", addr);
    
    // Clone token for shutdown signal handler
    let shutdown_token_clone = shutdown_token.clone();
    
    // Enhanced shutdown signal handler
    let shutdown_handler = async move {
        shutdown_signal().await;
        info!("Shutdown signal received, initiating graceful shutdown");
        shutdown_token_clone.cancel();
    };

    // Graceful shutdown handling
    let graceful = axum::serve(listener, app).with_graceful_shutdown(shutdown_handler);

    info!("Service ready to accept connections");

    tokio::select! {
        result = graceful => {
            if let Err(e) = result {
                error!("HTTP server error: {}", e);
                shutdown_token.cancel();
                return Err(e.into());
            }
        }
        _ = shutdown_token.cancelled() => {
            info!("Shutdown token cancelled, stopping HTTP server");
        }
    }

    info!("HTTP server stopped, waiting for background tasks to finish");
    
    // Wait for background tasks to complete
    if let Err(e) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        queue_processor_handle
    ).await {
        warn!("Queue processor didn't shutdown cleanly within 10s: {:?}", e);
    } else {
        info!("Queue processor shutdown complete");
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
