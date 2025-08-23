use anyhow::Result;
use std::sync::Arc;
use tokio::{net::TcpListener, signal};
use tracing::{error, info};

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

    // Initialize ingestion service
    let ingestion_service = IngestionService::new(config.clone()).await?;
    info!("Ingestion service initialized");

    // Create shared state
    let state = AppState {
        ingestion_service: Arc::new(ingestion_service),
        config: config.clone(),
    };

    // Build router using the library function
    let app = create_router(state);

    // Start server with graceful shutdown
    let listener = TcpListener::bind(&config.bind_address).await?;
    let addr = listener.local_addr()?;
    info!("KairosDB Ingest Service listening on {}", addr);

    // Graceful shutdown handling
    let graceful = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal());

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
