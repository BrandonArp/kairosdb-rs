use anyhow::Result;
use std::sync::Arc;
use tokio::{net::TcpListener, signal};
use tracing::{error, info, warn};

// Use the library modules
use kairosdb_ingest::{create_router, AppState, IngestConfig, IngestionService, ShutdownConfig, ShutdownManager};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing with development-friendly logging levels
    // Default to INFO for development, WARN for performance testing
    // Can be overridden with RUST_LOG environment variable
    let log_level = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());

    tracing_subscriber::fmt()
        .with_env_filter(log_level)
        .with_target(false) // Remove module paths from logs for cleaner output
        .init();

    info!("Starting KairosDB Ingestion Service");

    // Load configuration
    let config = Arc::new(IngestConfig::load()?);
    info!("Configuration loaded successfully");

    // Create shutdown manager with configurable load balancer detection delay
    let detection_delay = config.load_balancer_detection_delay();
    info!("Load balancer detection delay configured: {}s", detection_delay.as_secs());
    let shutdown_config = ShutdownConfig {
        health_check_grace_period: detection_delay,
        ..ShutdownConfig::default()
    };
    let shutdown_manager = Arc::new(ShutdownManager::new_with_config(shutdown_config));

    // Initialize ingestion service
    let (ingestion_service, null_queue_work_channel) =
        IngestionService::new(config.clone(), shutdown_manager.clone()).await?;
    info!("Ingestion service initialized");

    // Initialize HTTP metrics
    let http_metrics = Arc::new(kairosdb_ingest::http_metrics::HttpMetrics::new()?);
    info!("HTTP metrics initialized");

    // Start background tasks that need coordinated shutdown
    let queue_processor_handle = ingestion_service
        .start_queue_processor(shutdown_manager.clone(), null_queue_work_channel)
        .await;

    // Start background metrics update task
    let metrics_update_handle =
        ingestion_service.start_metrics_update_task(shutdown_manager.clone());

    // Start background garbage collection task to manage Fjall memory usage
    let gc_handle = ingestion_service.start_garbage_collection_task(shutdown_manager.clone());

    // Create shared state
    let state = AppState {
        ingestion_service: Arc::new(ingestion_service),
        config: config.clone(),
        http_metrics,
        shutdown_manager: shutdown_manager.clone(),
    };

    // Build router using the library function
    let app = create_router(state);

    // Start server with graceful shutdown
    let listener = TcpListener::bind(&config.bind_address).await?;
    let addr = listener.local_addr()?;
    info!("KairosDB Ingest Service listening on {}", addr);

    // Clone shutdown manager for shutdown signal handler
    let shutdown_manager_clone = shutdown_manager.clone();

    // Enhanced shutdown signal handler that starts the graceful shutdown sequence
    let shutdown_handler = async move {
        shutdown_signal().await;
        info!("Shutdown signal received, initiating 10-step graceful shutdown sequence");
        shutdown_manager_clone.start_shutdown().await;
    };

    // Graceful HTTP server shutdown (will be triggered by step 4 of shutdown sequence)
    let shutdown_manager_for_http = shutdown_manager.clone();
    let graceful = axum::serve(listener, app).with_graceful_shutdown(async move {
        shutdown_manager_for_http
            .http_server_token()
            .cancelled()
            .await;
        info!("HTTP server shutdown token triggered");
    });

    info!("Service ready to accept connections");

    // Start the shutdown handler in the background
    let shutdown_task = tokio::spawn(shutdown_handler);

    tokio::select! {
        result = graceful => {
            if let Err(e) = result {
                error!("HTTP server error: {}", e);
                // Force immediate shutdown on HTTP server error
                shutdown_manager.force_termination().await;
                return Err(e.into());
            }
            info!("HTTP server stopped gracefully");
        }
        _ = shutdown_manager.termination_token().cancelled() => {
            info!("Termination requested, stopping HTTP server immediately");
        }
    }

    info!("Waiting for background tasks to complete graceful shutdown");

    // Wait for background tasks to complete with longer timeouts since we now have an ordered shutdown
    if let Err(e) =
        tokio::time::timeout(std::time::Duration::from_secs(30), queue_processor_handle).await
    {
        warn!(
            "Queue processor didn't shutdown cleanly within 30s: {:?}",
            e
        );
    } else {
        info!("Queue processor shutdown complete");
    }

    // Wait for metrics update task to complete
    // This task should shutdown during step 4 (HTTP server shutdown), so give it enough time
    if let Err(e) =
        tokio::time::timeout(std::time::Duration::from_secs(8), metrics_update_handle).await
    {
        warn!(
            "Metrics update task didn't shutdown cleanly within 8s: {:?}",
            e
        );
    } else {
        info!("Metrics update task shutdown complete");
    }

    // Wait for garbage collection task to complete
    if let Err(e) = tokio::time::timeout(std::time::Duration::from_secs(8), gc_handle).await {
        warn!(
            "Garbage collection task didn't shutdown cleanly within 8s: {:?}",
            e
        );
    } else {
        info!("Garbage collection task shutdown complete");
    }

    // Wait for the shutdown task to complete with a reasonable timeout
    // The shutdown task runs the entire shutdown sequence which can take up to 2 minutes
    if let Err(e) = tokio::time::timeout(std::time::Duration::from_secs(130), shutdown_task).await {
        warn!("Shutdown task didn't complete within 130s: {:?}", e);
    }

    info!("Graceful shutdown sequence completed successfully");
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
