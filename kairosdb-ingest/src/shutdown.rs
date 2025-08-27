//! Graceful shutdown manager for ordered component shutdown
//!
//! This module provides a structured approach to shutting down the ingestion service
//! with the following ordered sequence:
//! 1. Start failing health checks  
//! 2. Wait a few seconds (grace period)
//! 3. Start closing connections after requests complete
//! 4. Shut down the HTTP server
//! 5. Stop dequeueing data points from the persistent queue
//! 6. Wait for the worker channel to be empty
//! 7. Shutdown the workers
//! 8. Wait for the response channel to be empty
//! 9. Shut down the response worker
//! 10. Terminate application

use parking_lot::RwLock;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::Notify, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Tracks active HTTP connections for graceful shutdown
#[derive(Debug, Clone)]
pub struct ConnectionTracker {
    /// Number of active connections
    active_connections: Arc<AtomicU64>,
}

impl ConnectionTracker {
    pub fn new() -> Self {
        Self {
            active_connections: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Increment the active connection count
    pub fn increment(&self) {
        let count = self.active_connections.fetch_add(1, Ordering::Relaxed) + 1;
        debug!("Connection opened, active connections: {}", count);
    }

    /// Decrement the active connection count
    pub fn decrement(&self) {
        let count = self.active_connections.fetch_sub(1, Ordering::Relaxed) - 1;
        debug!("Connection closed, active connections: {}", count);
    }

    /// Get the current number of active connections
    pub fn active_count(&self) -> u64 {
        self.active_connections.load(Ordering::Relaxed)
    }

    /// Check if all connections are closed
    pub fn all_connections_closed(&self) -> bool {
        self.active_count() == 0
    }
}

impl Default for ConnectionTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Phases of the shutdown sequence
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ShutdownPhase {
    /// Normal operation
    Running,
    /// Health checks start failing, new requests discouraged
    HealthCheckFailing,
    /// Graceful connection draining in progress
    DrainingConnections,
    /// HTTP server shutting down
    HttpServerShutdown,
    /// Queue dequeuing stopped
    QueueDequeueingStopped,
    /// Waiting for worker channels to empty
    WaitingForWorkerChannels,
    /// Worker threads shutting down
    WorkersShuttingDown,
    /// Waiting for response channels to empty
    WaitingForResponseChannels,
    /// Response workers shutting down
    ResponseWorkersShuttingDown,
    /// Final termination
    Terminated,
}

/// Manages the graceful shutdown sequence
#[derive(Clone)]
pub struct ShutdownManager {
    /// Current shutdown phase
    current_phase: Arc<RwLock<ShutdownPhase>>,

    /// Tracks active HTTP connections
    connection_tracker: ConnectionTracker,

    /// Individual cancellation tokens for each component
    health_check_token: CancellationToken,
    connection_draining_token: CancellationToken,
    http_server_token: CancellationToken,
    queue_dequeuing_token: CancellationToken,
    worker_channels_token: CancellationToken,
    workers_token: CancellationToken,
    response_channels_token: CancellationToken,
    response_workers_token: CancellationToken,
    termination_token: CancellationToken,

    /// Notification mechanism for phase transitions
    phase_notify: Arc<Notify>,

    /// Configuration for timeouts
    config: ShutdownConfig,
}

/// Configuration for shutdown timeouts and behavior
#[derive(Debug, Clone)]
pub struct ShutdownConfig {
    /// Time to wait after health checks start failing before draining connections
    pub health_check_grace_period: Duration,
    /// Time to allow for connection draining
    pub connection_draining_timeout: Duration,
    /// Time to wait for HTTP server to shut down
    pub http_server_timeout: Duration,
    /// Time to wait for worker channels to empty
    pub worker_channel_timeout: Duration,
    /// Time to wait for workers to shut down
    pub worker_shutdown_timeout: Duration,
    /// Time to wait for response channels to empty
    pub response_channel_timeout: Duration,
    /// Time to wait for response workers to shut down
    pub response_worker_timeout: Duration,
    /// Total maximum shutdown time before forced termination
    pub total_shutdown_timeout: Duration,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            health_check_grace_period: Duration::from_secs(3),
            connection_draining_timeout: Duration::from_secs(30),
            http_server_timeout: Duration::from_secs(10),
            worker_channel_timeout: Duration::from_secs(15),
            worker_shutdown_timeout: Duration::from_secs(10),
            response_channel_timeout: Duration::from_secs(10),
            response_worker_timeout: Duration::from_secs(5),
            total_shutdown_timeout: Duration::from_secs(120), // 2 minutes total
        }
    }
}

impl ShutdownManager {
    /// Create a new shutdown manager with default configuration
    pub fn new() -> Self {
        Self::new_with_config(ShutdownConfig::default())
    }

    /// Create a new shutdown manager with custom configuration
    pub fn new_with_config(config: ShutdownConfig) -> Self {
        Self {
            current_phase: Arc::new(RwLock::new(ShutdownPhase::Running)),
            connection_tracker: ConnectionTracker::new(),
            health_check_token: CancellationToken::new(),
            connection_draining_token: CancellationToken::new(),
            http_server_token: CancellationToken::new(),
            queue_dequeuing_token: CancellationToken::new(),
            worker_channels_token: CancellationToken::new(),
            workers_token: CancellationToken::new(),
            response_channels_token: CancellationToken::new(),
            response_workers_token: CancellationToken::new(),
            termination_token: CancellationToken::new(),
            phase_notify: Arc::new(Notify::new()),
            config,
        }
    }

    /// Get the current shutdown phase
    pub fn current_phase(&self) -> ShutdownPhase {
        *self.current_phase.read()
    }

    /// Check if the service should fail health checks
    pub fn should_fail_health_checks(&self) -> bool {
        self.current_phase() >= ShutdownPhase::HealthCheckFailing
    }

    /// Check if connections should be drained
    pub fn should_drain_connections(&self) -> bool {
        self.current_phase() >= ShutdownPhase::DrainingConnections
    }

    /// Get the connection tracker
    pub fn connection_tracker(&self) -> &ConnectionTracker {
        &self.connection_tracker
    }

    /// Get the current number of active connections
    pub fn active_connection_count(&self) -> u64 {
        self.connection_tracker.active_count()
    }

    /// Get cancellation token for health checks
    pub fn health_check_token(&self) -> &CancellationToken {
        &self.health_check_token
    }

    /// Get cancellation token for connection draining
    pub fn connection_draining_token(&self) -> &CancellationToken {
        &self.connection_draining_token
    }

    /// Get cancellation token for HTTP server
    pub fn http_server_token(&self) -> &CancellationToken {
        &self.http_server_token
    }

    /// Get cancellation token for queue dequeuing
    pub fn queue_dequeuing_token(&self) -> &CancellationToken {
        &self.queue_dequeuing_token
    }

    /// Get cancellation token for worker channels
    pub fn worker_channels_token(&self) -> &CancellationToken {
        &self.worker_channels_token
    }

    /// Get cancellation token for workers
    pub fn workers_token(&self) -> &CancellationToken {
        &self.workers_token
    }

    /// Get cancellation token for response channels
    pub fn response_channels_token(&self) -> &CancellationToken {
        &self.response_channels_token
    }

    /// Get cancellation token for response workers
    pub fn response_workers_token(&self) -> &CancellationToken {
        &self.response_workers_token
    }

    /// Get cancellation token for final termination
    pub fn termination_token(&self) -> &CancellationToken {
        &self.termination_token
    }

    /// Start the graceful shutdown sequence
    pub async fn start_shutdown(&self) {
        info!("Starting graceful shutdown sequence");

        // Start the shutdown process with a timeout for the entire sequence
        let shutdown_future = self.execute_shutdown_sequence();
        let timeout_future = sleep(self.config.total_shutdown_timeout);

        tokio::select! {
            _ = shutdown_future => {
                info!("Graceful shutdown completed successfully");
            }
            _ = timeout_future => {
                error!("Graceful shutdown timed out after {:?}, forcing termination",
                       self.config.total_shutdown_timeout);
                self.force_termination().await;
            }
        }
    }

    /// Execute the complete shutdown sequence
    async fn execute_shutdown_sequence(&self) {
        // Step 1: Start failing health checks
        self.transition_to_phase(ShutdownPhase::HealthCheckFailing)
            .await;
        info!("Step 1/10: Health checks now failing");

        // Step 2: Wait grace period
        info!(
            "Step 2/10: Waiting {:?} grace period for load balancers to detect unhealthy status",
            self.config.health_check_grace_period
        );
        sleep(self.config.health_check_grace_period).await;

        // Step 3: Start draining connections
        self.transition_to_phase(ShutdownPhase::DrainingConnections)
            .await;
        let active_connections = self.active_connection_count();
        info!("Step 3/10: Starting connection draining, {} active connections, allowing in-flight requests to complete", active_connections);

        // Wait for connections to drain or timeout
        self.wait_for_connections_to_drain().await;

        // Step 4: Shut down HTTP server
        self.transition_to_phase(ShutdownPhase::HttpServerShutdown)
            .await;
        info!("Step 4/8: Shutting down HTTP server");

        // No sleep needed - Axum handles graceful shutdown when the token is cancelled

        // Step 5: Stop dequeueing from persistent queue
        self.transition_to_phase(ShutdownPhase::QueueDequeueingStopped)
            .await;
        info!("Step 5/10: Stopped dequeueing data points from persistent queue");

        // Step 6: Workers and response processors will naturally shutdown via channel closes
        // Queue processor stops publishing work -> work channel closes -> workers exit -> response channel closes
        info!("Step 6/8: Workers will shutdown naturally when work channel closes");

        // Give some time for natural shutdown to complete
        sleep(self.config.worker_shutdown_timeout).await;

        // Step 7: Shutdown response workers (cleanup any remaining background tasks)
        self.transition_to_phase(ShutdownPhase::ResponseWorkersShuttingDown)
            .await;
        info!("Step 7/8: Shutting down response worker threads");

        sleep(self.config.response_worker_timeout).await;

        // Step 8: Final termination
        self.transition_to_phase(ShutdownPhase::Terminated).await;
        info!("Step 8/8: All components shut down, terminating application");
    }

    /// Wait for connections to drain with intelligent timeout
    async fn wait_for_connections_to_drain(&self) {
        let start_time = std::time::Instant::now();
        let timeout = self.config.connection_draining_timeout;
        let poll_interval = Duration::from_millis(100);

        loop {
            let active_connections = self.active_connection_count();

            if active_connections == 0 {
                info!("All connections drained successfully");
                break;
            }

            let elapsed = start_time.elapsed();
            if elapsed >= timeout {
                warn!("Connection draining timeout after {:?}, {} connections still active, proceeding with shutdown", 
                      timeout, active_connections);
                break;
            }

            debug!(
                "Waiting for {} connections to drain, elapsed: {:?}",
                active_connections, elapsed
            );
            sleep(poll_interval).await;
        }
    }

    /// Transition to a new shutdown phase and cancel appropriate tokens
    async fn transition_to_phase(&self, new_phase: ShutdownPhase) {
        let old_phase = {
            let mut phase = self.current_phase.write();
            let old = *phase;
            *phase = new_phase;
            old
        };

        info!(
            "Shutdown phase transition: {:?} -> {:?}",
            old_phase, new_phase
        );

        // Cancel tokens based on the new phase
        match new_phase {
            ShutdownPhase::Running => {
                // No tokens to cancel
            }
            ShutdownPhase::HealthCheckFailing => {
                self.health_check_token.cancel();
            }
            ShutdownPhase::DrainingConnections => {
                self.connection_draining_token.cancel();
            }
            ShutdownPhase::HttpServerShutdown => {
                self.http_server_token.cancel();
            }
            ShutdownPhase::QueueDequeueingStopped => {
                self.queue_dequeuing_token.cancel();
            }
            ShutdownPhase::WaitingForWorkerChannels => {
                self.worker_channels_token.cancel();
            }
            ShutdownPhase::WorkersShuttingDown => {
                self.workers_token.cancel();
            }
            ShutdownPhase::WaitingForResponseChannels => {
                self.response_channels_token.cancel();
            }
            ShutdownPhase::ResponseWorkersShuttingDown => {
                self.response_workers_token.cancel();
            }
            ShutdownPhase::Terminated => {
                self.termination_token.cancel();
            }
        }

        // Notify any waiters about the phase change
        self.phase_notify.notify_waiters();
    }

    /// Force immediate termination of all components
    pub async fn force_termination(&self) {
        warn!("Forcing immediate termination of all components");

        // Cancel all tokens immediately
        self.health_check_token.cancel();
        self.connection_draining_token.cancel();
        self.http_server_token.cancel();
        self.queue_dequeuing_token.cancel();
        self.worker_channels_token.cancel();
        self.workers_token.cancel();
        self.response_channels_token.cancel();
        self.response_workers_token.cancel();
        self.termination_token.cancel();

        // Set phase to terminated
        *self.current_phase.write() = ShutdownPhase::Terminated;
        self.phase_notify.notify_waiters();
    }

    /// Wait for a specific shutdown phase to be reached
    pub async fn wait_for_phase(&self, target_phase: ShutdownPhase) {
        loop {
            if self.current_phase() >= target_phase {
                return;
            }

            // Wait for notification of phase change
            self.phase_notify.notified().await;
        }
    }

    /// Get a summary of the current shutdown status for diagnostics
    pub fn get_status_summary(&self) -> ShutdownStatusSummary {
        let phase = self.current_phase();
        ShutdownStatusSummary {
            current_phase: phase,
            health_checks_failing: self.should_fail_health_checks(),
            connections_draining: self.should_drain_connections(),
            active_connections: self.active_connection_count(),
            tokens_cancelled: ShutdownTokenStatus {
                health_check: self.health_check_token.is_cancelled(),
                connection_draining: self.connection_draining_token.is_cancelled(),
                http_server: self.http_server_token.is_cancelled(),
                queue_dequeuing: self.queue_dequeuing_token.is_cancelled(),
                worker_channels: self.worker_channels_token.is_cancelled(),
                workers: self.workers_token.is_cancelled(),
                response_channels: self.response_channels_token.is_cancelled(),
                response_workers: self.response_workers_token.is_cancelled(),
                termination: self.termination_token.is_cancelled(),
            },
        }
    }
}

/// Summary of shutdown status for diagnostics
#[derive(Debug, Clone)]
pub struct ShutdownStatusSummary {
    pub current_phase: ShutdownPhase,
    pub health_checks_failing: bool,
    pub connections_draining: bool,
    pub active_connections: u64,
    pub tokens_cancelled: ShutdownTokenStatus,
}

/// Status of individual shutdown tokens
#[derive(Debug, Clone)]
pub struct ShutdownTokenStatus {
    pub health_check: bool,
    pub connection_draining: bool,
    pub http_server: bool,
    pub queue_dequeuing: bool,
    pub worker_channels: bool,
    pub workers: bool,
    pub response_channels: bool,
    pub response_workers: bool,
    pub termination: bool,
}

impl Default for ShutdownManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_connection_tracker() {
        let tracker = ConnectionTracker::new();

        assert_eq!(tracker.active_count(), 0);
        assert!(tracker.all_connections_closed());

        tracker.increment();
        assert_eq!(tracker.active_count(), 1);
        assert!(!tracker.all_connections_closed());

        tracker.increment();
        assert_eq!(tracker.active_count(), 2);

        tracker.decrement();
        assert_eq!(tracker.active_count(), 1);

        tracker.decrement();
        assert_eq!(tracker.active_count(), 0);
        assert!(tracker.all_connections_closed());
    }

    #[tokio::test]
    async fn test_shutdown_manager_creation() {
        let manager = ShutdownManager::new();
        assert_eq!(manager.current_phase(), ShutdownPhase::Running);
        assert!(!manager.should_fail_health_checks());
        assert!(!manager.should_drain_connections());
        assert_eq!(manager.active_connection_count(), 0);
    }

    #[tokio::test]
    async fn test_phase_transitions() {
        let manager = ShutdownManager::new();

        // Start shutdown in background
        let manager_clone = manager.clone();
        tokio::spawn(async move {
            manager_clone.start_shutdown().await;
        });

        // Wait for health check phase
        timeout(
            Duration::from_secs(1),
            manager.wait_for_phase(ShutdownPhase::HealthCheckFailing),
        )
        .await
        .expect("Should reach health check failing phase");

        assert!(manager.should_fail_health_checks());

        // Wait for connection draining phase
        timeout(
            Duration::from_secs(5),
            manager.wait_for_phase(ShutdownPhase::DrainingConnections),
        )
        .await
        .expect("Should reach connection draining phase");

        assert!(manager.should_drain_connections());
    }

    #[tokio::test]
    async fn test_status_summary() {
        let manager = ShutdownManager::new();
        let status = manager.get_status_summary();

        assert_eq!(status.current_phase, ShutdownPhase::Running);
        assert!(!status.health_checks_failing);
        assert!(!status.connections_draining);
        assert!(!status.tokens_cancelled.health_check);
    }

    #[tokio::test]
    async fn test_force_termination() {
        let manager = ShutdownManager::new();

        manager.force_termination().await;

        assert_eq!(manager.current_phase(), ShutdownPhase::Terminated);
        assert!(manager.health_check_token().is_cancelled());
        assert!(manager.termination_token().is_cancelled());
    }
}
