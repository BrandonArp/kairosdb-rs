//! KairosDB Query Service Library
//!
//! This library provides the core components for the KairosDB query service,
//! including query processing, data aggregation, and HTTP handlers.

// Core modules
pub mod aggregation;
pub mod config;
pub mod handlers;
pub mod metrics;
pub mod query_engine;

// Re-export commonly used types
pub use config::QueryConfig;

/// Application state shared across handlers
#[derive(Clone)]
pub struct AppState {
    pub datastore: std::sync::Arc<dyn kairosdb_core::datastore::TimeSeriesStore>,
    pub config: std::sync::Arc<QueryConfig>,
}