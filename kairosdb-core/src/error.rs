//! Error types for KairosDB operations

use thiserror::Error;

/// Result type for KairosDB operations
pub type KairosResult<T> = Result<T, KairosError>;

/// Comprehensive error types for KairosDB operations
#[derive(Error, Debug)]
pub enum KairosError {
    #[error("Cassandra error: {0}")]
    Cassandra(String),

    #[error("Cassandra error: {0}")]
    CassandraError(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Time range error: {0}")]
    TimeRange(String),

    #[error("Metric not found: {0}")]
    MetricNotFound(String),

    #[error("Invalid data point: {0}")]
    InvalidDataPoint(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Timeout error: operation timed out after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },

    #[error("Rate limit exceeded: {message}")]
    RateLimit { message: String },

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("UUID error: {0}")]
    Uuid(#[from] uuid::Error),

    #[error("Parse error: {0}")]
    Parse(String),
}

impl KairosError {
    /// Create a new validation error
    pub fn validation<S: Into<String>>(message: S) -> Self {
        Self::Validation(message.into())
    }

    /// Create a new query error
    pub fn query<S: Into<String>>(message: S) -> Self {
        Self::Query(message.into())
    }

    /// Create a new cassandra error
    pub fn cassandra<S: Into<String>>(message: S) -> Self {
        Self::Cassandra(message.into())
    }

    /// Create a new parse error
    pub fn parse<S: Into<String>>(message: S) -> Self {
        Self::Parse(message.into())
    }

    /// Create a new schema error
    pub fn schema<S: Into<String>>(message: S) -> Self {
        Self::Schema(message.into())
    }

    /// Create a new timeout error
    pub fn timeout<S: Into<String>>(_message: S) -> Self {
        Self::Timeout { timeout_ms: 5000 } // Default timeout
    }

    /// Create a new rate limit error
    pub fn rate_limit<S: Into<String>>(message: S) -> Self {
        Self::RateLimit {
            message: message.into(),
        }
    }

    /// Create a new internal error  
    pub fn internal<S: Into<String>>(message: S) -> Self {
        Self::Internal(message.into())
    }

    /// Check if this is a retriable error
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            KairosError::Connection(_)
                | KairosError::Timeout { .. }
                | KairosError::Cassandra(_)
                | KairosError::CassandraError(_)
                | KairosError::Io(_)
        )
    }

    /// Get the error category for monitoring/metrics
    pub fn category(&self) -> &'static str {
        match self {
            KairosError::Cassandra(_) => "cassandra",
            KairosError::CassandraError(_) => "cassandra",
            KairosError::Serialization(_) => "serialization",
            KairosError::Validation(_) => "validation",
            KairosError::Query(_) => "query",
            KairosError::Configuration(_) => "configuration",
            KairosError::TimeRange(_) => "time_range",
            KairosError::MetricNotFound(_) => "metric_not_found",
            KairosError::InvalidDataPoint(_) => "invalid_data_point",
            KairosError::Schema(_) => "schema",
            KairosError::Connection(_) => "connection",
            KairosError::Timeout { .. } => "timeout",
            KairosError::RateLimit { .. } => "rate_limit",
            KairosError::Internal(_) => "internal",
            KairosError::Io(_) => "io",
            KairosError::Json(_) => "json",
            KairosError::Uuid(_) => "uuid",
            KairosError::Parse(_) => "parse",
        }
    }
}
