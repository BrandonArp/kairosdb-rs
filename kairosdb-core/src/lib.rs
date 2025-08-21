//! # KairosDB Core Library
//!
//! Shared library providing core data types, Cassandra integration, and utilities
//! for KairosDB ingest and query services.
//!
//! ## Features
//!
//! - **Data Types**: Time series data points, metrics, and tags
//! - **Cassandra**: Connection management, schema operations, and data access
//! - **Serialization**: Efficient binary and JSON serialization
//! - **Validation**: Input validation and data integrity checks
//! - **Utilities**: Time handling, hashing, and performance monitoring
//!
//! ## Architecture
//!
//! This library is designed to be shared between the ingest and query services,
//! providing a common foundation for:
//! - Data format compatibility
//! - Consistent Cassandra access patterns  
//! - Shared utilities and error types
//! - Performance-optimized data structures

pub mod cassandra;
pub mod datapoint;
pub mod datastore;
pub mod error;
pub mod histogram_key_utility;
pub mod metrics;
pub mod query;
pub mod schema;
pub mod tags;
pub mod time;
pub mod validation;

// Re-export commonly used types
pub use datapoint::{DataPoint, DataPointValue, HistogramBuilder, HistogramData};
pub use datastore::{
    AdvancedTimeSeriesStore, BatchedTimeSeriesStore, MaintainableTimeSeriesStore, TimeSeriesStore,
};
pub use datastore::{
    TagFilter, TagSet as DataStoreTagSet, TagValue as DataStoreTagValue, WriteResult,
};
pub use error::{KairosError, KairosResult};
pub use metrics::MetricName;
pub use tags::{TagKey, TagSet, TagValue};
pub use time::{TimeRange, Timestamp};

/// Version information for KairosDB-rs
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Maximum number of data points allowed in a single batch
pub const MAX_BATCH_SIZE: usize = 10_000;

/// Maximum number of tags allowed per data point
pub const MAX_TAGS_PER_POINT: usize = 100;

/// Maximum length for metric names
pub const MAX_METRIC_NAME_LENGTH: usize = 256;

/// Maximum length for tag keys and values
pub const MAX_TAG_LENGTH: usize = 256;
