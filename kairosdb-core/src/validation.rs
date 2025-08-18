//! Validation utilities for KairosDB data types

use crate::datapoint::{DataPoint, DataPointBatch, DataPointValue};
use crate::error::{KairosError, KairosResult};
use crate::metrics::MetricName;
use crate::query::{QueryRequest, MetricQuery, Aggregator};
use crate::tags::{TagKey, TagValue, TagSet};
use crate::time::{Timestamp, TimeRange, RelativeTime};

/// Validation limits and constants
pub struct ValidationLimits {
    /// Maximum metric name length
    pub max_metric_name_length: usize,
    /// Maximum tag key/value length
    pub max_tag_length: usize,
    /// Maximum number of tags per data point
    pub max_tags_per_point: usize,
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Maximum query range in milliseconds
    pub max_query_range_ms: i64,
    /// Maximum number of metrics per query
    pub max_metrics_per_query: usize,
    /// Maximum text value length
    pub max_text_value_length: usize,
    /// Maximum binary value length
    pub max_binary_value_length: usize,
    /// Maximum histogram buckets
    pub max_histogram_buckets: usize,
}

impl Default for ValidationLimits {
    fn default() -> Self {
        Self {
            max_metric_name_length: crate::MAX_METRIC_NAME_LENGTH,
            max_tag_length: crate::MAX_TAG_LENGTH,
            max_tags_per_point: crate::MAX_TAGS_PER_POINT,
            max_batch_size: crate::MAX_BATCH_SIZE,
            max_query_range_ms: 365 * 24 * 60 * 60 * 1000, // 1 year
            max_metrics_per_query: 100,
            max_text_value_length: 1024 * 1024, // 1MB
            max_binary_value_length: 10 * 1024 * 1024, // 10MB
            max_histogram_buckets: 1000, // Maximum buckets per histogram
        }
    }
}

/// Comprehensive validator for KairosDB data
pub struct Validator {
    limits: ValidationLimits,
}

impl Default for Validator {
    fn default() -> Self {
        Self {
            limits: ValidationLimits::default(),
        }
    }
}

impl Validator {
    /// Create a new validator with custom limits
    pub fn new(limits: ValidationLimits) -> Self {
        Self { limits }
    }
    
    /// Validate a metric name
    pub fn validate_metric_name(&self, metric: &MetricName) -> KairosResult<()> {
        if metric.is_empty() {
            return Err(KairosError::validation("Metric name cannot be empty"));
        }
        
        if metric.len() > self.limits.max_metric_name_length {
            return Err(KairosError::validation(format!(
                "Metric name too long: {} > {}",
                metric.len(),
                self.limits.max_metric_name_length
            )));
        }
        
        // Check for valid characters
        if !metric.as_str().chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '-') {
            return Err(KairosError::validation("Metric name contains invalid characters"));
        }
        
        // Check for consecutive dots
        if metric.as_str().contains("..") {
            return Err(KairosError::validation("Metric name cannot contain consecutive dots"));
        }
        
        // Check for leading/trailing dots
        if metric.as_str().starts_with('.') || metric.as_str().ends_with('.') {
            return Err(KairosError::validation("Metric name cannot start or end with dots"));
        }
        
        Ok(())
    }
    
    /// Validate a tag key
    pub fn validate_tag_key(&self, key: &TagKey) -> KairosResult<()> {
        if key.is_empty() {
            return Err(KairosError::validation("Tag key cannot be empty"));
        }
        
        if key.len() > self.limits.max_tag_length {
            return Err(KairosError::validation(format!(
                "Tag key too long: {} > {}",
                key.len(),
                self.limits.max_tag_length
            )));
        }
        
        // Check for valid characters (more restrictive than metric names)
        if !key.as_str().chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') {
            return Err(KairosError::validation("Tag key contains invalid characters"));
        }
        
        // Reserved tag keys
        if key.as_str().starts_with("kairosdb_") {
            return Err(KairosError::validation("Tag key cannot start with 'kairosdb_' (reserved)"));
        }
        
        Ok(())
    }
    
    /// Validate a tag value
    pub fn validate_tag_value(&self, value: &TagValue) -> KairosResult<()> {
        if value.is_empty() {
            return Err(KairosError::validation("Tag value cannot be empty"));
        }
        
        if value.len() > self.limits.max_tag_length {
            return Err(KairosError::validation(format!(
                "Tag value too long: {} > {}",
                value.len(),
                self.limits.max_tag_length
            )));
        }
        
        // Tag values are more permissive than keys
        // Disallow only control characters
        if value.as_str().chars().any(|c| c.is_control() && c != '\t') {
            return Err(KairosError::validation("Tag value contains invalid control characters"));
        }
        
        Ok(())
    }
    
    /// Validate a tag set
    pub fn validate_tag_set(&self, tags: &TagSet) -> KairosResult<()> {
        if tags.len() > self.limits.max_tags_per_point {
            return Err(KairosError::validation(format!(
                "Too many tags: {} > {}",
                tags.len(),
                self.limits.max_tags_per_point
            )));
        }
        
        for (key, value) in tags.iter() {
            self.validate_tag_key(key)?;
            self.validate_tag_value(value)?;
        }
        
        Ok(())
    }
    
    /// Validate a timestamp
    pub fn validate_timestamp(&self, timestamp: &Timestamp) -> KairosResult<()> {
        let millis = timestamp.timestamp_millis();
        
        // Check for reasonable timestamp bounds (year 1970 to 2100)
        let min_timestamp = 0i64; // 1970-01-01
        let max_timestamp = 4_102_444_800_000i64; // 2100-01-01
        
        if millis < min_timestamp || millis > max_timestamp {
            return Err(KairosError::validation(format!(
                "Timestamp out of bounds: {}",
                timestamp
            )));
        }
        
        Ok(())
    }
    
    /// Validate a data point value
    pub fn validate_data_point_value(&self, value: &DataPointValue) -> KairosResult<()> {
        match value {
            DataPointValue::Long(v) => {
                // Check for special values that might cause issues
                if *v == i64::MIN {
                    return Err(KairosError::validation("Long value cannot be i64::MIN"));
                }
            }
            DataPointValue::Double(v) => {
                let val = v.into_inner();
                if val.is_infinite() {
                    return Err(KairosError::validation("Double value cannot be infinite"));
                }
                if val.is_nan() {
                    return Err(KairosError::validation("Double value cannot be NaN"));
                }
            }
            DataPointValue::Complex { real, imaginary } => {
                if real.is_infinite() || real.is_nan() {
                    return Err(KairosError::validation("Complex real part cannot be infinite or NaN"));
                }
                if imaginary.is_infinite() || imaginary.is_nan() {
                    return Err(KairosError::validation("Complex imaginary part cannot be infinite or NaN"));
                }
            }
            DataPointValue::Text(text) => {
                if text.len() > self.limits.max_text_value_length {
                    return Err(KairosError::validation(format!(
                        "Text value too long: {} > {}",
                        text.len(),
                        self.limits.max_text_value_length
                    )));
                }
                
                // Check for control characters
                if text.chars().any(|c| c.is_control() && c != '\t' && c != '\n' && c != '\r') {
                    return Err(KairosError::validation("Text value contains invalid control characters"));
                }
            }
            DataPointValue::Binary(data) => {
                if data.len() > self.limits.max_binary_value_length {
                    return Err(KairosError::validation(format!(
                        "Binary value too long: {} > {}",
                        data.len(),
                        self.limits.max_binary_value_length
                    )));
                }
            }
            DataPointValue::Histogram(hist) => {
                // Validate histogram structure
                if hist.boundaries.is_empty() {
                    return Err(KairosError::validation("Histogram must have at least one boundary"));
                }
                
                if hist.boundaries.len() != hist.counts.len() {
                    return Err(KairosError::validation("Histogram boundaries and counts must have same length"));
                }
                
                // Check boundaries are sorted
                for i in 1..hist.boundaries.len() {
                    if hist.boundaries[i] <= hist.boundaries[i-1] {
                        return Err(KairosError::validation("Histogram boundaries must be sorted"));
                    }
                }
                
                // Check counts sum matches computed total
                let counts_sum: u64 = hist.counts.iter().sum();
                if counts_sum != hist.total_count() {
                    return Err(KairosError::validation("Histogram counts sum must equal total_count"));
                }
                
                // Validate bucket count limit
                if hist.boundaries.len() > self.limits.max_histogram_buckets {
                    return Err(KairosError::validation(format!(
                        "Too many histogram buckets: {} > {}",
                        hist.boundaries.len(),
                        self.limits.max_histogram_buckets
                    )));
                }
                
                // Validate numeric values
                for boundary in &hist.boundaries {
                    if boundary.is_infinite() || boundary.is_nan() {
                        return Err(KairosError::validation("Histogram boundary cannot be infinite or NaN"));
                    }
                }
                
                if hist.sum.is_infinite() || hist.sum.is_nan() {
                    return Err(KairosError::validation("Histogram sum cannot be infinite or NaN"));
                }
                
                if hist.min.is_infinite() || hist.min.is_nan() {
                    return Err(KairosError::validation("Histogram min cannot be infinite or NaN"));
                }
                
                if hist.max.is_infinite() || hist.max.is_nan() {
                    return Err(KairosError::validation("Histogram max cannot be infinite or NaN"));
                }
            }
        }
        
        Ok(())
    }
    
    /// Validate a data point
    pub fn validate_data_point(&self, point: &DataPoint) -> KairosResult<()> {
        self.validate_metric_name(&point.metric)?;
        self.validate_timestamp(&point.timestamp)?;
        self.validate_data_point_value(&point.value)?;
        self.validate_tag_set(&point.tags)?;
        
        // Validate TTL
        if point.ttl > 0 {
            let max_ttl = 365 * 24 * 60 * 60; // 1 year in seconds
            if point.ttl > max_ttl {
                return Err(KairosError::validation(format!(
                    "TTL too large: {} > {}",
                    point.ttl,
                    max_ttl
                )));
            }
        }
        
        Ok(())
    }
    
    /// Validate a data point batch
    pub fn validate_data_point_batch(&self, batch: &DataPointBatch) -> KairosResult<()> {
        if batch.is_empty() {
            return Err(KairosError::validation("Batch cannot be empty"));
        }
        
        if batch.len() > self.limits.max_batch_size {
            return Err(KairosError::validation(format!(
                "Batch too large: {} > {}",
                batch.len(),
                self.limits.max_batch_size
            )));
        }
        
        for point in &batch.points {
            self.validate_data_point(point)?;
        }
        
        // Check for duplicate data points (same metric, timestamp, tags)
        // This is expensive but important for data quality
        for i in 0..batch.points.len() {
            for j in i + 1..batch.points.len() {
                let p1 = &batch.points[i];
                let p2 = &batch.points[j];
                
                if p1.metric == p2.metric 
                    && p1.timestamp == p2.timestamp 
                    && p1.tags == p2.tags {
                    return Err(KairosError::validation("Batch contains duplicate data points"));
                }
            }
        }
        
        Ok(())
    }
    
    /// Validate a time range
    pub fn validate_time_range(&self, range: &TimeRange) -> KairosResult<()> {
        self.validate_timestamp(&range.start)?;
        self.validate_timestamp(&range.end)?;
        
        if range.start >= range.end {
            return Err(KairosError::validation("Start time must be before end time"));
        }
        
        let duration = range.duration_millis();
        if duration > self.limits.max_query_range_ms {
            return Err(KairosError::validation(format!(
                "Query range too large: {} > {} ms",
                duration,
                self.limits.max_query_range_ms
            )));
        }
        
        Ok(())
    }
    
    /// Validate a query request
    pub fn validate_query_request(&self, query: &QueryRequest) -> KairosResult<()> {
        // Validate time range
        let range = query.time_range()?;
        self.validate_time_range(&range)?;
        
        // Validate metrics
        if query.metrics.is_empty() {
            return Err(KairosError::validation("Query must specify at least one metric"));
        }
        
        if query.metrics.len() > self.limits.max_metrics_per_query {
            return Err(KairosError::validation(format!(
                "Too many metrics in query: {} > {}",
                query.metrics.len(),
                self.limits.max_metrics_per_query
            )));
        }
        
        for metric_query in &query.metrics {
            self.validate_metric_query(metric_query)?;
        }
        
        // Validate limits
        if let Some(limit) = query.limit {
            if limit == 0 {
                return Err(KairosError::validation("Query limit must be greater than 0"));
            }
            if limit > 1_000_000 {
                return Err(KairosError::validation("Query limit too large"));
            }
        }
        
        Ok(())
    }
    
    /// Validate a metric query
    pub fn validate_metric_query(&self, query: &MetricQuery) -> KairosResult<()> {
        self.validate_metric_name(&query.name)?;
        
        // Validate tag filters
        if query.tags.len() > self.limits.max_tags_per_point {
            return Err(KairosError::validation("Too many tag filters"));
        }
        
        for (key, value) in &query.tags {
            if key.is_empty() || value.is_empty() {
                return Err(KairosError::validation("Tag filter key and value cannot be empty"));
            }
        }
        
        // Validate aggregators
        for aggregator in &query.aggregators {
            self.validate_aggregator(aggregator)?;
        }
        
        Ok(())
    }
    
    /// Validate an aggregator
    pub fn validate_aggregator(&self, aggregator: &Aggregator) -> KairosResult<()> {
        match aggregator {
            Aggregator::Percentile { percentile, .. } => {
                if *percentile < 0.0 || *percentile > 100.0 {
                    return Err(KairosError::validation("Percentile must be between 0 and 100"));
                }
            }
            _ => {}
        }
        
        Ok(())
    }
}

/// Quick validation functions for common use cases
impl Validator {
    /// Quick validation for a metric name string
    pub fn is_valid_metric_name(name: &str) -> bool {
        if let Ok(metric) = MetricName::new(name) {
            Self::default().validate_metric_name(&metric).is_ok()
        } else {
            false
        }
    }
    
    /// Quick validation for a tag key string
    pub fn is_valid_tag_key(key: &str) -> bool {
        if let Ok(tag_key) = TagKey::new(key) {
            Self::default().validate_tag_key(&tag_key).is_ok()
        } else {
            false
        }
    }
    
    /// Quick validation for a tag value string
    pub fn is_valid_tag_value(value: &str) -> bool {
        if let Ok(tag_value) = TagValue::new(value) {
            Self::default().validate_tag_value(&tag_value).is_ok()
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datapoint::DataPoint;
    use crate::time::Timestamp;
    
    #[test]
    fn test_metric_name_validation() {
        let validator = Validator::default();
        
        // Valid metric names
        assert!(validator.validate_metric_name(&MetricName::new("simple").unwrap()).is_ok());
        assert!(validator.validate_metric_name(&MetricName::new("with.dots").unwrap()).is_ok());
        assert!(validator.validate_metric_name(&MetricName::new("with_underscores").unwrap()).is_ok());
        assert!(validator.validate_metric_name(&MetricName::new("with-dashes").unwrap()).is_ok());
        
        // Invalid metric names would fail at MetricName::new() stage
    }
    
    #[test]
    fn test_data_point_validation() {
        let validator = Validator::default();
        
        let point = DataPoint::new_long("test.metric", Timestamp::now(), 42);
        assert!(validator.validate_data_point(&point).is_ok());
        
        // Test with invalid TTL
        let point_with_ttl = point.with_ttl(365 * 24 * 60 * 60 + 1); // > 1 year
        assert!(validator.validate_data_point(&point_with_ttl).is_err());
    }
    
    #[test]
    fn test_batch_validation() {
        let validator = Validator::default();
        
        let mut batch = DataPointBatch::new();
        batch.add_point(DataPoint::new_long("metric1", Timestamp::now(), 1)).unwrap();
        batch.add_point(DataPoint::new_long("metric2", Timestamp::now(), 2)).unwrap();
        
        assert!(validator.validate_data_point_batch(&batch).is_ok());
        
        // Test empty batch
        let empty_batch = DataPointBatch::new();
        assert!(validator.validate_data_point_batch(&empty_batch).is_err());
    }
    
    #[test]
    fn test_quick_validation_functions() {
        assert!(Validator::is_valid_metric_name("test.metric"));
        assert!(!Validator::is_valid_metric_name(""));
        
        assert!(Validator::is_valid_tag_key("host"));
        assert!(!Validator::is_valid_tag_key(""));
        
        assert!(Validator::is_valid_tag_value("server1"));
        assert!(!Validator::is_valid_tag_value(""));
    }
}