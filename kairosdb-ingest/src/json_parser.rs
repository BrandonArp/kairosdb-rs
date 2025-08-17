//! JSON parsing for KairosDB-compatible data point ingestion
//!
//! This module provides JSON parsing that is fully compatible with the Java KairosDB API.
//! It handles the specific JSON format used by KairosDB for data point ingestion.

use anyhow::{Context, Result};
use kairosdb_core::{
    datapoint::{DataPoint, DataPointBatch, DataPointValue},
    error::{KairosError, KairosResult},
    metrics::MetricName,
    tags::{TagKey, TagSet, TagValue},
    time::Timestamp,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tracing::{debug, warn};
use validator::Validate;

/// JSON request format for KairosDB data point ingestion
/// Compatible with Java KairosDB NewMetricRequest format
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct MetricRequest {
    /// Metric name (required)
    #[validate(length(min = 1, max = 256, message = "Metric name must be 1-256 characters"))]
    pub name: String,
    
    /// Data points for this metric (required)
    #[validate(length(min = 1, message = "At least one data point is required"))]
    pub datapoints: Vec<DataPointRequest>,
    
    /// Tags for this metric (optional)
    #[serde(default)]
    pub tags: HashMap<String, String>,
    
    /// TTL in seconds (optional, 0 means no TTL)
    #[serde(default)]
    pub ttl: u32,
}

/// Individual data point in the request
/// Compatible with Java KairosDB DataPointRequest format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPointRequest {
    /// Timestamp (Unix milliseconds or seconds)
    pub timestamp: i64,
    
    /// Value (can be number or string)
    pub value: Value,
    
    /// Optional tags specific to this data point
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

/// Batch request containing multiple metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IngestRequest {
    /// Single metric object
    Single(MetricRequest),
    /// Array of metrics
    Multiple(Vec<MetricRequest>),
}

/// Response format for successful ingestion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestResponse {
    /// Number of data points successfully ingested
    pub datapoints_ingested: usize,
    
    /// Processing time in milliseconds
    pub ingest_time: u64,
    
    /// Optional warnings (non-fatal issues)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Error response format compatible with KairosDB
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    /// List of error messages
    pub errors: Vec<String>,
}

/// Validation errors collected during parsing
#[derive(Debug, Default)]
pub struct ValidationErrors {
    pub errors: Vec<String>,
}

impl ValidationErrors {
    pub fn new() -> Self {
        Self { errors: Vec::new() }
    }
    
    pub fn add_error<S: Into<String>>(&mut self, error: S) {
        self.errors.push(error.into());
    }
    
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
    
    pub fn into_result(self) -> KairosResult<()> {
        if self.has_errors() {
            Err(KairosError::validation(self.errors.join("; ")))
        } else {
            Ok(())
        }
    }
}

/// JSON parser for KairosDB data points
pub struct JsonParser {
    /// Maximum batch size allowed
    max_batch_size: usize,
    /// Enable strict validation
    strict_validation: bool,
}

impl JsonParser {
    /// Create a new JSON parser
    pub fn new(max_batch_size: usize, strict_validation: bool) -> Self {
        Self {
            max_batch_size,
            strict_validation,
        }
    }
    
    /// Parse JSON into a batch of data points
    pub fn parse_json(&self, json_str: &str) -> KairosResult<(DataPointBatch, Vec<String>)> {
        debug!("Parsing JSON input, length: {}", json_str.len());
        
        // Parse the JSON string
        let request: IngestRequest = serde_json::from_str(json_str)
            .context("Failed to parse JSON")
            .map_err(|e| KairosError::parse(e.to_string()))?;
        
        // Convert to metric requests
        let metrics = match request {
            IngestRequest::Single(metric) => vec![metric],
            IngestRequest::Multiple(metrics) => metrics,
        };
        
        // Validate and convert to data points
        let mut validation_errors = ValidationErrors::new();
        let mut warnings = Vec::new();
        let mut all_points = Vec::new();
        
        for (metric_idx, metric) in metrics.iter().enumerate() {
            match self.process_metric(metric, metric_idx, &mut validation_errors) {
                Ok(mut points) => {
                    all_points.append(&mut points);
                }
                Err(e) => {
                    if self.strict_validation {
                        return Err(e);
                    } else {
                        warnings.push(format!("Metric {}: {}", metric_idx, e));
                    }
                }
            }
        }
        
        // Check if we have any validation errors
        validation_errors.into_result()?;
        
        // Check batch size limits
        if all_points.len() > self.max_batch_size {
            return Err(KairosError::validation(format!(
                "Batch size {} exceeds maximum of {}",
                all_points.len(),
                self.max_batch_size
            )));
        }
        
        if all_points.is_empty() {
            return Err(KairosError::validation("No valid data points found"));
        }
        
        let batch = DataPointBatch::from_points(all_points)?;
        debug!("Successfully parsed {} data points", batch.len());
        
        Ok((batch, warnings))
    }
    
    /// Process a single metric request
    fn process_metric(
        &self,
        metric: &MetricRequest,
        metric_idx: usize,
        validation_errors: &mut ValidationErrors,
    ) -> KairosResult<Vec<DataPoint>> {
        // Validate the metric request
        if let Err(validation_err) = metric.validate() {
            validation_errors.add_error(format!("Metric {}: {}", metric_idx, validation_err));
            if self.strict_validation {
                return Err(KairosError::validation(validation_err.to_string()));
            }
        }
        
        // Create metric name
        let metric_name = MetricName::new(&metric.name)
            .map_err(|e| KairosError::validation(format!("Invalid metric name '{}': {}", metric.name, e)))?;
        
        // Parse tags
        let base_tags = self.parse_tags(&metric.tags)
            .map_err(|e| KairosError::validation(format!("Invalid tags for metric '{}': {}", metric.name, e)))?;
        
        let mut points = Vec::new();
        
        // Process each data point
        for (dp_idx, datapoint) in metric.datapoints.iter().enumerate() {
            match self.process_datapoint(&metric_name, datapoint, &base_tags, metric.ttl) {
                Ok(point) => points.push(point),
                Err(e) => {
                    let error_msg = format!("Metric '{}', datapoint {}: {}", metric.name, dp_idx, e);
                    validation_errors.add_error(&error_msg);
                    if self.strict_validation {
                        return Err(e);
                    }
                }
            }
        }
        
        Ok(points)
    }
    
    /// Process a single data point
    fn process_datapoint(
        &self,
        metric_name: &MetricName,
        datapoint: &DataPointRequest,
        base_tags: &TagSet,
        ttl: u32,
    ) -> KairosResult<DataPoint> {
        // Parse timestamp
        let timestamp = self.parse_timestamp(datapoint.timestamp)?;
        
        // Parse value
        let value = self.parse_value(&datapoint.value)?;
        
        // Combine base tags with datapoint-specific tags
        let mut tags = base_tags.clone();
        for (key, val) in &datapoint.tags {
            let tag_key = TagKey::new(key)
                .map_err(|e| KairosError::validation(format!("Invalid tag key '{}': {}", key, e)))?;
            let tag_value = TagValue::new(val)
                .map_err(|e| KairosError::validation(format!("Invalid tag value '{}': {}", val, e)))?;
            tags.insert(tag_key, tag_value)?;
        }
        
        // Create data point
        let mut point = DataPoint {
            metric: metric_name.clone(),
            timestamp,
            value,
            tags,
            ttl,
        };
        
        // Validate the complete data point
        point.validate_self()?;
        
        Ok(point)
    }
    
    /// Parse timestamp from JSON value
    fn parse_timestamp(&self, timestamp: i64) -> KairosResult<Timestamp> {
        // Handle both seconds and milliseconds timestamps
        let millis = if timestamp > 9999999999 {
            // Assume milliseconds if > year 2001 in seconds
            timestamp
        } else {
            // Assume seconds, convert to milliseconds
            timestamp * 1000
        };
        
        if millis <= 0 {
            return Err(KairosError::validation("Timestamp must be positive"));
        }
        
        Timestamp::from_millis(millis)
            .map_err(|e| KairosError::validation(format!("Invalid timestamp: {}", e)))
    }
    
    /// Parse value from JSON
    fn parse_value(&self, value: &Value) -> KairosResult<DataPointValue> {
        match value {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(DataPointValue::Long(i))
                } else if let Some(f) = n.as_f64() {
                    if f.is_finite() {
                        Ok(DataPointValue::Double(f.into()))
                    } else {
                        Err(KairosError::validation("Value cannot be infinite or NaN"))
                    }
                } else {
                    Err(KairosError::validation("Invalid numeric value"))
                }
            }
            Value::String(s) => {
                // Try to parse as number first (for compatibility)
                if let Ok(i) = s.parse::<i64>() {
                    Ok(DataPointValue::Long(i))
                } else if let Ok(f) = s.parse::<f64>() {
                    if f.is_finite() {
                        Ok(DataPointValue::Double(f.into()))
                    } else {
                        Err(KairosError::validation("Value cannot be infinite or NaN"))
                    }
                } else {
                    // Store as text value
                    Ok(DataPointValue::Text(s.clone()))
                }
            }
            Value::Bool(b) => {
                // Convert boolean to integer (false=0, true=1)
                Ok(DataPointValue::Long(if *b { 1 } else { 0 }))
            }
            _ => Err(KairosError::validation("Value must be a number, string, or boolean")),
        }
    }
    
    /// Parse tags from HashMap
    fn parse_tags(&self, tags: &HashMap<String, String>) -> KairosResult<TagSet> {
        let mut tag_set = TagSet::new();
        
        for (key, value) in tags {
            let tag_key = TagKey::new(key)
                .map_err(|e| KairosError::validation(format!("Invalid tag key '{}': {}", key, e)))?;
            let tag_value = TagValue::new(value)
                .map_err(|e| KairosError::validation(format!("Invalid tag value '{}': {}", value, e)))?;
            tag_set.insert(tag_key, tag_value)?;
        }
        
        Ok(tag_set)
    }
}

impl Default for JsonParser {
    fn default() -> Self {
        Self::new(10_000, true)
    }
}

impl ErrorResponse {
    /// Create error response from a single error
    pub fn from_error<S: Into<String>>(error: S) -> Self {
        Self {
            errors: vec![error.into()],
        }
    }
    
    /// Create error response from multiple errors
    pub fn from_errors<I, S>(errors: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            errors: errors.into_iter().map(|e| e.into()).collect(),
        }
    }
    
    /// Create error response from KairosError
    pub fn from_kairos_error(error: &KairosError) -> Self {
        Self::from_error(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    
    #[test]
    fn test_parse_single_metric_long_value() {
        let parser = JsonParser::default();
        let json = json!({
            "name": "test.metric",
            "datapoints": [[1634567890000i64, 42]],
            "tags": {"host": "server1"}
        }).to_string();
        
        let (batch, warnings) = parser.parse_json(&json).unwrap();
        assert_eq!(batch.len(), 1);
        assert!(warnings.is_empty());
        
        let point = &batch.points[0];
        assert_eq!(point.metric.as_str(), "test.metric");
        assert!(matches!(point.value, DataPointValue::Long(42)));
        assert_eq!(point.tags.get("host").unwrap(), "server1");
    }
    
    #[test]
    fn test_parse_single_metric_double_value() {
        let parser = JsonParser::default();
        let json = json!({
            "name": "test.metric",
            "datapoints": [[1634567890000i64, 3.14]],
            "tags": {"host": "server1"}
        }).to_string();
        
        let (batch, warnings) = parser.parse_json(&json).unwrap();
        assert_eq!(batch.len(), 1);
        assert!(warnings.is_empty());
        
        let point = &batch.points[0];
        assert_eq!(point.metric.as_str(), "test.metric");
        assert!(matches!(point.value, DataPointValue::Double(_)));
    }
    
    #[test]
    fn test_parse_multiple_datapoints() {
        let parser = JsonParser::default();
        let json = json!({
            "name": "test.metric",
            "datapoints": [
                [1634567890000i64, 42],
                [1634567891000i64, 43],
                [1634567892000i64, 44]
            ],
            "tags": {"host": "server1"}
        }).to_string();
        
        let (batch, warnings) = parser.parse_json(&json).unwrap();
        assert_eq!(batch.len(), 3);
        assert!(warnings.is_empty());
    }
    
    #[test]
    fn test_parse_array_of_metrics() {
        let parser = JsonParser::default();
        let json = json!([
            {
                "name": "metric1",
                "datapoints": [[1634567890000i64, 42]],
                "tags": {"host": "server1"}
            },
            {
                "name": "metric2", 
                "datapoints": [[1634567890000i64, 3.14]],
                "tags": {"host": "server2"}
            }
        ]).to_string();
        
        let (batch, warnings) = parser.parse_json(&json).unwrap();
        assert_eq!(batch.len(), 2);
        assert!(warnings.is_empty());
    }
    
    #[test]
    fn test_parse_timestamp_seconds() {
        let parser = JsonParser::default();
        let json = json!({
            "name": "test.metric",
            "datapoints": [[1634567890, 42]], // seconds timestamp
            "tags": {}
        }).to_string();
        
        let (batch, _) = parser.parse_json(&json).unwrap();
        let point = &batch.points[0];
        assert_eq!(point.timestamp_millis(), 1634567890000);
    }
    
    #[test]
    fn test_parse_timestamp_milliseconds() {
        let parser = JsonParser::default();
        let json = json!({
            "name": "test.metric", 
            "datapoints": [[1634567890000i64, 42]], // milliseconds timestamp
            "tags": {}
        }).to_string();
        
        let (batch, _) = parser.parse_json(&json).unwrap();
        let point = &batch.points[0];
        assert_eq!(point.timestamp_millis(), 1634567890000);
    }
    
    #[test]
    fn test_parse_string_numeric_value() {
        let parser = JsonParser::default();
        let json = json!({
            "name": "test.metric",
            "datapoints": [[1634567890000i64, "42"]],
            "tags": {}
        }).to_string();
        
        let (batch, _) = parser.parse_json(&json).unwrap();
        let point = &batch.points[0];
        assert!(matches!(point.value, DataPointValue::Long(42)));
    }
    
    #[test]
    fn test_parse_text_value() {
        let parser = JsonParser::default();
        let json = json!({
            "name": "test.metric",
            "datapoints": [[1634567890000i64, "hello world"]],
            "tags": {}
        }).to_string();
        
        let (batch, _) = parser.parse_json(&json).unwrap();
        let point = &batch.points[0];
        assert!(matches!(point.value, DataPointValue::Text(_)));
    }
    
    #[test]
    fn test_validation_empty_metric_name() {
        let parser = JsonParser::default();
        let json = json!({
            "name": "",
            "datapoints": [[1634567890000i64, 42]],
            "tags": {}
        }).to_string();
        
        let result = parser.parse_json(&json);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_validation_no_datapoints() {
        let parser = JsonParser::default();
        let json = json!({
            "name": "test.metric",
            "datapoints": [],
            "tags": {}
        }).to_string();
        
        let result = parser.parse_json(&json);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_batch_size_limit() {
        let parser = JsonParser::new(2, true);
        let json = json!({
            "name": "test.metric",
            "datapoints": [
                [1634567890000i64, 1],
                [1634567891000i64, 2],
                [1634567892000i64, 3] // Exceeds limit of 2
            ],
            "tags": {}
        }).to_string();
        
        let result = parser.parse_json(&json);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_error_response_creation() {
        let error = KairosError::validation("Test error");
        let response = ErrorResponse::from_kairos_error(&error);
        assert_eq!(response.errors.len(), 1);
        assert!(response.errors[0].contains("Test error"));
    }
}