//! JSON parsing for KairosDB-compatible data point ingestion
//!
//! This module provides JSON parsing that is fully compatible with the Java KairosDB API.
//! It handles the specific JSON format used by KairosDB for data point ingestion.

use anyhow::Context;
use kairosdb_core::{
    datapoint::{DataPoint, DataPointBatch, DataPointValue, HistogramData},
    error::{KairosError, KairosResult},
    metrics::MetricName,
    tags::{TagKey, TagSet, TagValue},
    time::Timestamp,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tracing::trace;
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
        trace!("Parsing JSON input, length: {}", json_str.len());

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
        trace!("Successfully parsed {} data points", batch.len());

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
        let metric_name = MetricName::new(&metric.name).map_err(|e| {
            KairosError::validation(format!("Invalid metric name '{}': {}", metric.name, e))
        })?;

        // Parse tags
        let base_tags = self.parse_tags(&metric.tags).map_err(|e| {
            KairosError::validation(format!("Invalid tags for metric '{}': {}", metric.name, e))
        })?;

        let mut points = Vec::new();

        // Process each data point
        for (dp_idx, datapoint) in metric.datapoints.iter().enumerate() {
            match self.process_datapoint(&metric_name, datapoint, &base_tags, metric.ttl) {
                Ok(point) => points.push(point),
                Err(e) => {
                    let error_msg =
                        format!("Metric '{}', datapoint {}: {}", metric.name, dp_idx, e);
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
            let tag_key = TagKey::new(key).map_err(|e| {
                KairosError::validation(format!("Invalid tag key '{}': {}", key, e))
            })?;
            let tag_value = TagValue::new(val).map_err(|e| {
                KairosError::validation(format!("Invalid tag value '{}': {}", val, e))
            })?;
            tags.insert(tag_key, tag_value)?;
        }

        // Create data point
        let point = DataPoint {
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
            Value::Object(obj) => {
                // Check if this is a histogram object
                if obj.contains_key("buckets")
                    || obj.contains_key("boundaries")
                    || obj.contains_key("bins")
                {
                    self.parse_histogram(obj)
                } else if obj.contains_key("real") && obj.contains_key("imaginary") {
                    // Complex number
                    let real = obj.get("real").and_then(|v| v.as_f64()).ok_or_else(|| {
                        KairosError::validation("Complex number 'real' must be a number")
                    })?;
                    let imaginary =
                        obj.get("imaginary")
                            .and_then(|v| v.as_f64())
                            .ok_or_else(|| {
                                KairosError::validation(
                                    "Complex number 'imaginary' must be a number",
                                )
                            })?;

                    if !real.is_finite() || !imaginary.is_finite() {
                        return Err(KairosError::validation(
                            "Complex number parts cannot be infinite or NaN",
                        ));
                    }

                    Ok(DataPointValue::Complex { real, imaginary })
                } else {
                    Err(KairosError::validation("Unsupported object value format"))
                }
            }
            _ => Err(KairosError::validation(
                "Value must be a number, string, boolean, or histogram object",
            )),
        }
    }

    /// Parse histogram from JSON object
    /// Supports multiple histogram formats:
    /// 1. KairosDB V1/V2: {"bins": {"0.1": 10, "1.0": 20}, "min": 0.05, "max": 0.95, "sum": 15.5, "mean": 0.52}
    /// 2. Prometheus-style: {"buckets": [{"le": 10.0, "count": 5}, ...], "sum": 100, "count": 10}
    /// 3. Direct format: {"boundaries": [0, 10, 50], "counts": [2, 3, 5], "sum": 150, "total_count": 10}
    fn parse_histogram(
        &self,
        obj: &serde_json::Map<String, Value>,
    ) -> KairosResult<DataPointValue> {
        // Check for KairosDB bins format first (most common)
        if obj.contains_key("bins") {
            return self.parse_bins_histogram(obj);
        }

        // Check for Prometheus-style buckets format
        if let Some(buckets_value) = obj.get("buckets") {
            return self.parse_prometheus_histogram(obj, buckets_value);
        }

        // Check for direct boundaries format
        if let Some(boundaries_value) = obj.get("boundaries") {
            return self.parse_direct_histogram(obj, boundaries_value);
        }

        Err(KairosError::validation(
            "Histogram must have 'bins', 'buckets', or 'boundaries' field",
        ))
    }

    /// Parse Prometheus-style histogram
    fn parse_prometheus_histogram(
        &self,
        obj: &serde_json::Map<String, Value>,
        buckets_value: &Value,
    ) -> KairosResult<DataPointValue> {
        let buckets = buckets_value
            .as_array()
            .ok_or_else(|| KairosError::validation("Histogram 'buckets' must be an array"))?;

        // Pre-allocate with known capacity and parse buckets with cumulative-to-individual conversion
        let len = buckets.len();
        let mut bins = Vec::with_capacity(len);
        let mut prev_cumulative = 0u64;

        for bucket in buckets.iter() {
            let bucket_obj = bucket
                .as_object()
                .ok_or_else(|| KairosError::validation("Bucket must be an object"))?;

            let le = bucket_obj
                .get("le")
                .and_then(|v| v.as_f64())
                .filter(|&f| f.is_finite())
                .ok_or_else(|| KairosError::validation("Bucket missing finite 'le' field"))?;

            let cumulative_count = bucket_obj
                .get("count")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| KairosError::validation("Bucket missing 'count' field"))?;

            // Validate non-decreasing cumulative counts and convert to individual count
            if cumulative_count < prev_cumulative {
                return Err(KairosError::validation(
                    "Histogram bucket counts must be non-decreasing",
                ));
            }
            let individual_count = cumulative_count - prev_cumulative;
            prev_cumulative = cumulative_count;

            bins.push((le, individual_count));
        }

        // Sort bins by boundary (Prometheus buckets might not be ordered)
        bins.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        // Parse remaining fields with consolidated validation
        let sum = obj
            .get("sum")
            .and_then(|v| v.as_f64())
            .filter(|&f| f.is_finite())
            .unwrap_or(0.0);

        let min = obj
            .get("min")
            .and_then(|v| v.as_f64())
            .filter(|&f| f.is_finite())
            .unwrap_or(0.0);

        let max = obj
            .get("max")
            .and_then(|v| v.as_f64())
            .filter(|&f| f.is_finite())
            .unwrap_or(0.0);

        let histogram = HistogramData::from_bins(bins, sum, min, max, None)?;
        Ok(DataPointValue::Histogram(histogram))
    }

    /// Parse KairosDB bins format: {"bins": {"0.1": 10, "1.0": 20}, "min": 0.05, "max": 0.95, "sum": 15.5, "mean": 0.52}
    fn parse_bins_histogram(
        &self,
        obj: &serde_json::Map<String, Value>,
    ) -> KairosResult<DataPointValue> {
        // Get bins object with early validation
        let bins_value = obj
            .get("bins")
            .ok_or_else(|| KairosError::validation("Histogram missing 'bins' field"))?;

        let bins_obj = bins_value
            .as_object()
            .ok_or_else(|| KairosError::validation("Histogram 'bins' must be an object"))?;

        // Pre-allocate with known capacity and parse bins with string-to-f64 conversion
        let mut bins = Vec::with_capacity(bins_obj.len());
        for (boundary_str, count_val) in bins_obj {
            // Parse boundary string to f64 with combined validation
            let boundary = boundary_str
                .parse::<f64>()
                .ok()
                .filter(|&f| f.is_finite())
                .ok_or_else(|| KairosError::validation("Invalid or non-finite bin boundary"))?;

            let count = count_val.as_u64().ok_or_else(|| {
                KairosError::validation("Bin count must be a non-negative integer")
            })?;

            bins.push((boundary, count));
        }

        // Sort bins by boundary (KairosDB uses TreeMap which is sorted)
        bins.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        // Parse remaining fields with consolidated validation
        let sum = obj
            .get("sum")
            .and_then(|v| v.as_f64())
            .filter(|&f| f.is_finite())
            .ok_or_else(|| KairosError::validation("Histogram missing or invalid 'sum' field"))?;

        let min = obj
            .get("min")
            .and_then(|v| v.as_f64())
            .filter(|&f| f.is_finite())
            .ok_or_else(|| KairosError::validation("Histogram missing or invalid 'min' field"))?;

        let max = obj
            .get("max")
            .and_then(|v| v.as_f64())
            .filter(|&f| f.is_finite())
            .ok_or_else(|| KairosError::validation("Histogram missing or invalid 'max' field"))?;

        // Precision is optional (V1 has no precision field, V2 does)
        let precision = obj
            .get("precision")
            .and_then(|v| v.as_u64())
            .map(|p| p as u8)
            .unwrap_or(7); // Default V1 precision

        let histogram = HistogramData::from_bins(bins, sum, min, max, Some(precision))?;
        Ok(DataPointValue::Histogram(histogram))
    }

    /// Parse direct histogram format (boundaries/counts arrays)
    fn parse_direct_histogram(
        &self,
        obj: &serde_json::Map<String, Value>,
        boundaries_value: &Value,
    ) -> KairosResult<DataPointValue> {
        // Get arrays upfront with early validation
        let boundaries_array = boundaries_value
            .as_array()
            .ok_or_else(|| KairosError::validation("Histogram 'boundaries' must be an array"))?;

        let counts_value = obj
            .get("counts")
            .ok_or_else(|| KairosError::validation("Histogram missing 'counts' field"))?;

        let counts_array = counts_value
            .as_array()
            .ok_or_else(|| KairosError::validation("Histogram 'counts' must be an array"))?;

        // Early length check to avoid partial processing
        if boundaries_array.len() != counts_array.len() {
            return Err(KairosError::validation(
                "Histogram boundaries and counts must have same length",
            ));
        }

        // Pre-allocate with known capacity and parse both arrays in single pass
        let len = boundaries_array.len();
        let mut bins = Vec::with_capacity(len);

        for (boundary_val, count_val) in boundaries_array.iter().zip(counts_array.iter()) {
            let boundary = boundary_val
                .as_f64()
                .filter(|&f| f.is_finite())
                .ok_or_else(|| KairosError::validation("Boundary must be a finite number"))?;

            let count = count_val
                .as_u64()
                .ok_or_else(|| KairosError::validation("Count must be a non-negative integer"))?;

            bins.push((boundary, count));
        }

        // Sort bins by boundary (only once, and only the references we need)
        bins.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        // Parse remaining fields with consolidated validation
        let sum = obj
            .get("sum")
            .and_then(|v| v.as_f64())
            .filter(|&f| f.is_finite())
            .unwrap_or(0.0);

        let min = obj
            .get("min")
            .and_then(|v| v.as_f64())
            .filter(|&f| f.is_finite())
            .unwrap_or(0.0);

        let max = obj
            .get("max")
            .and_then(|v| v.as_f64())
            .filter(|&f| f.is_finite())
            .unwrap_or(0.0);

        let histogram = HistogramData::from_bins(bins, sum, min, max, None)?;
        Ok(DataPointValue::Histogram(histogram))
    }

    /// Parse tags from HashMap
    fn parse_tags(&self, tags: &HashMap<String, String>) -> KairosResult<TagSet> {
        let mut tag_set = TagSet::new();

        for (key, value) in tags {
            let tag_key = TagKey::new(key).map_err(|e| {
                KairosError::validation(format!("Invalid tag key '{}': {}", key, e))
            })?;
            let tag_value = TagValue::new(value).map_err(|e| {
                KairosError::validation(format!("Invalid tag value '{}': {}", value, e))
            })?;
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
        })
        .to_string();

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
            "datapoints": [[1634567890000i64, std::f64::consts::PI]],
            "tags": {"host": "server1"}
        })
        .to_string();

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
        })
        .to_string();

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
                "datapoints": [[1634567890000i64, std::f64::consts::PI]],
                "tags": {"host": "server2"}
            }
        ])
        .to_string();

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
        })
        .to_string();

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
        })
        .to_string();

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
        })
        .to_string();

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
        })
        .to_string();

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
        })
        .to_string();

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
        })
        .to_string();

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
        })
        .to_string();

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

    #[test]
    fn test_parse_kairosdb_bins_histogram() {
        let parser = JsonParser::default();
        let json = json!({
            "name": "response_time_histogram",
            "datapoints": [[
                1634567890000i64,
                {
                    "bins": {
                        "0.1": 10,
                        "1.0": 50,
                        "5.0": 25,
                        "10.0": 5
                    },
                    "min": 0.05,
                    "max": 9.8,
                    "sum": 195.5,
                    "precision": 7
                }
            ]],
            "tags": {"service": "web"}
        })
        .to_string();

        let (batch, warnings) = parser.parse_json(&json).unwrap();
        assert_eq!(batch.len(), 1);
        assert!(warnings.is_empty());

        let point = &batch.points[0];
        assert_eq!(point.metric.as_str(), "response_time_histogram");
        assert!(matches!(point.value, DataPointValue::Histogram(_)));

        if let DataPointValue::Histogram(hist) = &point.value {
            // Check sorted bins
            let bins = hist.bins();
            assert_eq!(bins.len(), 4);
            assert_eq!(bins[0], (0.1, 10));
            assert_eq!(bins[1], (1.0, 50));
            assert_eq!(bins[2], (5.0, 25));
            assert_eq!(bins[3], (10.0, 5));

            assert_eq!(hist.sum(), 195.5);
            assert_eq!(hist.min(), 0.05);
            assert_eq!(hist.max(), 9.8);
            assert!((hist.mean() - 2.172222222222222).abs() < 1e-10); // sum/count = 195.5/90
            assert_eq!(hist.precision(), 7);
            assert_eq!(hist.total_count(), 90);
        }
    }

    #[test]
    fn test_parse_kairosdb_v1_histogram_no_precision() {
        let parser = JsonParser::default();
        let json = json!({
            "name": "latency_histogram",
            "datapoints": [[
                1634567890000i64,
                {
                    "bins": {
                        "0.0": 5,
                        "10.0": 15,
                        "50.0": 8
                    },
                    "min": 1.0,
                    "max": 45.0,
                    "sum": 425.0
                }
            ]],
            "tags": {"endpoint": "/api"}
        })
        .to_string();

        let (batch, _warnings) = parser.parse_json(&json).unwrap();
        assert_eq!(batch.len(), 1);

        if let DataPointValue::Histogram(hist) = &batch.points[0].value {
            assert_eq!(hist.precision(), 7); // Default V1 precision
            assert_eq!(hist.total_count(), 28);
        }
    }

    #[test]
    fn test_parse_direct_histogram() {
        let parser = JsonParser::default();
        let json = json!({
            "name": "response_time_histogram",
            "datapoints": [[
                1634567890000i64,
                {
                    "boundaries": [0.0, 10.0, 50.0, 100.0],
                    "counts": [5, 10, 8, 2],
                    "total_count": 25,
                    "sum": 650.5,
                    "min": 1.2,
                    "max": 95.0
                }
            ]],
            "tags": {"service": "web"}
        })
        .to_string();

        let (batch, warnings) = parser.parse_json(&json).unwrap();
        assert_eq!(batch.len(), 1);
        assert!(warnings.is_empty());

        let point = &batch.points[0];
        assert_eq!(point.metric.as_str(), "response_time_histogram");
        assert!(matches!(point.value, DataPointValue::Histogram(_)));

        if let DataPointValue::Histogram(hist) = &point.value {
            assert_eq!(hist.boundaries, vec![0.0, 10.0, 50.0, 100.0]);
            assert_eq!(hist.counts, vec![5, 10, 8, 2]);
            assert_eq!(hist.total_count(), 25);
            assert_eq!(hist.sum, 650.5);
            assert_eq!(hist.min, 1.2);
            assert_eq!(hist.max, 95.0);
        }
    }

    #[test]
    fn test_parse_prometheus_histogram() {
        let parser = JsonParser::default();
        let json = json!({
            "name": "http_request_duration",
            "datapoints": [[
                1634567890000i64,
                {
                    "buckets": [
                        {"le": 0.1, "count": 5},
                        {"le": 0.5, "count": 15},
                        {"le": 1.0, "count": 25},
                        {"le": 5.0, "count": 30}
                    ],
                    "count": 30,
                    "sum": 45.5
                }
            ]],
            "tags": {"method": "GET"}
        })
        .to_string();

        let (batch, warnings) = parser.parse_json(&json).unwrap();
        assert_eq!(batch.len(), 1);
        assert!(warnings.is_empty());

        let point = &batch.points[0];
        assert!(matches!(point.value, DataPointValue::Histogram(_)));

        if let DataPointValue::Histogram(hist) = &point.value {
            assert_eq!(hist.boundaries, vec![0.1, 0.5, 1.0, 5.0]);
            assert_eq!(hist.counts, vec![5, 10, 10, 5]); // Individual bucket counts, not cumulative
            assert_eq!(hist.total_count(), 30);
            assert_eq!(hist.sum, 45.5);
        }
    }

    #[test]
    fn test_parse_failing_e2e_histogram() {
        let parser = JsonParser::default();

        // Test with the exact boundaries from the failing end-to-end test
        let json = json!({
            "name": "test.histogram",
            "datapoints": [[
                1634567890000i64,
                {
                    "boundaries": [0.1, 0.5, 1.0, 5.0],
                    "counts": [15, 30, 20, 5],
                    "total_count": 70,
                    "sum": 55.5
                }
            ]],
            "tags": {"service": "rust-histogram-test"}
        })
        .to_string();

        let (batch, warnings) = parser.parse_json(&json).unwrap();
        assert_eq!(batch.len(), 1);
        assert!(warnings.is_empty());

        let point = &batch.points[0];
        assert_eq!(point.metric.as_str(), "test.histogram");

        if let DataPointValue::Histogram(hist) = &point.value {
            // Verify boundaries are sorted
            for i in 1..hist.boundaries.len() {
                assert!(
                    hist.boundaries[i] > hist.boundaries[i - 1],
                    "Boundaries not sorted after JSON parsing: {:?}",
                    hist.boundaries
                );
            }

            assert_eq!(hist.counts, vec![15, 30, 20, 5]);
            assert_eq!(hist.sum, 55.5);
        } else {
            panic!("Expected histogram value");
        }
    }

    #[test]
    fn test_parse_complex_number() {
        let parser = JsonParser::default();
        let json = json!({
            "name": "signal.complex",
            "datapoints": [[
                1634567890000i64,
                {
                    "real": std::f64::consts::PI,
                    "imaginary": 2.71
                }
            ]],
            "tags": {}
        })
        .to_string();

        let (batch, warnings) = parser.parse_json(&json).unwrap();
        assert_eq!(batch.len(), 1);
        assert!(warnings.is_empty());

        let point = &batch.points[0];
        assert!(matches!(
            point.value,
            DataPointValue::Complex {
                real: std::f64::consts::PI,
                imaginary: 2.71
            }
        ));
    }

    #[test]
    fn test_histogram_validation_errors() {
        let parser = JsonParser::default();

        // Test missing required fields
        let json = json!({
            "name": "test_histogram",
            "datapoints": [[
                1634567890000i64,
                {
                    "boundaries": [1.0, 2.0]
                    // Missing counts
                }
            ]],
            "tags": {}
        })
        .to_string();

        let result = parser.parse_json(&json);
        assert!(result.is_err());

        // Test mismatched array lengths
        let json = json!({
            "name": "test_histogram",
            "datapoints": [[
                1634567890000i64,
                {
                    "boundaries": [1.0, 2.0, 3.0],
                    "counts": [5, 10] // Wrong length
                }
            ]],
            "tags": {}
        })
        .to_string();

        let result = parser.parse_json(&json);
        assert!(result.is_err());
    }
}
