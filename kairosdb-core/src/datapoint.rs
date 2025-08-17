//! Data point types and operations

use chrono::{DateTime, Utc};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use validator::Validate;

use crate::error::{KairosError, KairosResult};
use crate::metrics::MetricName;
use crate::tags::{TagKey, TagSet, TagValue};
use crate::time::Timestamp;

/// A single time series data point
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataPoint {
    /// Metric name
    pub metric: MetricName,
    
    /// Timestamp when the measurement was taken
    pub timestamp: Timestamp,
    
    /// The measured value
    pub value: DataPointValue,
    
    /// Associated tags for this data point
    pub tags: TagSet,
    
    /// Optional TTL in seconds (0 means no TTL)
    #[serde(default)]
    pub ttl: u32,
}

/// Supported data point value types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DataPointValue {
    /// 64-bit signed integer
    Long(i64),
    
    /// 64-bit floating point number  
    Double(OrderedFloat<f64>),
    
    /// Complex number (for signal processing use cases)
    Complex { real: f64, imaginary: f64 },
    
    /// Text value (for log aggregation or categorical data)
    Text(String),
    
    /// Binary data (base64 encoded in JSON)
    #[serde(with = "base64_serde")]
    Binary(Vec<u8>),
}

/// Batch of data points for efficient processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPointBatch {
    /// Data points in this batch
    pub points: Vec<DataPoint>,
}

impl DataPoint {
    /// Create a new data point with long value
    pub fn new_long<M, T>(metric: M, timestamp: T, value: i64) -> Self
    where
        M: Into<MetricName>,
        T: Into<Timestamp>,
    {
        Self {
            metric: metric.into(),
            timestamp: timestamp.into(),
            value: DataPointValue::Long(value),
            tags: TagSet::new(),
            ttl: 0,
        }
    }
    
    /// Create a new data point with double value
    pub fn new_double<M, T>(metric: M, timestamp: T, value: f64) -> Self
    where
        M: Into<MetricName>,
        T: Into<Timestamp>,
    {
        Self {
            metric: metric.into(),
            timestamp: timestamp.into(),
            value: DataPointValue::Double(OrderedFloat(value)),
            tags: TagSet::new(),
            ttl: 0,
        }
    }
    
    /// Add a tag to this data point
    pub fn with_tag<K, V>(mut self, key: K, value: V) -> KairosResult<Self>
    where
        K: Into<TagKey>,
        V: Into<TagValue>,
    {
        self.tags.insert(key.into(), value.into())?;
        Ok(self)
    }
    
    /// Set TTL for this data point
    pub fn with_ttl(mut self, ttl_seconds: u32) -> Self {
        self.ttl = ttl_seconds;
        self
    }
    
    /// Get the data type identifier for Cassandra storage
    pub fn data_type(&self) -> &'static str {
        match self.value {
            DataPointValue::Long(_) => "kairos_long",
            DataPointValue::Double(_) => "kairos_double", 
            DataPointValue::Complex { .. } => "kairos_complex",
            DataPointValue::Text(_) => "kairos_string",
            DataPointValue::Binary(_) => "kairos_binary",
        }
    }
    
    /// Get the size estimate for this data point in bytes
    pub fn estimated_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let metric_size = self.metric.len();
        let tags_size = self.tags.estimated_size();
        let value_size = match &self.value {
            DataPointValue::Long(_) => 8,
            DataPointValue::Double(_) => 8,
            DataPointValue::Complex { .. } => 16,
            DataPointValue::Text(s) => s.len(),
            DataPointValue::Binary(b) => b.len(),
        };
        
        base_size + metric_size + tags_size + value_size
    }
    
    /// Convert timestamp to milliseconds since epoch
    pub fn timestamp_millis(&self) -> i64 {
        self.timestamp.timestamp_millis()
    }
    
    /// Validate the data point
    pub fn validate_self(&self) -> KairosResult<()> {
        // Manual validation since we removed the Validate derive
        if self.metric.is_empty() {
            return Err(KairosError::validation("Metric name cannot be empty"));
        }
        if self.metric.len() > crate::MAX_METRIC_NAME_LENGTH {
            return Err(KairosError::validation("Metric name too long"));
        }
        Ok(())
    }
}

impl DataPointValue {
    /// Convert to f64 if possible
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            DataPointValue::Long(v) => Some(*v as f64),
            DataPointValue::Double(v) => Some(v.into_inner()),
            DataPointValue::Complex { real, .. } => Some(*real),
            _ => None,
        }
    }
    
    /// Convert to i64 if possible  
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            DataPointValue::Long(v) => Some(*v),
            DataPointValue::Double(v) => {
                let val = v.into_inner();
                if val.fract() == 0.0 && val >= i64::MIN as f64 && val <= i64::MAX as f64 {
                    Some(val as i64)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    
    /// Check if this is a numeric value
    pub fn is_numeric(&self) -> bool {
        matches!(self, DataPointValue::Long(_) | DataPointValue::Double(_))
    }
    
    /// Get the value type name
    pub fn type_name(&self) -> &'static str {
        match self {
            DataPointValue::Long(_) => "long",
            DataPointValue::Double(_) => "double",
            DataPointValue::Complex { .. } => "complex",
            DataPointValue::Text(_) => "text", 
            DataPointValue::Binary(_) => "binary",
        }
    }
}

impl DataPointBatch {
    /// Create a new empty batch
    pub fn new() -> Self {
        Self {
            points: Vec::new(),
        }
    }
    
    /// Create a batch from a vector of data points
    pub fn from_points(points: Vec<DataPoint>) -> KairosResult<Self> {
        if points.len() > crate::MAX_BATCH_SIZE {
            return Err(KairosError::validation(format!(
                "Batch size {} exceeds maximum of {}",
                points.len(),
                crate::MAX_BATCH_SIZE
            )));
        }
        
        let batch = Self { points };
        batch.validate_self()?;
        Ok(batch)
    }
    
    /// Add a data point to the batch
    pub fn add_point(&mut self, point: DataPoint) -> KairosResult<()> {
        if self.points.len() >= crate::MAX_BATCH_SIZE {
            return Err(KairosError::validation("Batch is full"));
        }
        
        point.validate_self()?;
        self.points.push(point);
        Ok(())
    }
    
    /// Get the total estimated size of the batch
    pub fn estimated_size(&self) -> usize {
        self.points.iter().map(|p| p.estimated_size()).sum()
    }
    
    /// Get the number of data points in the batch
    pub fn len(&self) -> usize {
        self.points.len()
    }
    
    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }
    
    /// Validate the entire batch
    pub fn validate_self(&self) -> KairosResult<()> {
        if self.points.is_empty() {
            return Err(KairosError::validation("Batch cannot be empty"));
        }
        if self.points.len() > crate::MAX_BATCH_SIZE {
            return Err(KairosError::validation("Batch too large"));
        }
        for point in &self.points {
            point.validate_self()?;
        }
        Ok(())
    }
}

impl Default for DataPointBatch {
    fn default() -> Self {
        Self::new()
    }
}


/// Base64 serialization for binary data
mod base64_serde {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use serde::{Deserialize, Deserializer, Serializer};
    
    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }
    
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        STANDARD.decode(s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_data_point_creation() {
        let dp = DataPoint::new_long("test.metric", Timestamp::now(), 42);
        assert_eq!(dp.metric, "test.metric");
        assert!(matches!(dp.value, DataPointValue::Long(42)));
        assert_eq!(dp.ttl, 0);
    }
    
    #[test]
    fn test_data_point_with_tags() {
        let dp = DataPoint::new_double("test.metric", Timestamp::now(), 3.14)
            .with_tag("host", "server1")
            .unwrap()
            .with_tag("region", "us-east-1")
            .unwrap();
            
        assert_eq!(dp.tags.len(), 2);
        assert_eq!(dp.tags.get("host").unwrap(), "server1");
        assert_eq!(dp.tags.get("region").unwrap(), "us-east-1");
    }
    
    #[test]
    fn test_batch_operations() {
        let mut batch = DataPointBatch::new();
        
        let dp1 = DataPoint::new_long("metric1", Timestamp::now(), 1);
        let dp2 = DataPoint::new_long("metric2", Timestamp::now(), 2);
        
        batch.add_point(dp1).unwrap();
        batch.add_point(dp2).unwrap();
        
        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
    }
    
    #[test]
    fn test_value_conversions() {
        let long_val = DataPointValue::Long(42);
        assert_eq!(long_val.as_i64(), Some(42));
        assert_eq!(long_val.as_f64(), Some(42.0));
        
        let double_val = DataPointValue::Double(OrderedFloat(3.14));
        assert_eq!(double_val.as_f64(), Some(3.14));
        assert!(double_val.as_i64().is_none());
    }
}