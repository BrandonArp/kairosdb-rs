//! Data point types and operations

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::error::{KairosError, KairosResult};
use crate::histogram_key_utility::HistogramKeyUtility;
use crate::metrics::MetricName;
use crate::tags::{TagKey, TagSet, TagValue};
use crate::time::Timestamp;

use prost::Message;

// Include generated protobuf types with alias to avoid name conflict
mod proto {
    include!(concat!(
        env!("OUT_DIR"),
        "/io.inscopemetrics.kairosdb.proto.v2.rs"
    ));
}

// Alias the protobuf DataPoint to avoid conflict with our DataPoint struct
type ProtoDataPoint = proto::DataPoint;

/// Histogram data structure for storing aggregated measurements
/// Compatible with KairosDB histogram V1/V2 formats
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistogramData {
    /// Bucket boundaries (bin keys) - must be sorted
    /// These represent the bucket boundaries in TreeMap<Double, Long> format
    pub boundaries: Vec<f64>,

    /// Bucket counts (frequencies) - length must equal boundaries.len()
    pub counts: Vec<u64>,

    /// Sum of all observed values
    pub sum: f64,

    /// Mean of all observed values
    pub mean: f64,

    /// Minimum observed value
    pub min: f64,

    /// Maximum observed value  
    pub max: f64,

    /// Bucket precision in bits (default 7 for V1, configurable for V2)
    pub precision: u8,
}

/// Builder for creating histogram data points
#[derive(Debug, Clone)]
pub struct HistogramBuilder {
    boundaries: Vec<f64>,
    counts: Vec<u64>,
    sum: f64,
    mean: f64,
    min: f64,
    max: f64,
    precision: u8,
}

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

    /// Histogram data for aggregated metrics
    Histogram(HistogramData),
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

    /// Create a new data point with histogram value
    pub fn new_histogram<M, T>(metric: M, timestamp: T, histogram: HistogramData) -> Self
    where
        M: Into<MetricName>,
        T: Into<Timestamp>,
    {
        Self {
            metric: metric.into(),
            timestamp: timestamp.into(),
            value: DataPointValue::Histogram(histogram),
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
            DataPointValue::Histogram(_) => "kairos_histogram_v2", // Use V2 as default
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
            DataPointValue::Histogram(h) => h.estimated_size(),
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
            DataPointValue::Histogram(h) => Some(h.mean()),
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
        matches!(
            self,
            DataPointValue::Long(_) | DataPointValue::Double(_) | DataPointValue::Histogram(_)
        )
    }

    /// Get the value type name
    pub fn type_name(&self) -> &'static str {
        match self {
            DataPointValue::Long(_) => "long",
            DataPointValue::Double(_) => "double",
            DataPointValue::Complex { .. } => "complex",
            DataPointValue::Text(_) => "text",
            DataPointValue::Binary(_) => "binary",
            DataPointValue::Histogram(_) => "histogram",
        }
    }
}

impl DataPointBatch {
    /// Create a new empty batch
    pub fn new() -> Self {
        Self { points: Vec::new() }
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

impl HistogramData {
    /// Create a new histogram from bins (KairosDB TreeMap format)
    /// bins: TreeMap-like structure where key is bucket boundary, value is count
    pub fn from_bins(
        bins: Vec<(f64, u64)>,
        sum: f64,
        min: f64,
        max: f64,
        precision: Option<u8>,
    ) -> KairosResult<Self> {
        if bins.is_empty() {
            return Err(KairosError::validation("Histogram bins cannot be empty"));
        }

        let mut boundaries = Vec::new();
        let mut counts = Vec::new();

        for (boundary, count) in bins {
            if !boundary.is_finite() {
                return Err(KairosError::validation("Histogram boundary must be finite"));
            }
            boundaries.push(boundary);
            counts.push(count);
        }

        // Validate boundaries are sorted (KairosDB uses TreeMap which is sorted)
        for i in 1..boundaries.len() {
            if boundaries[i] <= boundaries[i - 1] {
                return Err(KairosError::validation(
                    "Histogram boundaries must be sorted",
                ));
            }
        }

        if !sum.is_finite() || !min.is_finite() || !max.is_finite() {
            return Err(KairosError::validation(
                "Histogram sum, min, max must be finite",
            ));
        }

        let total_count: u64 = counts.iter().sum();
        let mean = if total_count > 0 {
            sum / total_count as f64
        } else {
            0.0
        };

        Ok(Self {
            boundaries,
            counts,
            sum,
            mean,
            min,
            max,
            precision: precision.unwrap_or(7), // Default precision is 7 bits
        })
    }

    /// Get total count of all observations
    pub fn total_count(&self) -> u64 {
        self.counts.iter().sum()
    }

    /// Get mean value (pre-computed)
    pub fn mean(&self) -> f64 {
        self.mean
    }

    /// Calculate percentile (0.0 to 1.0)
    pub fn percentile(&self, p: f64) -> Option<f64> {
        let total = self.total_count();
        if !(0.0..=1.0).contains(&p) || total == 0 {
            return None;
        }

        let target_count = (p * total as f64) as u64;
        let mut cumulative_count = 0;

        for (i, &count) in self.counts.iter().enumerate() {
            cumulative_count += count;
            if cumulative_count >= target_count {
                if i == 0 {
                    return Some(self.min);
                } else {
                    // Linear interpolation within bucket
                    let bucket_start = if i > 0 {
                        self.boundaries[i - 1]
                    } else {
                        self.min
                    };
                    let bucket_end = self.boundaries[i];
                    let bucket_progress = if count > 0 {
                        (target_count - (cumulative_count - count)) as f64 / count as f64
                    } else {
                        0.0
                    };
                    return Some(bucket_start + bucket_progress * (bucket_end - bucket_start));
                }
            }
        }

        Some(self.max)
    }

    /// Get bins as KairosDB TreeMap format: Vec<(bucket_key, count)>
    pub fn bins(&self) -> Vec<(f64, u64)> {
        self.boundaries
            .iter()
            .zip(self.counts.iter())
            .map(|(&boundary, &count)| (boundary, count))
            .collect()
    }

    /// Get the sum of all observations
    pub fn sum(&self) -> f64 {
        self.sum
    }

    /// Get minimum value
    pub fn min(&self) -> f64 {
        self.min
    }

    /// Get maximum value
    pub fn max(&self) -> f64 {
        self.max
    }

    /// Get precision in bits
    pub fn precision(&self) -> u8 {
        self.precision
    }

    /// Merge another histogram into this one
    pub fn merge(&mut self, other: &HistogramData) -> KairosResult<()> {
        if self.boundaries != other.boundaries {
            return Err(KairosError::validation(
                "Cannot merge histograms with different boundaries",
            ));
        }

        for (i, &count) in other.counts.iter().enumerate() {
            self.counts[i] += count;
        }

        self.sum += other.sum;

        // Update min/max
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);

        // Recalculate mean
        let total = self.total_count();
        self.mean = if total > 0 {
            self.sum / total as f64
        } else {
            0.0
        };

        Ok(())
    }

    /// Get estimated size in bytes
    pub fn estimated_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.boundaries.len() * std::mem::size_of::<f64>()
            + self.counts.len() * std::mem::size_of::<u64>()
    }

    /// Get bucket count
    pub fn bucket_count(&self) -> usize {
        self.boundaries.len()
    }

    /// Check if histogram is empty
    pub fn is_empty(&self) -> bool {
        self.total_count() == 0
    }

    /// Serialize histogram as V2 binary format (Protocol Buffers)
    pub fn to_v2_bytes(&self) -> Vec<u8> {
        let key_utility = HistogramKeyUtility::get_instance(self.precision);

        let mut bucket_keys = Vec::new();
        let mut bucket_counts = Vec::new();

        for (boundary, count) in self.bins() {
            bucket_keys.push(key_utility.pack(boundary));
            bucket_counts.push(count);
        }

        let data_point = ProtoDataPoint {
            bucket_key: bucket_keys,
            bucket_count: bucket_counts,
            min: self.min,
            max: self.max,
            sum: self.sum,
            mean: self.mean,
            precision: self.precision as u32,
        };

        // Serialize to protobuf bytes
        let proto_bytes = data_point.encode_to_vec();

        // Prepend length as required by KairosDB V2 format
        let mut result = Vec::new();
        result.extend_from_slice(&(proto_bytes.len() as u32).to_be_bytes());
        result.extend_from_slice(&proto_bytes);
        result
    }

    /// Deserialize histogram from V2 binary format (Protocol Buffers)
    pub fn from_v2_bytes(bytes: &[u8]) -> KairosResult<Self> {
        if bytes.len() < 4 {
            return Err(KairosError::parse("V2 data too short".to_string()));
        }

        // Read length prefix
        let length = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;

        if bytes.len() < 4 + length {
            return Err(KairosError::parse("V2 data truncated".to_string()));
        }

        // Parse protobuf data
        let proto_data = ProtoDataPoint::decode(&bytes[4..4 + length])
            .map_err(|e| KairosError::parse(format!("Failed to decode V2 protobuf: {}", e)))?;

        if proto_data.bucket_key.len() != proto_data.bucket_count.len() {
            return Err(KairosError::validation("Mismatched bucket keys and counts"));
        }

        let precision = proto_data.precision as u8;
        let key_utility = HistogramKeyUtility::get_instance(precision);

        let mut bins = Vec::new();
        for (i, &packed_key) in proto_data.bucket_key.iter().enumerate() {
            let boundary = key_utility.unpack(packed_key);
            let count = proto_data.bucket_count[i];
            bins.push((boundary, count));
        }

        Self::from_bins(
            bins,
            proto_data.sum,
            proto_data.min,
            proto_data.max,
            Some(precision),
        )
    }
}

impl HistogramBuilder {
    /// Create a new histogram builder
    pub fn new() -> Self {
        Self {
            boundaries: Vec::new(),
            counts: Vec::new(),
            sum: 0.0,
            mean: 0.0,
            min: 0.0,
            max: 0.0,
            precision: 7, // Default V1 precision
        }
    }

    /// Set bins (KairosDB TreeMap format)
    pub fn bins(mut self, bins: Vec<(f64, u64)>) -> Self {
        self.boundaries = bins.iter().map(|(k, _)| *k).collect();
        self.counts = bins.iter().map(|(_, v)| *v).collect();
        self
    }

    /// Set bucket boundaries
    pub fn boundaries(mut self, boundaries: Vec<f64>) -> Self {
        self.boundaries = boundaries;
        self.counts = vec![0; self.boundaries.len()];
        self
    }

    /// Set bucket counts
    pub fn counts(mut self, counts: Vec<u64>) -> Self {
        self.counts = counts;
        self
    }

    /// Set sum
    pub fn sum(mut self, sum: f64) -> Self {
        self.sum = sum;
        self
    }

    /// Set mean
    pub fn mean(mut self, mean: f64) -> Self {
        self.mean = mean;
        self
    }

    /// Set min value
    pub fn min(mut self, min: f64) -> Self {
        self.min = min;
        self
    }

    /// Set max value
    pub fn max(mut self, max: f64) -> Self {
        self.max = max;
        self
    }

    /// Set precision
    pub fn precision(mut self, precision: u8) -> Self {
        self.precision = precision;
        self
    }

    /// Build the histogram
    pub fn build(self) -> KairosResult<HistogramData> {
        let bins: Vec<(f64, u64)> = self.boundaries.into_iter().zip(self.counts).collect();

        HistogramData::from_bins(bins, self.sum, self.min, self.max, Some(self.precision))
    }
}

impl Default for HistogramBuilder {
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
        let dp = DataPoint::new_double("test.metric", Timestamp::now(), std::f64::consts::PI)
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

        let double_val = DataPointValue::Double(OrderedFloat(std::f64::consts::PI));
        assert_eq!(double_val.as_f64(), Some(std::f64::consts::PI));
        assert!(double_val.as_i64().is_none());
    }

    #[test]
    fn test_histogram_creation() {
        let boundaries = vec![0.0, 10.0, 50.0, 100.0];
        let bins: Vec<(f64, u64)> = boundaries.iter().map(|&b| (b, 0)).collect();
        let hist = HistogramData::from_bins(bins, 0.0, 0.0, 0.0, None).unwrap();

        assert_eq!(hist.boundaries, boundaries);
        assert_eq!(hist.counts, vec![0, 0, 0, 0]);
        assert_eq!(hist.total_count(), 0);
        assert_eq!(hist.sum, 0.0);
        assert_eq!(hist.min, 0.0);
        assert_eq!(hist.max, 0.0);
    }

    #[test]
    fn test_histogram_from_bins() {
        let bins = vec![(0.0, 5), (10.0, 10), (50.0, 8), (100.0, 2)];
        let hist = HistogramData::from_bins(bins, 650.5, 1.2, 95.0, Some(7)).unwrap();

        assert_eq!(hist.boundaries, vec![0.0, 10.0, 50.0, 100.0]);
        assert_eq!(hist.counts, vec![5, 10, 8, 2]);
        assert_eq!(hist.total_count(), 25);
        assert_eq!(hist.sum, 650.5);
        assert_eq!(hist.min, 1.2);
        assert_eq!(hist.max, 95.0);
        assert_eq!(hist.mean(), 26.02);
        assert_eq!(hist.precision(), 7);
    }

    #[test]
    fn test_histogram_percentiles() {
        // Create histogram with pre-aggregated bucket data
        let bins = vec![(0.0, 10), (10.0, 20), (20.0, 30), (30.0, 40)];
        let hist = HistogramData::from_bins(bins, 2500.0, 5.0, 35.0, None).unwrap();

        // Test percentiles
        assert!(hist.percentile(0.1).is_some()); // 10th percentile
        assert!(hist.percentile(0.5).is_some()); // 50th percentile (median)
        assert!(hist.percentile(0.9).is_some()); // 90th percentile
    }

    #[test]
    fn test_histogram_merge() {
        let bins1 = vec![(0.0, 2), (10.0, 3), (20.0, 1)];
        let bins2 = vec![(0.0, 1), (10.0, 2), (20.0, 3)];

        let mut hist1 = HistogramData::from_bins(bins1, 50.0, 5.0, 25.0, None).unwrap();
        let hist2 = HistogramData::from_bins(bins2, 70.0, 3.0, 27.0, None).unwrap();

        hist1.merge(&hist2).unwrap();

        assert_eq!(hist1.total_count(), 12);
        assert_eq!(hist1.sum, 120.0);
        assert_eq!(hist1.min, 3.0);
        assert_eq!(hist1.max, 27.0);
        assert_eq!(hist1.counts, vec![3, 5, 4]);
    }

    #[test]
    fn test_histogram_data_point() {
        let bins = vec![(0.0, 1), (10.0, 0), (50.0, 1), (100.0, 0)];
        let hist = HistogramData::from_bins(bins, 100.0, 25.0, 75.0, None).unwrap();

        let dp = DataPoint::new_histogram("response_time", Timestamp::now(), hist);

        assert_eq!(dp.metric, "response_time");
        assert!(matches!(dp.value, DataPointValue::Histogram(_)));
        assert_eq!(dp.data_type(), "kairos_histogram_v2");
        assert!(dp.value.is_numeric());

        if let DataPointValue::Histogram(h) = &dp.value {
            assert_eq!(h.total_count(), 2);
            assert_eq!(h.mean(), 50.0);
        }
    }

    #[test]
    fn test_histogram_builder() {
        let hist = HistogramBuilder::new()
            .boundaries(vec![0.0, 10.0, 50.0])
            .counts(vec![5, 10, 3])
            .sum(270.0)
            .min(1.0)
            .max(45.0)
            .build()
            .unwrap();

        assert_eq!(hist.boundaries, vec![0.0, 10.0, 50.0]);
        assert_eq!(hist.counts, vec![5, 10, 3]);
        assert_eq!(hist.total_count(), 18);
        assert_eq!(hist.sum, 270.0);
        assert_eq!(hist.min, 1.0);
        assert_eq!(hist.max, 45.0);
        assert_eq!(hist.mean(), 15.0);
    }
}
