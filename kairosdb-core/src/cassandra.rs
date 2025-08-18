//! Cassandra integration types and utilities

use crate::datapoint::{DataPoint, DataPointValue};
use crate::error::{KairosError, KairosResult};
use crate::metrics::MetricName;
use crate::tags::TagSet;
use crate::time::Timestamp;

/// Cassandra table names used by KairosDB
#[derive(Debug, Clone)]
pub struct TableNames {
    pub data_points: &'static str,
    pub row_key_index: &'static str,
    pub string_index: &'static str,
}

impl Default for TableNames {
    fn default() -> Self {
        Self {
            data_points: "data_points",
            row_key_index: "row_key_index",
            string_index: "string_index",
        }
    }
}

/// Row key components for Cassandra partitioning
#[derive(Debug, Clone, PartialEq)]
pub struct RowKey {
    /// Metric name
    pub metric: MetricName,
    /// Data type identifier
    pub data_type: String,
    /// Row time (3-week boundary)
    pub row_time: Timestamp,
    /// Tags in Cassandra format
    pub tags: String,
}

impl RowKey {
    /// Create a row key from a data point
    pub fn from_data_point(point: &DataPoint) -> Self {
        Self {
            metric: point.metric.clone(),
            data_type: point.data_type().to_string(),
            row_time: point.timestamp.row_time(),
            tags: point.tags.to_cassandra_format(),
        }
    }
    
    /// Get the row key as bytes for Cassandra
    pub fn to_bytes(&self) -> Vec<u8> {
        // Create a consistent byte representation for the row key
        let mut bytes = Vec::new();
        
        // Add metric name length and data
        let metric_bytes = self.metric.as_str().as_bytes();
        bytes.extend_from_slice(&(metric_bytes.len() as u32).to_be_bytes());
        bytes.extend_from_slice(metric_bytes);
        
        // Add data type length and data
        let type_bytes = self.data_type.as_bytes();
        bytes.extend_from_slice(&(type_bytes.len() as u32).to_be_bytes());
        bytes.extend_from_slice(type_bytes);
        
        // Add row time
        bytes.extend_from_slice(&self.row_time.timestamp_millis().to_be_bytes());
        
        // Add tags length and data
        let tags_bytes = self.tags.as_bytes();
        bytes.extend_from_slice(&(tags_bytes.len() as u32).to_be_bytes());
        bytes.extend_from_slice(tags_bytes);
        
        bytes
    }
    
    /// Parse row key from bytes
    pub fn from_bytes(bytes: &[u8]) -> KairosResult<Self> {
        let mut offset = 0;
        
        // Read metric name
        if bytes.len() < offset + 4 {
            return Err(KairosError::parse("Invalid row key: too short for metric length"));
        }
        let metric_len = u32::from_be_bytes([
            bytes[offset], bytes[offset + 1], 
            bytes[offset + 2], bytes[offset + 3]
        ]) as usize;
        offset += 4;
        
        if bytes.len() < offset + metric_len {
            return Err(KairosError::parse("Invalid row key: too short for metric data"));
        }
        let metric = MetricName::new(String::from_utf8_lossy(&bytes[offset..offset + metric_len]))?;
        offset += metric_len;
        
        // Read data type
        if bytes.len() < offset + 4 {
            return Err(KairosError::parse("Invalid row key: too short for type length"));
        }
        let type_len = u32::from_be_bytes([
            bytes[offset], bytes[offset + 1],
            bytes[offset + 2], bytes[offset + 3]
        ]) as usize;
        offset += 4;
        
        if bytes.len() < offset + type_len {
            return Err(KairosError::parse("Invalid row key: too short for type data"));
        }
        let data_type = String::from_utf8_lossy(&bytes[offset..offset + type_len]).to_string();
        offset += type_len;
        
        // Read row time
        if bytes.len() < offset + 8 {
            return Err(KairosError::parse("Invalid row key: too short for row time"));
        }
        let row_time_millis = i64::from_be_bytes([
            bytes[offset], bytes[offset + 1], bytes[offset + 2], bytes[offset + 3],
            bytes[offset + 4], bytes[offset + 5], bytes[offset + 6], bytes[offset + 7]
        ]);
        let row_time = Timestamp::from_millis(row_time_millis)?;
        offset += 8;
        
        // Read tags
        if bytes.len() < offset + 4 {
            return Err(KairosError::parse("Invalid row key: too short for tags length"));
        }
        let tags_len = u32::from_be_bytes([
            bytes[offset], bytes[offset + 1],
            bytes[offset + 2], bytes[offset + 3]
        ]) as usize;
        offset += 4;
        
        if bytes.len() < offset + tags_len {
            return Err(KairosError::parse("Invalid row key: too short for tags data"));
        }
        let tags = String::from_utf8_lossy(&bytes[offset..offset + tags_len]).to_string();
        
        Ok(Self {
            metric,
            data_type,
            row_time,
            tags,
        })
    }
}

/// Column name for a data point in Cassandra
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnName {
    /// Offset within the row (timestamp - row_time)
    pub offset: i64,
    /// Optional additional qualifier for uniqueness
    pub qualifier: Option<String>,
}

impl ColumnName {
    /// Create a column name from a timestamp
    pub fn from_timestamp(timestamp: Timestamp) -> Self {
        Self {
            offset: timestamp.row_offset(),
            qualifier: None,
        }
    }
    
    /// Create a column name with a qualifier
    pub fn with_qualifier(timestamp: Timestamp, qualifier: String) -> Self {
        Self {
            offset: timestamp.row_offset(),
            qualifier: Some(qualifier),
        }
    }
    
    /// Get the column name as bytes for Cassandra
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        
        // Add offset
        bytes.extend_from_slice(&self.offset.to_be_bytes());
        
        // Add qualifier if present
        if let Some(ref qualifier) = self.qualifier {
            let qualifier_bytes = qualifier.as_bytes();
            bytes.extend_from_slice(&(qualifier_bytes.len() as u32).to_be_bytes());
            bytes.extend_from_slice(qualifier_bytes);
        } else {
            bytes.extend_from_slice(&0u32.to_be_bytes());
        }
        
        bytes
    }
    
    /// Parse column name from bytes
    pub fn from_bytes(bytes: &[u8]) -> KairosResult<Self> {
        if bytes.len() < 8 {
            return Err(KairosError::parse("Invalid column name: too short for offset"));
        }
        
        let offset = i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7]
        ]);
        
        let qualifier = if bytes.len() > 12 {
            let qualifier_len = u32::from_be_bytes([
                bytes[8], bytes[9], bytes[10], bytes[11]
            ]) as usize;
            
            if qualifier_len > 0 && bytes.len() >= 12 + qualifier_len {
                Some(String::from_utf8_lossy(&bytes[12..12 + qualifier_len]).to_string())
            } else {
                None
            }
        } else {
            None
        };
        
        Ok(Self { offset, qualifier })
    }
}

/// Cassandra value encoding for different data types
#[derive(Debug, Clone)]
pub struct CassandraValue {
    /// The encoded value bytes
    pub bytes: Vec<u8>,
    /// Optional TTL in seconds
    pub ttl: Option<u32>,
}

impl CassandraValue {
    /// Encode a data point value for Cassandra storage
    pub fn from_data_point_value(value: &DataPointValue, ttl: Option<u32>) -> Self {
        let bytes = match value {
            DataPointValue::Long(v) => v.to_be_bytes().to_vec(),
            DataPointValue::Double(v) => v.to_be_bytes().to_vec(),
            DataPointValue::Complex { real, imaginary } => {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&real.to_be_bytes());
                bytes.extend_from_slice(&imaginary.to_be_bytes());
                bytes
            }
            DataPointValue::Text(s) => s.as_bytes().to_vec(),
            DataPointValue::Binary(b) => b.clone(),
            DataPointValue::Histogram(h) => {
                // Serialize histogram as binary data
                // Format: [boundaries_len][boundaries...][counts...][total_count][sum][min][max]
                let mut bytes = Vec::new();
                
                // Write boundaries length
                bytes.extend_from_slice(&(h.boundaries.len() as u32).to_be_bytes());
                
                // Write boundaries
                for boundary in &h.boundaries {
                    bytes.extend_from_slice(&boundary.to_be_bytes());
                }
                
                // Write counts
                for count in &h.counts {
                    bytes.extend_from_slice(&count.to_be_bytes());
                }
                
                // Write metadata  
                bytes.extend_from_slice(&h.total_count().to_le_bytes());
                bytes.extend_from_slice(&h.sum.to_le_bytes());
                bytes.extend_from_slice(&h.mean.to_le_bytes());
                bytes.extend_from_slice(&h.min.to_le_bytes());
                bytes.extend_from_slice(&h.max.to_le_bytes());
                bytes.push(h.precision);
                
                bytes
            }
        };
        
        Self { bytes, ttl }
    }
    
    /// Decode a Cassandra value back to a data point value
    pub fn to_data_point_value(&self, data_type: &str) -> KairosResult<DataPointValue> {
        match data_type {
            "kairos_long" => {
                if self.bytes.len() != 8 {
                    return Err(KairosError::parse("Invalid long value length"));
                }
                let value = i64::from_be_bytes([
                    self.bytes[0], self.bytes[1], self.bytes[2], self.bytes[3],
                    self.bytes[4], self.bytes[5], self.bytes[6], self.bytes[7]
                ]);
                Ok(DataPointValue::Long(value))
            }
            "kairos_double" => {
                if self.bytes.len() != 8 {
                    return Err(KairosError::parse("Invalid double value length"));
                }
                let value = f64::from_be_bytes([
                    self.bytes[0], self.bytes[1], self.bytes[2], self.bytes[3],
                    self.bytes[4], self.bytes[5], self.bytes[6], self.bytes[7]
                ]);
                Ok(DataPointValue::Double(value.into()))
            }
            "kairos_complex" => {
                if self.bytes.len() != 16 {
                    return Err(KairosError::parse("Invalid complex value length"));
                }
                let real = f64::from_be_bytes([
                    self.bytes[0], self.bytes[1], self.bytes[2], self.bytes[3],
                    self.bytes[4], self.bytes[5], self.bytes[6], self.bytes[7]
                ]);
                let imaginary = f64::from_be_bytes([
                    self.bytes[8], self.bytes[9], self.bytes[10], self.bytes[11],
                    self.bytes[12], self.bytes[13], self.bytes[14], self.bytes[15]
                ]);
                Ok(DataPointValue::Complex { real, imaginary })
            }
            "kairos_string" => {
                let text = String::from_utf8_lossy(&self.bytes).to_string();
                Ok(DataPointValue::Text(text))
            }
            "kairos_binary" => {
                Ok(DataPointValue::Binary(self.bytes.clone()))
            }
            _ => Err(KairosError::parse(format!("Unknown data type: {}", data_type)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_row_key_encoding() {
        let point = DataPoint::new_long("test.metric", Timestamp::now(), 42);
        let row_key = RowKey::from_data_point(&point);
        
        let bytes = row_key.to_bytes();
        let decoded = RowKey::from_bytes(&bytes).unwrap();
        
        assert_eq!(row_key, decoded);
    }
    
    #[test]
    fn test_column_name_encoding() {
        let col = ColumnName::from_timestamp(Timestamp::now());
        
        let bytes = col.to_bytes();
        let decoded = ColumnName::from_bytes(&bytes).unwrap();
        
        assert_eq!(col, decoded);
    }
    
    #[test]
    fn test_value_encoding() {
        let value = DataPointValue::Long(12345);
        let cassandra_value = CassandraValue::from_data_point_value(&value, None);
        let decoded = cassandra_value.to_data_point_value("kairos_long").unwrap();
        
        assert_eq!(value, decoded);
    }
}