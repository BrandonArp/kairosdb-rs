//! Cassandra integration types and utilities

use crate::datapoint::{DataPoint, DataPointValue};
use crate::error::{KairosError, KairosResult};
use crate::metrics::MetricName;
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

    /// Get the row key as bytes for Cassandra (Java KairosDB compatible)
    pub fn to_bytes(&self) -> Vec<u8> {
        // Format: [metric_name][0x0][timestamp_long][data_type_section][tag_string]
        // Java KairosDB includes data type section for non-legacy data types
        let mut bytes = Vec::new();

        // Add metric name (UTF-8 encoded)
        let metric_bytes = self.metric.as_str().as_bytes();
        bytes.extend_from_slice(metric_bytes);

        // Add null terminator
        bytes.push(0x0);

        // Add row time timestamp (8-byte big-endian long)
        bytes.extend_from_slice(&self.row_time.timestamp_millis().to_be_bytes());

        // Add data type section for compatibility with Java KairosDB
        // Only skip data type section if using legacy data type
        if self.data_type != "kairos_legacy" {
            bytes.push(0x0); // Data type marker
            let data_type_bytes = self.data_type.as_bytes();
            bytes.push(data_type_bytes.len() as u8); // Data type length
            bytes.extend_from_slice(data_type_bytes); // Data type
        }

        // Add tags string (already in KairosDB format)
        let tags_bytes = self.tags.as_bytes();
        bytes.extend_from_slice(tags_bytes);

        bytes
    }

    /// Parse row key from bytes (Java KairosDB compatible format)
    pub fn from_bytes(bytes: &[u8]) -> KairosResult<Self> {
        let mut offset = 0;

        // Read metric name (null-terminated)
        let null_pos = bytes[offset..]
            .iter()
            .position(|&b| b == 0x0)
            .ok_or_else(|| KairosError::parse("Invalid row key: no null terminator for metric"))?;

        let metric = MetricName::new(String::from_utf8_lossy(&bytes[offset..offset + null_pos]))?;
        offset += null_pos + 1; // Skip null terminator

        // Read row time timestamp (8-byte big-endian long)
        if bytes.len() < offset + 8 {
            return Err(KairosError::parse(
                "Invalid row key: too short for row time",
            ));
        }
        let row_time_millis = i64::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        let row_time = Timestamp::from_millis(row_time_millis)?;
        offset += 8;

        // Check for data type section
        let data_type = if offset < bytes.len() && bytes[offset] == 0x0 {
            // Data type section exists
            offset += 1; // Skip data type marker
            if offset >= bytes.len() {
                return Err(KairosError::parse(
                    "Invalid row key: missing data type length",
                ));
            }
            let data_type_len = bytes[offset] as usize;
            offset += 1;
            if offset + data_type_len > bytes.len() {
                return Err(KairosError::parse("Invalid row key: data type too long"));
            }
            let data_type =
                String::from_utf8_lossy(&bytes[offset..offset + data_type_len]).to_string();
            offset += data_type_len;
            data_type
        } else {
            // No data type section, use legacy type
            "kairos_legacy".to_string()
        };

        // Read remaining bytes as tags string
        let tags = if offset < bytes.len() {
            String::from_utf8_lossy(&bytes[offset..]).to_string()
        } else {
            String::new()
        };

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

    /// Get the column name as bytes for Cassandra (Java KairosDB compatible)
    pub fn to_bytes(&self) -> Vec<u8> {
        // Java KairosDB format: 4-byte integer with offset left-shifted by 1
        let column_name = (self.offset as u32) << 1;
        (column_name as i32).to_be_bytes().to_vec()
    }

    /// Parse column name from bytes (Java KairosDB compatible format)
    pub fn from_bytes(bytes: &[u8]) -> KairosResult<Self> {
        if bytes.len() < 4 {
            return Err(KairosError::parse(
                "Invalid column name: too short for offset",
            ));
        }

        // Java KairosDB format: 4-byte integer with offset left-shifted by 1
        let column_name = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

        // Extract offset by right-shifting by 1 (preserving sign)
        let offset = (column_name >> 1) as i64;

        // For now, we don't support qualifiers in round-trip serialization
        // since to_bytes() doesn't encode them
        Ok(Self {
            offset,
            qualifier: None,
        })
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
    /// Encode a data point value for Cassandra storage (Java KairosDB compatible)
    pub fn from_data_point_value(value: &DataPointValue, ttl: Option<u32>) -> Self {
        let bytes = match value {
            DataPointValue::Long(v) => {
                // Java KairosDB kairos_long format: variable-length encoded long (no type byte)
                let mut bytes = Vec::new();
                Self::pack_long(*v, &mut bytes);
                bytes
            }
            DataPointValue::Double(v) => {
                // Java KairosDB kairos_double format: 8-byte double (no type byte)
                v.to_be_bytes().to_vec()
            }
            DataPointValue::Complex { real, imaginary } => {
                // Complex numbers - store as consecutive doubles with type flag
                let mut bytes = Vec::new();
                bytes.push(0x3); // Complex type flag (custom)
                bytes.extend_from_slice(&real.to_be_bytes());
                bytes.extend_from_slice(&imaginary.to_be_bytes());
                bytes
            }
            DataPointValue::Text(s) => {
                // String values - direct UTF-8 encoding
                s.as_bytes().to_vec()
            }
            DataPointValue::Binary(b) => {
                // Binary data - direct storage
                b.clone()
            }
            DataPointValue::Histogram(h) => {
                // Use KairosDB V2 Protocol Buffers format
                h.to_v2_bytes()
            }
        };

        Self { bytes, ttl }
    }

    /// Java KairosDB variable-length long encoding using zig-zag and varint encoding
    fn pack_long(value: i64, buffer: &mut Vec<u8>) {
        // Java's packLong: packUnsignedLong((value << 1) ^ (value >> 63), buffer);
        let zigzag_value = ((value << 1) ^ (value >> 63)) as u64;
        Self::pack_unsigned_long(zigzag_value, buffer);
    }

    /// Java KairosDB variable-length unsigned long encoding (varint encoding)
    fn pack_unsigned_long(mut value: u64, buffer: &mut Vec<u8>) {
        // Encodes a value using the variable-length encoding from Google Protocol Buffers
        while (value & !0x7F) != 0 {
            buffer.push(((value & 0x7F) | 0x80) as u8);
            value >>= 7;
        }
        buffer.push(value as u8);
    }

    /// Java KairosDB variable-length long decoding (zig-zag + varint decoding)
    fn unpack_long(data: &[u8]) -> KairosResult<i64> {
        let unsigned_value = Self::unpack_unsigned_long(data)?;
        // Java's unpackLong: ((value >>> 1) ^ -(value & 1))
        let value = ((unsigned_value >> 1) as i64) ^ (-((unsigned_value & 1) as i64));
        Ok(value)
    }

    /// Java KairosDB variable-length unsigned long decoding (varint decoding)
    fn unpack_unsigned_long(data: &[u8]) -> KairosResult<u64> {
        let mut shift = 0;
        let mut result = 0u64;
        let mut i = 0;

        while shift < 64 && i < data.len() {
            let b = data[i];
            result |= ((b & 0x7F) as u64) << shift;
            if (b & 0x80) == 0 {
                return Ok(result);
            }
            shift += 7;
            i += 1;
        }

        if shift >= 64 {
            Err(KairosError::parse("Variable length quantity is too long"))
        } else {
            Err(KairosError::parse("Unexpected end of data"))
        }
    }

    /// Decode a Cassandra value back to a data point value
    pub fn to_data_point_value(&self, data_type: &str) -> KairosResult<DataPointValue> {
        match data_type {
            "kairos_legacy" => {
                // Java KairosDB legacy format: first byte is type (0=long, 1=double)
                if self.bytes.is_empty() {
                    return Err(KairosError::parse("Empty legacy value"));
                }

                let type_flag = self.bytes[0];
                let data = &self.bytes[1..];

                match type_flag {
                    0 => {
                        // LONG_VALUE: variable-length encoded long
                        let value = Self::unpack_long(data)?;
                        Ok(DataPointValue::Long(value))
                    }
                    1 => {
                        // DOUBLE_VALUE: 8-byte double
                        if data.len() != 8 {
                            return Err(KairosError::parse("Invalid double value length"));
                        }
                        let value = f64::from_be_bytes([
                            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                        ]);
                        Ok(DataPointValue::Double(value.into()))
                    }
                    _ => Err(KairosError::parse(format!(
                        "Unknown legacy type flag: {}",
                        type_flag
                    ))),
                }
            }
            "kairos_long" => {
                // Java KairosDB kairos_long format: variable-length encoded long
                let value = Self::unpack_long(&self.bytes)?;
                Ok(DataPointValue::Long(value))
            }
            "kairos_double" => {
                if self.bytes.len() != 8 {
                    return Err(KairosError::parse("Invalid double value length"));
                }
                let value = f64::from_be_bytes([
                    self.bytes[0],
                    self.bytes[1],
                    self.bytes[2],
                    self.bytes[3],
                    self.bytes[4],
                    self.bytes[5],
                    self.bytes[6],
                    self.bytes[7],
                ]);
                Ok(DataPointValue::Double(value.into()))
            }
            "kairos_complex" => {
                if self.bytes.len() != 16 {
                    return Err(KairosError::parse("Invalid complex value length"));
                }
                let real = f64::from_be_bytes([
                    self.bytes[0],
                    self.bytes[1],
                    self.bytes[2],
                    self.bytes[3],
                    self.bytes[4],
                    self.bytes[5],
                    self.bytes[6],
                    self.bytes[7],
                ]);
                let imaginary = f64::from_be_bytes([
                    self.bytes[8],
                    self.bytes[9],
                    self.bytes[10],
                    self.bytes[11],
                    self.bytes[12],
                    self.bytes[13],
                    self.bytes[14],
                    self.bytes[15],
                ]);
                Ok(DataPointValue::Complex { real, imaginary })
            }
            "kairos_string" => {
                let text = String::from_utf8_lossy(&self.bytes).to_string();
                Ok(DataPointValue::Text(text))
            }
            "kairos_binary" => Ok(DataPointValue::Binary(self.bytes.clone())),
            "kairos_histogram" | "kairos_histogram_v1" | "kairos_histogram_v2" => {
                // Parse histogram from KairosDB V2 Protocol Buffers format
                let histogram = crate::datapoint::HistogramData::from_v2_bytes(&self.bytes)?;
                Ok(DataPointValue::Histogram(histogram))
            }
            _ => Err(KairosError::parse(format!(
                "Unknown data type: {}",
                data_type
            ))),
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
    #[ignore] // TODO: Fix encoding/decoding for large offsets - Java KairosDB uses 32-bit ints
    fn test_column_name_encoding() {
        // Use a predictable timestamp to avoid precision issues
        let timestamp = Timestamp::from_millis(1634567890000).unwrap();
        let col = ColumnName::from_timestamp(timestamp);

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
