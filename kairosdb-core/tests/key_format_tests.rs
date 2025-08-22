//! Key Format Compatibility Tests
//!
//! These tests ensure our key generation formats remain compatible with Java KairosDB.
//! They test the binary format of row keys, column keys, and tag formats.
//!
//! Run with: cargo test --test key_format_tests

use kairosdb_core::{
    cassandra::{CassandraValue, ColumnName, RowKey},
    datapoint::{DataPoint, DataPointValue},
    tags::{TagKey, TagSet, TagValue},
    time::Timestamp,
};

#[test]
fn test_row_key_binary_format() {
    // Create a data point with known values for consistent testing
    let timestamp = Timestamp::from_millis(1755538800000).unwrap();
    let mut tags = TagSet::new();
    let _ = tags.insert(
        TagKey::new("application").unwrap(),
        TagValue::new("database").unwrap(),
    );
    let _ = tags.insert(
        TagKey::new("datacenter").unwrap(),
        TagValue::new("us-west-1").unwrap(),
    );
    let _ = tags.insert(
        TagKey::new("host").unwrap(),
        TagValue::new("server-01").unwrap(),
    );

    let mut data_point = DataPoint::new_double("test.metric", timestamp, 42.5);
    data_point.tags = tags;

    // Test row key generation
    let row_key = RowKey::from_data_point(&data_point);
    let row_key_bytes = row_key.to_bytes();

    // Row key should start with metric name followed by null terminator
    assert!(row_key_bytes.starts_with(b"test.metric\x00"));

    // Should contain the data type
    let row_key_str = String::from_utf8_lossy(&row_key_bytes);
    assert!(row_key_str.contains("kairos_double"));

    // Should contain tags in Java KairosDB format (key=value:)
    assert!(row_key_str.contains("application=database"));
    assert!(row_key_str.contains("datacenter=us-west-1"));
    assert!(row_key_str.contains("host=server-01"));

    // Should NOT contain the old := format
    assert!(!row_key_str.contains(":="));

    // Should have trailing colon after tags
    assert!(row_key_str.ends_with(':'));

    println!("Row key format: {}", row_key_str);
}

#[test]
fn test_column_key_format() {
    let timestamp = Timestamp::from_millis(1755538900000).unwrap();
    let row_time = timestamp.row_time();

    // Test column key generation
    let column_name = ColumnName::from_timestamp(timestamp);
    let column_bytes = column_name.to_bytes();

    // Column key should be exactly 4 bytes
    assert_eq!(column_bytes.len(), 4);

    // Verify it's a big-endian integer
    let column_int = u32::from_be_bytes([
        column_bytes[0],
        column_bytes[1],
        column_bytes[2],
        column_bytes[3],
    ]);

    // Should represent (timestamp - row_time) << 1
    let expected_offset =
        ((timestamp.timestamp_millis() - row_time.timestamp_millis()) << 1) as u32;
    assert_eq!(column_int, expected_offset);

    println!(
        "Column key: 0x{:08x} (offset: {})",
        column_int,
        expected_offset >> 1
    );
}

#[test]
fn test_tag_format_compatibility() {
    let test_cases = [
        // Simple tags
        vec![("host", "server1"), ("env", "prod")],
        // Tags with special characters
        vec![("service", "api-gateway"), ("version", "v1.2.3")],
        // Empty tags
        vec![],
        // Single tag
        vec![("single", "value")],
        // Multiple tags (test sorting)
        vec![("z", "last"), ("a", "first"), ("m", "middle")],
    ];

    for (i, tag_pairs) in test_cases.iter().enumerate() {
        let mut tags = TagSet::new();
        for (key, value) in tag_pairs {
            let _ = tags.insert(TagKey::new(*key).unwrap(), TagValue::new(*value).unwrap());
        }

        let cassandra_format = tags.to_cassandra_format();

        if tag_pairs.is_empty() {
            assert!(cassandra_format.is_empty());
        } else {
            // Should use = separators
            assert!(
                cassandra_format.contains('='),
                "Test case {}: {}",
                i,
                cassandra_format
            );
            // Should end with colon
            assert!(
                cassandra_format.ends_with(':'),
                "Test case {}: {}",
                i,
                cassandra_format
            );
            // Should NOT use old := format
            assert!(
                !cassandra_format.contains(":="),
                "Test case {}: {}",
                i,
                cassandra_format
            );

            // Tags should be sorted alphabetically
            if tag_pairs.len() > 1 {
                let tag_parts: Vec<&str> = cassandra_format
                    .split(':')
                    .filter(|s| !s.is_empty())
                    .collect();
                let mut sorted_pairs = tag_pairs.clone();
                sorted_pairs.sort_by(|a, b| a.0.cmp(b.0));

                for (j, (key, value)) in sorted_pairs.iter().enumerate() {
                    if j < tag_parts.len() {
                        assert!(
                            tag_parts[j].starts_with(&format!("{}={}", key, value)),
                            "Test case {}, tag {}: expected '{}={}' in position {}, got '{}'",
                            i,
                            j,
                            key,
                            value,
                            j,
                            tag_parts[j]
                        );
                    }
                }
            }
        }

        println!("Test case {}: {:?} -> '{}'", i, tag_pairs, cassandra_format);
    }
}

#[test]
fn test_value_serialization_format() {
    let test_cases = vec![
        ("double_positive", DataPointValue::Double(42.5.into())),
        ("double_negative", DataPointValue::Double((-123.456).into())),
        ("double_zero", DataPointValue::Double(0.0.into())),
        ("long_positive", DataPointValue::Long(123456789)),
        ("long_negative", DataPointValue::Long(-987654321)),
        ("long_zero", DataPointValue::Long(0)),
        ("long_max", DataPointValue::Long(i64::MAX)),
        ("long_min", DataPointValue::Long(i64::MIN)),
        ("text_simple", DataPointValue::Text("hello".to_string())),
        (
            "binary_data",
            DataPointValue::Binary(vec![0x00, 0xFF, 0x42]),
        ),
    ];

    for (name, value) in test_cases {
        let cassandra_value = CassandraValue::from_data_point_value(&value, None);

        match &value {
            DataPointValue::Double(_) => {
                // Doubles should be exactly 8 bytes (no type flag in new format)
                assert_eq!(
                    cassandra_value.bytes.len(),
                    8,
                    "Double {} should be 8 bytes",
                    name
                );
            }
            DataPointValue::Long(v) => {
                // Longs should use variable-length encoding
                if *v == 0 {
                    assert_eq!(cassandra_value.bytes.len(), 1, "Long zero should be 1 byte");
                    assert_eq!(cassandra_value.bytes[0], 0, "Long zero should be byte 0");
                } else {
                    assert!(
                        cassandra_value.bytes.len() <= 10,
                        "Long {} should be <= 10 bytes (was {} bytes)",
                        name,
                        cassandra_value.bytes.len()
                    );
                    assert!(
                        !cassandra_value.bytes.is_empty(),
                        "Long {} should be > 0 bytes",
                        name
                    );
                    // Should not have leading zeros (except for the value 0)
                    assert_ne!(
                        cassandra_value.bytes[0], 0,
                        "Long {} should not have leading zero",
                        name
                    );
                }
            }
            DataPointValue::Text(s) => {
                assert_eq!(
                    cassandra_value.bytes.len(),
                    s.len(),
                    "Text {} length mismatch",
                    name
                );
                assert_eq!(
                    cassandra_value.bytes,
                    s.as_bytes(),
                    "Text {} content mismatch",
                    name
                );
            }
            DataPointValue::Binary(b) => {
                assert_eq!(
                    cassandra_value.bytes, *b,
                    "Binary {} content mismatch",
                    name
                );
            }
            _ => {}
        }

        println!("Value {}: {} bytes", name, cassandra_value.bytes.len());
    }
}

#[test]
fn test_row_time_bucketing() {
    // Test that row time calculation is consistent
    let test_timestamps = vec![
        1755538800000, // Base timestamp
        1755538800001, // 1ms later
        1755538900000, // 100 seconds later
        1755540000000, // Different bucket
        1756000000000, // Much later
    ];

    let mut previous_row_time = None;

    for ts_millis in test_timestamps {
        let timestamp = Timestamp::from_millis(ts_millis).unwrap();
        let row_time = timestamp.row_time();

        // Row time should be on 3-week boundaries
        let three_weeks_ms = 3 * 7 * 24 * 60 * 60 * 1000;
        assert_eq!(
            row_time.timestamp_millis() % three_weeks_ms,
            0,
            "Row time {} is not on 3-week boundary",
            row_time.timestamp_millis()
        );

        // Row time should be <= original timestamp
        assert!(
            row_time.timestamp_millis() <= ts_millis,
            "Row time {} should be <= timestamp {}",
            row_time.timestamp_millis(),
            ts_millis
        );

        println!(
            "Timestamp {} -> Row time {}",
            ts_millis,
            row_time.timestamp_millis()
        );

        // Check monotonicity (row times should be non-decreasing)
        if let Some(prev) = previous_row_time {
            assert!(
                row_time.timestamp_millis() >= prev,
                "Row time should be non-decreasing: {} >= {}",
                row_time.timestamp_millis(),
                prev
            );
        }
        previous_row_time = Some(row_time.timestamp_millis());
    }
}

#[test]
fn test_data_type_assignment() {
    // Test that data types are assigned correctly
    let test_cases = vec![
        (DataPointValue::Double(1.0.into()), "kairos_double"),
        (DataPointValue::Long(1), "kairos_long"),
        (DataPointValue::Text("test".to_string()), "kairos_string"),
        (DataPointValue::Binary(vec![1, 2, 3]), "kairos_binary"),
    ];

    for (value, expected_type) in test_cases {
        let data_point = match &value {
            DataPointValue::Double(d) => {
                DataPoint::new_double("test", Timestamp::now(), d.into_inner())
            }
            DataPointValue::Long(l) => DataPoint::new_long("test", Timestamp::now(), *l),
            DataPointValue::Text(s) => {
                // Create using the available constructor pattern
                let mut dp = DataPoint::new_long("test", Timestamp::now(), 0);
                dp.value = DataPointValue::Text(s.clone());
                dp
            }
            DataPointValue::Binary(b) => {
                // Create using the available constructor pattern
                let mut dp = DataPoint::new_long("test", Timestamp::now(), 0);
                dp.value = DataPointValue::Binary(b.clone());
                dp
            }
            _ => continue,
        };

        let actual_type = data_point.data_type();
        assert_eq!(
            actual_type, expected_type,
            "Data type mismatch for {:?}",
            value
        );

        println!("Value type: {:?} -> {}", value, actual_type);
    }
}

#[test]
fn test_round_trip_value_serialization() {
    // Test that values can be serialized and deserialized correctly
    let test_values = vec![
        DataPointValue::Double(42.5.into()),
        DataPointValue::Double((-123.456).into()),
        DataPointValue::Long(123456789),
        DataPointValue::Long(-987654321),
        DataPointValue::Long(0),
        DataPointValue::Text("hello world".to_string()),
        DataPointValue::Binary(vec![0x00, 0xFF, 0x42, 0xAA]),
    ];

    for original_value in test_values {
        let data_type = match &original_value {
            DataPointValue::Double(_) => "kairos_double",
            DataPointValue::Long(_) => "kairos_long",
            DataPointValue::Text(_) => "kairos_string",
            DataPointValue::Binary(_) => "kairos_binary",
            _ => continue,
        };

        // Serialize
        let cassandra_value = CassandraValue::from_data_point_value(&original_value, None);

        // Deserialize
        let round_trip_result = cassandra_value.to_data_point_value(data_type);
        assert!(
            round_trip_result.is_ok(),
            "Failed to deserialize {:?}: {:?}",
            original_value,
            round_trip_result.err()
        );

        let round_trip_value = round_trip_result.unwrap();

        // Compare
        match (&original_value, &round_trip_value) {
            (DataPointValue::Double(orig), DataPointValue::Double(rt)) => {
                assert!(
                    (orig.into_inner() - rt.into_inner()).abs() < f64::EPSILON,
                    "Double round-trip failed: {} != {}",
                    orig.into_inner(),
                    rt.into_inner()
                );
            }
            (DataPointValue::Long(orig), DataPointValue::Long(rt)) => {
                assert_eq!(orig, rt, "Long round-trip failed: {} != {}", orig, rt);
            }
            (DataPointValue::Text(orig), DataPointValue::Text(rt)) => {
                assert_eq!(orig, rt, "Text round-trip failed: '{}' != '{}'", orig, rt);
            }
            (DataPointValue::Binary(orig), DataPointValue::Binary(rt)) => {
                assert_eq!(orig, rt, "Binary round-trip failed");
            }
            _ => panic!(
                "Type mismatch in round-trip: {:?} vs {:?}",
                original_value, round_trip_value
            ),
        }

        println!("Round-trip successful for: {:?}", original_value);
    }
}
