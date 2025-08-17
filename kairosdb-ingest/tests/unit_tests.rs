//! Comprehensive unit tests for KairosDB ingestion service
//!
//! This module contains unit tests for all core components of the ingestion service.

use kairosdb_core::{
    datapoint::{DataPoint, DataPointValue}, 
    metrics::MetricName,
    time::Timestamp,
};
use kairosdb_ingest::{
    config::{IngestConfig, CassandraConfig, IngestionConfig, MetricsConfig},
    json_parser::{JsonParser, ErrorResponse, IngestResponse, ValidationErrors},
};
use serde_json::json;
use std::collections::HashMap;

#[cfg(test)]
mod json_parser_tests {
    use super::*;

    #[test]
    fn test_parse_single_metric_long_value() {
        let parser = JsonParser::new(1000, true);
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
        let parser = JsonParser::new(1000, true);
        let json = json!({
            "name": "cpu.usage",
            "datapoints": [[1634567890000i64, 75.5]],
            "tags": {"host": "server1", "cpu": "0"}
        }).to_string();
        
        let (batch, warnings) = parser.parse_json(&json).unwrap();
        assert_eq!(batch.len(), 1);
        assert!(warnings.is_empty());
        
        let point = &batch.points[0];
        assert_eq!(point.metric.as_str(), "cpu.usage");
        match &point.value {
            DataPointValue::Double(val) => assert_eq!(val.into_inner(), 75.5),
            _ => panic!("Expected double value"),
        }
        assert_eq!(point.tags.len(), 2);
    }

    #[test]
    fn test_parse_multiple_datapoints() {
        let parser = JsonParser::new(1000, true);
        let json = json!({
            "name": "memory.usage",
            "datapoints": [
                [1634567890000i64, 1024],
                [1634567891000i64, 1048],
                [1634567892000i64, 1072]
            ],
            "tags": {"host": "server1"}
        }).to_string();
        
        let (batch, warnings) = parser.parse_json(&json).unwrap();
        assert_eq!(batch.len(), 3);
        assert!(warnings.is_empty());
        
        for (i, point) in batch.points.iter().enumerate() {
            assert_eq!(point.metric.as_str(), "memory.usage");
            assert_eq!(point.tags.get("host").unwrap(), "server1");
            match &point.value {
                DataPointValue::Long(val) => assert_eq!(*val, 1024 + (i as i64 * 24)),
                _ => panic!("Expected long value"),
            }
        }
    }

    #[test]
    fn test_parse_array_of_metrics() {
        let parser = JsonParser::new(1000, true);
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
        
        assert_eq!(batch.points[0].metric.as_str(), "metric1");
        assert_eq!(batch.points[1].metric.as_str(), "metric2");
    }

    #[test]
    fn test_parse_timestamp_seconds() {
        let parser = JsonParser::new(1000, true);
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
        let parser = JsonParser::new(1000, true);
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
        let parser = JsonParser::new(1000, true);
        let json = json!({
            "name": "test.metric",
            "datapoints": [[1634567890000i64, "42.5"]],
            "tags": {}
        }).to_string();
        
        let (batch, _) = parser.parse_json(&json).unwrap();
        let point = &batch.points[0];
        match &point.value {
            DataPointValue::Double(val) => assert_eq!(val.into_inner(), 42.5),
            _ => panic!("Expected double value"),
        }
    }

    #[test]
    fn test_parse_text_value() {
        let parser = JsonParser::new(1000, true);
        let json = json!({
            "name": "status.message",
            "datapoints": [[1634567890000i64, "system operational"]],
            "tags": {"level": "info"}
        }).to_string();
        
        let (batch, _) = parser.parse_json(&json).unwrap();
        let point = &batch.points[0];
        match &point.value {
            DataPointValue::Text(text) => assert_eq!(text, "system operational"),
            _ => panic!("Expected text value"),
        }
    }

    #[test]
    fn test_validation_empty_metric_name() {
        let parser = JsonParser::new(1000, true);
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
        let parser = JsonParser::new(1000, true);
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
    fn test_ttl_parsing() {
        let parser = JsonParser::new(1000, true);
        let json = json!({
            "name": "temp.metric",
            "datapoints": [[1634567890000i64, 25.5]],
            "tags": {"location": "room1"},
            "ttl": 3600
        }).to_string();
        
        let (batch, _) = parser.parse_json(&json).unwrap();
        let point = &batch.points[0];
        assert_eq!(point.ttl, 3600);
    }

    #[test]
    fn test_invalid_json() {
        let parser = JsonParser::new(1000, true);
        let invalid_json = r#"{"name": "test", "datapoints"#; // Incomplete JSON
        
        let result = parser.parse_json(invalid_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_complex_tags() {
        let parser = JsonParser::new(1000, true);
        let json = json!({
            "name": "network.latency",
            "datapoints": [[1634567890000i64, 15.2]],
            "tags": {
                "source": "web-server-01",
                "destination": "database-primary",
                "protocol": "tcp",
                "port": "5432"
            }
        }).to_string();
        
        let (batch, _) = parser.parse_json(&json).unwrap();
        let point = &batch.points[0];
        assert_eq!(point.tags.len(), 4);
        assert_eq!(point.tags.get("source").unwrap(), "web-server-01");
        assert_eq!(point.tags.get("destination").unwrap(), "database-primary");
        assert_eq!(point.tags.get("protocol").unwrap(), "tcp");
        assert_eq!(point.tags.get("port").unwrap(), "5432");
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;
    use std::env;

    #[test]
    fn test_default_config() {
        let config = IngestConfig::default();
        assert_eq!(config.bind_address, "0.0.0.0:8080");
        assert_eq!(config.cassandra.keyspace, "kairosdb");
        assert_eq!(config.ingestion.max_batch_size, 10000);
        assert!(config.ingestion.enable_validation);
        assert!(config.metrics.enable_prometheus);
    }

    #[test]
    fn test_config_validation_valid() {
        let config = IngestConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_empty_contact_points() {
        let mut config = IngestConfig::default();
        config.cassandra.contact_points.clear();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_empty_keyspace() {
        let mut config = IngestConfig::default();
        config.cassandra.keyspace.clear();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_zero_batch_size() {
        let mut config = IngestConfig::default();
        config.ingestion.max_batch_size = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_timeout_methods() {
        let config = IngestConfig::default();
        assert_eq!(config.batch_timeout().as_millis(), 1000);
        assert_eq!(config.connection_timeout().as_millis(), 5000);
        assert_eq!(config.query_timeout().as_millis(), 10000);
    }

    #[test]
    fn test_config_memory_bytes() {
        let config = IngestConfig::default();
        let expected_bytes = 1024 * 1024 * 1024; // 1GB
        assert_eq!(config.max_memory_bytes(), expected_bytes);
    }

    #[test]
    fn test_config_flags() {
        let config = IngestConfig::default();
        assert!(config.is_prometheus_enabled());
        assert!(config.is_compression_enabled());
    }
}

#[cfg(test)]
mod error_response_tests {
    use super::*;
    use kairosdb_core::error::KairosError;

    #[test]
    fn test_error_response_from_string() {
        let error_response = ErrorResponse::from_error("Test error message");
        assert_eq!(error_response.errors.len(), 1);
        assert_eq!(error_response.errors[0], "Test error message");
    }

    #[test]
    fn test_error_response_from_multiple_errors() {
        let errors = vec!["Error 1", "Error 2", "Error 3"];
        let error_response = ErrorResponse::from_errors(errors);
        assert_eq!(error_response.errors.len(), 3);
        assert_eq!(error_response.errors[0], "Error 1");
        assert_eq!(error_response.errors[2], "Error 3");
    }

    #[test]
    fn test_error_response_from_kairos_error() {
        let kairos_error = KairosError::validation("Invalid input");
        let error_response = ErrorResponse::from_kairos_error(&kairos_error);
        assert_eq!(error_response.errors.len(), 1);
        assert!(error_response.errors[0].contains("Invalid input"));
    }
}

#[cfg(test)]
mod validation_tests {
    use super::*;

    #[test]
    fn test_validation_errors_empty() {
        let errors = ValidationErrors::new();
        assert!(!errors.has_errors());
        assert!(errors.into_result().is_ok());
    }

    #[test]
    fn test_validation_errors_with_errors() {
        let mut errors = ValidationErrors::new();
        errors.add_error("First error");
        errors.add_error("Second error");
        
        assert!(errors.has_errors());
        assert_eq!(errors.errors.len(), 2);
        
        let result = errors.into_result();
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod ingest_response_tests {
    use super::*;

    #[test]
    fn test_ingest_response_creation() {
        let response = IngestResponse {
            datapoints_ingested: 100,
            ingest_time: 250,
            warnings: vec!["Warning message".to_string()],
        };
        
        assert_eq!(response.datapoints_ingested, 100);
        assert_eq!(response.ingest_time, 250);
        assert_eq!(response.warnings.len(), 1);
    }

    #[test]
    fn test_ingest_response_serialization() {
        let response = IngestResponse {
            datapoints_ingested: 50,
            ingest_time: 100,
            warnings: vec![],
        };
        
        let json_str = serde_json::to_string(&response).unwrap();
        assert!(json_str.contains("\"datapoints_ingested\":50"));
        assert!(json_str.contains("\"ingest_time\":100"));
    }
}

#[cfg(test)]
mod integration_helpers {
    use super::*;

    /// Helper function to create test data points
    pub fn create_test_datapoints(count: usize) -> Vec<DataPoint> {
        let mut points = Vec::new();
        let base_time = 1634567890000i64;
        
        for i in 0..count {
            let point = DataPoint::new_long(
                format!("test.metric.{}", i),
                Timestamp::from_millis(base_time + (i as i64 * 1000)).unwrap(),
                i as i64,
            ).with_tag("index", &i.to_string()).unwrap()
             .with_tag("test", "true").unwrap();
            
            points.push(point);
        }
        
        points
    }

    /// Helper function to create test JSON payload
    pub fn create_test_json_payload(metric_name: &str, value: i64, tags: HashMap<String, String>) -> String {
        json!({
            "name": metric_name,
            "datapoints": [[1634567890000i64, value]],
            "tags": tags
        }).to_string()
    }

    #[test]
    fn test_create_test_datapoints() {
        let points = create_test_datapoints(5);
        assert_eq!(points.len(), 5);
        
        for (i, point) in points.iter().enumerate() {
            assert_eq!(point.metric.as_str(), format!("test.metric.{}", i));
            assert_eq!(point.tags.get("index").unwrap(), i.to_string());
            assert_eq!(point.tags.get("test").unwrap(), "true");
        }
    }

    #[test] 
    fn test_create_test_json_payload() {
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());
        
        let json_payload = create_test_json_payload("cpu.usage", 75, tags);
        assert!(json_payload.contains("cpu.usage"));
        assert!(json_payload.contains("75"));
        assert!(json_payload.contains("server1"));
    }
}