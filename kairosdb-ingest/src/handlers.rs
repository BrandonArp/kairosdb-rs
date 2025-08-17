use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
};
use kairosdb_core::{
    datapoint::{DataPoint, DataPointBatch},
    error::KairosError,
};
use serde_json::{json, Value};
use tracing::{info, warn, error, debug};

use crate::AppState;

/// Health check endpoint
pub async fn health_handler() -> Result<Json<Value>, StatusCode> {
    Ok(Json(json!({
        "status": "healthy",
        "service": "kairosdb-ingest",
        "timestamp": chrono::Utc::now().to_rfc3339()
    })))
}

/// Metrics endpoint (Prometheus format)
pub async fn metrics_handler(State(state): State<AppState>) -> Result<String, StatusCode> {
    // In a real implementation, this would return Prometheus metrics
    // For now, return basic metrics
    let metrics = state.ingestion_service.get_metrics().await;
    
    Ok(format!(
        "# HELP kairosdb_ingested_datapoints_total Total number of ingested data points\n\
         # TYPE kairosdb_ingested_datapoints_total counter\n\
         kairosdb_ingested_datapoints_total {}\n\
         # HELP kairosdb_ingestion_errors_total Total number of ingestion errors\n\
         # TYPE kairosdb_ingestion_errors_total counter\n\
         kairosdb_ingestion_errors_total {}\n\
         # HELP kairosdb_batches_processed_total Total number of batches processed\n\
         # TYPE kairosdb_batches_processed_total counter\n\
         kairosdb_batches_processed_total {}\n",
        metrics.datapoints_ingested,
        metrics.ingestion_errors,
        metrics.batches_processed
    ))
}

/// Main data ingestion endpoint
pub async fn ingest_handler(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    debug!("Received ingestion request: {:?}", payload);
    
    // Parse the payload into data points
    let batch = match parse_ingestion_payload(payload) {
        Ok(batch) => batch,
        Err(err) => {
            warn!("Failed to parse ingestion payload: {}", err);
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "Invalid payload format",
                    "message": err.to_string()
                }))
            ));
        }
    };
    
    // Validate the batch if validation is enabled
    if state.config.ingestion.enable_validation {
        if let Err(err) = batch.validate_self() {
            warn!("Batch validation failed: {}", err);
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "Validation failed",
                    "message": err.to_string()
                }))
            ));
        }
    }
    
    // Submit batch for ingestion
    match state.ingestion_service.ingest_batch(batch).await {
        Ok(_) => {
            info!("Successfully ingested batch");
            Ok(Json(json!({
                "status": "success",
                "message": "Data points ingested successfully"
            })))
        }
        Err(err) => {
            error!("Failed to ingest batch: {}", err);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "Ingestion failed",
                    "message": err.to_string()
                }))
            ))
        }
    }
}

/// Parse various ingestion payload formats
fn parse_ingestion_payload(payload: Value) -> Result<DataPointBatch, KairosError> {
    // Handle array of data points
    if let Some(array) = payload.as_array() {
        let mut points = Vec::new();
        
        for item in array {
            let point = parse_data_point(item)?;
            points.push(point);
        }
        
        return DataPointBatch::from_points(points);
    }
    
    // Handle single data point
    if let Ok(point) = parse_data_point(&payload) {
        return DataPointBatch::from_points(vec![point]);
    }
    
    // Handle KairosDB format with metrics array
    if let Some(metrics) = payload.get("metrics").and_then(|m| m.as_array()) {
        let mut points = Vec::new();
        
        for metric in metrics {
            let metric_points = parse_kairos_metric(metric)?;
            points.extend(metric_points);
        }
        
        return DataPointBatch::from_points(points);
    }
    
    Err(KairosError::parse("Unsupported payload format"))
}

/// Parse a single data point from JSON
fn parse_data_point(value: &Value) -> Result<DataPoint, KairosError> {
    // Extract required fields
    let name = value.get("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| KairosError::parse("Missing or invalid 'name' field"))?;
    
    let timestamp = value.get("timestamp")
        .and_then(|t| t.as_i64())
        .ok_or_else(|| KairosError::parse("Missing or invalid 'timestamp' field"))?;
    
    let data_value = value.get("value")
        .ok_or_else(|| KairosError::parse("Missing 'value' field"))?;
    
    // Parse the metric name
    let metric = kairosdb_core::metrics::MetricName::new(name)?;
    
    // Parse the timestamp
    let ts = kairosdb_core::time::Timestamp::from_millis(timestamp)?;
    
    // Parse the value
    let point_value = parse_data_point_value(data_value)?;
    
    // Create the base data point
    let mut point = DataPoint {
        metric,
        timestamp: ts,
        value: point_value,
        tags: kairosdb_core::tags::TagSet::new(),
        ttl: 0,
    };
    
    // Parse tags if present
    if let Some(tags) = value.get("tags").and_then(|t| t.as_object()) {
        let mut tag_map = std::collections::HashMap::new();
        for (key, value) in tags {
            if let Some(tag_value) = value.as_str() {
                tag_map.insert(key.clone(), tag_value.to_string());
            }
        }
        point.tags = kairosdb_core::tags::TagSet::from_map(tag_map)?;
    }
    
    // Parse TTL if present
    if let Some(ttl) = value.get("ttl").and_then(|t| t.as_u64()) {
        point.ttl = ttl as u32;
    }
    
    Ok(point)
}

/// Parse a KairosDB metric format (with datapoints array)
fn parse_kairos_metric(metric: &Value) -> Result<Vec<DataPoint>, KairosError> {
    let name = metric.get("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| KairosError::parse("Missing metric name"))?;
    
    let datapoints = metric.get("datapoints")
        .and_then(|dp| dp.as_array())
        .ok_or_else(|| KairosError::parse("Missing or invalid datapoints array"))?;
    
    let mut points = Vec::new();
    
    for dp in datapoints {
        if let Some(dp_array) = dp.as_array() {
            if dp_array.len() >= 2 {
                let timestamp = dp_array[0].as_i64()
                    .ok_or_else(|| KairosError::parse("Invalid timestamp in datapoint"))?;
                
                let value = parse_data_point_value(&dp_array[1])?;
                
                let metric_name = kairosdb_core::metrics::MetricName::new(name)?;
                let ts = kairosdb_core::time::Timestamp::from_millis(timestamp)?;
                
                let mut point = DataPoint {
                    metric: metric_name,
                    timestamp: ts,
                    value,
                    tags: kairosdb_core::tags::TagSet::new(),
                    ttl: 0,
                };
                
                // Parse tags from the metric level
                if let Some(tags) = metric.get("tags").and_then(|t| t.as_object()) {
                    let mut tag_map = std::collections::HashMap::new();
                    for (key, value) in tags {
                        if let Some(tag_value) = value.as_str() {
                            tag_map.insert(key.clone(), tag_value.to_string());
                        }
                    }
                    point.tags = kairosdb_core::tags::TagSet::from_map(tag_map)?;
                }
                
                points.push(point);
            }
        }
    }
    
    Ok(points)
}

/// Parse a data point value from JSON
fn parse_data_point_value(value: &Value) -> Result<kairosdb_core::datapoint::DataPointValue, KairosError> {
    use kairosdb_core::datapoint::DataPointValue;
    
    match value {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(DataPointValue::Long(i))
            } else if let Some(f) = n.as_f64() {
                Ok(DataPointValue::Double(f.into()))
            } else {
                Err(KairosError::parse("Invalid numeric value"))
            }
        }
        Value::String(s) => {
            Ok(DataPointValue::Text(s.clone()))
        }
        Value::Object(obj) => {
            // Check for complex number format
            if let (Some(real), Some(imag)) = (
                obj.get("real").and_then(|v| v.as_f64()),
                obj.get("imaginary").and_then(|v| v.as_f64())
            ) {
                Ok(DataPointValue::Complex { real, imaginary: imag })
            } else {
                Err(KairosError::parse("Unsupported object value format"))
            }
        }
        _ => Err(KairosError::parse("Unsupported value type"))
    }
}