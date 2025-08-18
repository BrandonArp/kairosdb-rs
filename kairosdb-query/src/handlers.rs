use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
};
use kairosdb_core::query::{MetricNamesQuery, QueryRequest, TagNamesQuery, TagValuesQuery};
use serde_json::{json, Value};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

use crate::AppState;

/// Health check endpoint
pub async fn health_handler() -> Result<Json<Value>, StatusCode> {
    Ok(Json(json!({
        "status": "healthy",
        "service": "kairosdb-query",
        "timestamp": chrono::Utc::now().to_rfc3339()
    })))
}

/// Metrics endpoint (Prometheus format)
pub async fn metrics_handler(State(state): State<AppState>) -> Result<String, StatusCode> {
    let metrics = state.query_engine.get_metrics().await;

    Ok(format!(
        "# HELP kairosdb_queries_total Total number of queries executed\n\
         # TYPE kairosdb_queries_total counter\n\
         kairosdb_queries_total {}\n\
         # HELP kairosdb_query_errors_total Total number of query errors\n\
         # TYPE kairosdb_query_errors_total counter\n\
         kairosdb_query_errors_total {}\n\
         # HELP kairosdb_datapoints_returned_total Total number of data points returned\n\
         # TYPE kairosdb_datapoints_returned_total counter\n\
         kairosdb_datapoints_returned_total {}\n\
         # HELP kairosdb_avg_query_time_ms Average query execution time in milliseconds\n\
         # TYPE kairosdb_avg_query_time_ms gauge\n\
         kairosdb_avg_query_time_ms {}\n",
        metrics.queries_executed,
        metrics.query_errors,
        metrics.datapoints_returned,
        metrics.avg_query_time_ms
    ))
}

/// Main query endpoint
pub async fn query_handler(
    State(state): State<AppState>,
    Json(payload): Json<QueryRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    debug!("Received query request: {:?}", payload);

    // Validate the query request
    if let Err(err) = payload.validate_self() {
        warn!("Query validation failed: {}", err);
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Invalid query",
                "message": err.to_string()
            })),
        ));
    }

    // Execute the query
    match state.query_engine.execute_query(payload).await {
        Ok(response) => {
            info!(
                "Query executed successfully, returned {} metrics",
                response.queries.len()
            );
            Ok(Json(serde_json::to_value(response).unwrap()))
        }
        Err(err) => {
            error!("Query execution failed: {}", err);
            let status_code = match err.category() {
                "validation" => StatusCode::BAD_REQUEST,
                "timeout" => StatusCode::REQUEST_TIMEOUT,
                "rate_limit" => StatusCode::TOO_MANY_REQUESTS,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };

            Err((
                status_code,
                Json(json!({
                    "error": "Query execution failed",
                    "message": err.to_string(),
                    "category": err.category()
                })),
            ))
        }
    }
}

/// Metric names discovery endpoint
pub async fn metric_names_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    debug!("Received metric names query: {:?}", params);

    let query = MetricNamesQuery {
        prefix: params.get("prefix").cloned(),
        limit: params.get("limit").and_then(|l| l.parse().ok()),
    };

    match state.query_engine.get_metric_names(query).await {
        Ok(response) => {
            info!("Returned {} metric names", response.results.len());
            Ok(Json(serde_json::to_value(response).unwrap()))
        }
        Err(err) => {
            error!("Failed to get metric names: {}", err);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "Failed to get metric names",
                    "message": err.to_string()
                })),
            ))
        }
    }
}

/// Tag names discovery endpoint
pub async fn tag_names_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    debug!("Received tag names query: {:?}", params);

    let metric = params
        .get("metric")
        .map(|m| kairosdb_core::metrics::MetricName::new(m))
        .transpose()
        .map_err(|err| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "Invalid metric name",
                    "message": err.to_string()
                })),
            )
        })?;

    let query = TagNamesQuery {
        metric,
        limit: params.get("limit").and_then(|l| l.parse().ok()),
    };

    match state.query_engine.get_tag_names(query).await {
        Ok(response) => {
            info!("Returned {} tag names", response.results.len());
            Ok(Json(serde_json::to_value(response).unwrap()))
        }
        Err(err) => {
            error!("Failed to get tag names: {}", err);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "Failed to get tag names",
                    "message": err.to_string()
                })),
            ))
        }
    }
}

/// Tag values discovery endpoint
pub async fn tag_values_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    debug!("Received tag values query: {:?}", params);

    let metric = params.get("metric").ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Missing required parameter 'metric'"
            })),
        )
    })?;

    let tag_name = params.get("tag").ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Missing required parameter 'tag'"
            })),
        )
    })?;

    let metric_name = kairosdb_core::metrics::MetricName::new(metric).map_err(|err| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Invalid metric name",
                "message": err.to_string()
            })),
        )
    })?;

    let query = TagValuesQuery {
        metric: metric_name,
        tag_name: tag_name.clone(),
        prefix: params.get("prefix").cloned(),
        limit: params.get("limit").and_then(|l| l.parse().ok()),
    };

    match state.query_engine.get_tag_values(query).await {
        Ok(response) => {
            info!(
                "Returned {} tag values for tag '{}'",
                response.results.len(),
                tag_name
            );
            Ok(Json(serde_json::to_value(response).unwrap()))
        }
        Err(err) => {
            error!("Failed to get tag values: {}", err);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": "Failed to get tag values",
                    "message": err.to_string()
                })),
            ))
        }
    }
}
