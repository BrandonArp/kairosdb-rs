use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
};
use kairosdb_core::{
    datastore::{TagFilter, TimeSeriesStore},
    metrics::MetricName,
    query::{
        DataPointResult, MetricNamesQuery, MetricNamesResponse, MetricQueryResult, QueryRequest,
        QueryResponse, QueryResult, TagNamesQuery, TagNamesResponse, TagValuesQuery,
        TagValuesResponse,
    },
    time::{TimeRange, Timestamp},
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
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
pub async fn metrics_handler(State(_state): State<AppState>) -> Result<String, StatusCode> {
    // For now, return basic metrics - in a real implementation, we'd track these
    Ok("# HELP kairosdb_queries_total Total number of queries executed\n\
         # TYPE kairosdb_queries_total counter\n\
         kairosdb_queries_total 0\n\
         # HELP kairosdb_query_errors_total Total number of query errors\n\
         # TYPE kairosdb_query_errors_total counter\n\
         kairosdb_query_errors_total 0\n\
         # HELP kairosdb_datapoints_returned_total Total number of data points returned\n\
         # TYPE kairosdb_datapoints_returned_total counter\n\
         kairosdb_datapoints_returned_total 0\n\
         # HELP kairosdb_avg_query_time_ms Average query execution time in milliseconds\n\
         # TYPE kairosdb_avg_query_time_ms gauge\n\
         kairosdb_avg_query_time_ms 0.0\n".to_string())
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

    // Execute the query using the datastore
    match execute_query_with_datastore(&state.datastore, payload).await {
        Ok(response) => {
            info!(
                "Query executed successfully, returned {} metrics",
                response.queries.len()
            );
            Ok(Json(serde_json::to_value(response).unwrap()))
        }
        Err(err) => {
            error!("Query execution failed: {}", err);
            let status_code = if err.to_string().contains("validation") {
                StatusCode::BAD_REQUEST
            } else if err.to_string().contains("timeout") {
                StatusCode::REQUEST_TIMEOUT
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };

            Err((
                status_code,
                Json(json!({
                    "error": "Query execution failed",
                    "message": err.to_string()
                })),
            ))
        }
    }
}

/// Execute a KairosDB query using the datastore abstraction
async fn execute_query_with_datastore(
    datastore: &Arc<dyn TimeSeriesStore>,
    query_request: QueryRequest,
) -> Result<QueryResponse, Box<dyn std::error::Error + Send + Sync>> {
    let mut metric_query_results = Vec::new();
    let mut total_sample_size = 0;

    for metric_query in query_request.metrics {
        let metric = MetricName::from(metric_query.name.as_str());

        // Convert KairosDB time range to our TimeRange
        let time_range = if let (Some(start), Some(end)) =
            (query_request.start_absolute, query_request.end_absolute)
        {
            TimeRange::new(start, end)?
        } else {
            // Default to last hour if no time range specified
            let now = Timestamp::now();
            let hour_ago = now.sub_millis(3600 * 1000)?;
            TimeRange::new(hour_ago, now)?
        };

        // Convert tags to TagFilter
        let tag_filter = if metric_query.tags.is_empty() {
            TagFilter::All
        } else {
            let tag_map: BTreeMap<String, String> = metric_query.tags.into_iter().collect();
            TagFilter::Exact(tag_map)
        };

        // Query the datastore
        let data_points = datastore
            .query_points(&metric, &tag_filter, time_range)
            .await?;

        // Convert data points to KairosDB format
        let mut data_point_results = Vec::new();
        for point in &data_points {
            data_point_results.push(DataPointResult {
                timestamp: point.timestamp,
                value: point.value.clone(),
            });
        }

        // Create tag map for response
        let tags = HashMap::new();
        if !data_points.is_empty() {
            // Use tags from first data point (simplified)
            // For now just leave tags empty since TagSet access is private
            // In a real implementation we'd extract the tags properly
        }

        let query_result = QueryResult {
            name: metric.clone(),
            tags,
            group_by: Vec::new(), // Simplified - no grouping for now
            values: data_point_results,
        };

        let sample_size = query_result.values.len();
        total_sample_size += sample_size;

        metric_query_results.push(MetricQueryResult {
            sample_size,
            results: vec![query_result],
        });
    }

    Ok(QueryResponse {
        queries: metric_query_results,
        sample_size: total_sample_size,
    })
}

/// Metric names discovery endpoint
pub async fn metric_names_handler(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    debug!("Received metric names query: {:?}", params);

    let _query = MetricNamesQuery {
        prefix: params.get("prefix").cloned(),
        limit: params.get("limit").and_then(|l| l.parse().ok()),
    };

    // Use the datastore to get metric names
    match state.datastore.list_metrics(None).await {
        Ok(metrics) => {
            // Convert to KairosDB response format
            let mut results: Vec<String> = metrics.iter().map(|m| m.as_str().to_string()).collect();

            // Apply prefix filter if specified
            if let Some(prefix) = &_query.prefix {
                results.retain(|name| name.starts_with(prefix));
            }

            // Apply limit if specified
            if let Some(limit) = _query.limit {
                results.truncate(limit);
            }

            let response = MetricNamesResponse { results };
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
        .map(kairosdb_core::metrics::MetricName::new)
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
        metric: metric.clone(),
        limit: params.get("limit").and_then(|l| l.parse().ok()),
    };

    // Use datastore to get tag names
    let result = if let Some(metric_name) = metric {
        // Get tags for specific metric
        state.datastore.list_tags(&metric_name, None).await
    } else {
        // If no metric specified, we'd need to aggregate across all metrics
        // For now, return empty result
        Ok(kairosdb_core::datastore::TagSet::new())
    };

    match result {
        Ok(tag_set) => {
            // Convert to KairosDB response format
            let mut results: Vec<String> = tag_set.keys().cloned().collect();

            // Apply limit if specified
            if let Some(limit) = query.limit {
                results.truncate(limit);
            }

            let response = TagNamesResponse { results };
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
        metric: metric_name.clone(),
        tag_name: tag_name.clone(),
        prefix: params.get("prefix").cloned(),
        limit: params.get("limit").and_then(|l| l.parse().ok()),
    };

    // Use the datastore to get tag values for the specific metric and tag
    match state.datastore.list_tags(&metric_name, None).await {
        Ok(tag_set) => {
            // Extract values for the specific tag name
            let mut results: Vec<String> = if let Some(tag_values) = tag_set.tags.get(tag_name) {
                tag_values.iter().map(|tv| tv.value.clone()).collect()
            } else {
                vec![]
            };

            // Apply prefix filter if specified
            if let Some(prefix) = query.prefix {
                results.retain(|value| value.starts_with(&prefix));
            }

            // Apply limit if specified
            if let Some(limit) = query.limit {
                results.truncate(limit);
            }

            let response = TagValuesResponse { results };
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
