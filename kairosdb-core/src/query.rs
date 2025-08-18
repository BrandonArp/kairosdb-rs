//! Query types and operations for KairosDB

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use validator::Validate;

use crate::datapoint::{DataPoint, DataPointValue};
use crate::error::{KairosError, KairosResult};
use crate::metrics::MetricName;
use crate::tags::TagSet;
use crate::time::{RelativeTime, TimeRange, Timestamp};

/// Query request for time series data
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct QueryRequest {
    /// Start time for the query
    pub start_absolute: Option<Timestamp>,

    /// Start time relative to now
    pub start_relative: Option<RelativeTime>,

    /// End time for the query (optional, defaults to now)
    pub end_absolute: Option<Timestamp>,

    /// End time relative to now
    pub end_relative: Option<RelativeTime>,

    /// Metrics to query
    #[validate(length(min = 1, max = 100))]
    pub metrics: Vec<MetricQuery>,

    /// Maximum number of data points to return
    #[serde(default)]
    pub limit: Option<usize>,

    /// Cache time in seconds
    #[serde(default)]
    pub cache_time: Option<u32>,
}

/// Query for a specific metric
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct MetricQuery {
    /// Name of the metric to query
    pub name: MetricName,

    /// Tags to filter by (all must match)
    #[serde(default)]
    pub tags: HashMap<String, String>,

    /// Tags to group by (creates separate series)
    #[serde(default)]
    pub group_by: Vec<GroupBy>,

    /// Aggregators to apply to the data
    #[serde(default)]
    pub aggregators: Vec<Aggregator>,

    /// Limit for this specific metric
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Group by specification
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "name")]
pub enum GroupBy {
    #[serde(rename = "tag")]
    Tag { tags: Vec<String> },

    #[serde(rename = "time")]
    Time {
        range_size: RelativeTime,
        group_count: Option<usize>,
    },

    #[serde(rename = "value")]
    Value { range_size: f64 },

    #[serde(rename = "type")]
    Type,
}

/// Aggregator specification
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "name")]
pub enum Aggregator {
    #[serde(rename = "avg")]
    Average {
        #[serde(default)]
        sampling: Option<Sampling>,
    },

    #[serde(rename = "sum")]
    Sum {
        #[serde(default)]
        sampling: Option<Sampling>,
    },

    #[serde(rename = "min")]
    Min {
        #[serde(default)]
        sampling: Option<Sampling>,
    },

    #[serde(rename = "max")]
    Max {
        #[serde(default)]
        sampling: Option<Sampling>,
    },

    #[serde(rename = "count")]
    Count {
        #[serde(default)]
        sampling: Option<Sampling>,
    },

    #[serde(rename = "dev")]
    StandardDeviation {
        #[serde(default)]
        sampling: Option<Sampling>,
    },

    #[serde(rename = "percentile")]
    Percentile {
        percentile: f64,
        #[serde(default)]
        sampling: Option<Sampling>,
    },

    #[serde(rename = "rate")]
    Rate { unit: String },

    #[serde(rename = "sampler")]
    Sampler { unit: String },
}

/// Sampling specification for aggregators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sampling {
    /// Sampling value (e.g., number or time amount)
    pub value: i64,

    /// Sampling unit
    pub unit: String,
}

/// Query response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    /// Queried metrics and their results
    pub queries: Vec<MetricQueryResult>,

    /// Sample count across all metrics
    pub sample_size: usize,
}

/// Result for a single metric query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricQueryResult {
    /// Sample count for this metric
    pub sample_size: usize,

    /// The queried results grouped by tags/time/etc
    pub results: Vec<QueryResult>,
}

/// A single query result (one time series)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Name of the metric
    pub name: MetricName,

    /// Tags for this time series
    pub tags: HashMap<String, String>,

    /// Group by values if grouping was applied
    #[serde(default)]
    pub group_by: Vec<GroupByResult>,

    /// Data points in this time series
    pub values: Vec<DataPointResult>,
}

/// Group by result value
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "name")]
pub enum GroupByResult {
    #[serde(rename = "tag")]
    Tag { group: HashMap<String, String> },

    #[serde(rename = "time")]
    Time {
        range_start: Timestamp,
        range_end: Timestamp,
    },

    #[serde(rename = "value")]
    Value { range_start: f64, range_end: f64 },

    #[serde(rename = "type")]
    Type {
        #[serde(rename = "type")]
        data_type: String,
    },
}

/// Data point in query result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPointResult {
    /// Timestamp
    pub timestamp: Timestamp,

    /// Value
    pub value: DataPointValue,
}

impl QueryRequest {
    /// Get the effective time range for this query
    pub fn time_range(&self) -> KairosResult<TimeRange> {
        let now = Timestamp::now();

        // Determine start time
        let start = if let Some(start_abs) = self.start_absolute {
            start_abs
        } else if let Some(ref start_rel) = self.start_relative {
            start_rel.before(now)?
        } else {
            return Err(KairosError::validation("Must specify start time"));
        };

        // Determine end time
        let end = if let Some(end_abs) = self.end_absolute {
            end_abs
        } else if let Some(ref end_rel) = self.end_relative {
            end_rel.before(now)?
        } else {
            now
        };

        TimeRange::new(start, end)
    }

    /// Validate the query request
    pub fn validate_self(&self) -> KairosResult<()> {
        self.validate()
            .map_err(|e| KairosError::validation(format!("Invalid query: {}", e)))?;

        // Check that we have either absolute or relative start time
        if self.start_absolute.is_none() && self.start_relative.is_none() {
            return Err(KairosError::validation("Must specify start time"));
        }

        // Validate time range
        let _range = self.time_range()?;

        // Validate each metric query
        for metric_query in &self.metrics {
            metric_query.validate_self()?;
        }

        Ok(())
    }
}

impl MetricQuery {
    /// Validate the metric query
    pub fn validate_self(&self) -> KairosResult<()> {
        self.validate()
            .map_err(|e| KairosError::validation(format!("Invalid metric query: {}", e)))?;

        // Validate aggregators
        for aggregator in &self.aggregators {
            aggregator.validate_self()?;
        }

        Ok(())
    }
}

impl Aggregator {
    /// Validate the aggregator
    pub fn validate_self(&self) -> KairosResult<()> {
        match self {
            Aggregator::Percentile { percentile, .. } => {
                if *percentile < 0.0 || *percentile > 100.0 {
                    return Err(KairosError::validation(
                        "Percentile must be between 0 and 100",
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Get the aggregator name
    pub fn name(&self) -> &'static str {
        match self {
            Aggregator::Average { .. } => "avg",
            Aggregator::Sum { .. } => "sum",
            Aggregator::Min { .. } => "min",
            Aggregator::Max { .. } => "max",
            Aggregator::Count { .. } => "count",
            Aggregator::StandardDeviation { .. } => "dev",
            Aggregator::Percentile { .. } => "percentile",
            Aggregator::Rate { .. } => "rate",
            Aggregator::Sampler { .. } => "sampler",
        }
    }
}

impl From<DataPoint> for DataPointResult {
    fn from(point: DataPoint) -> Self {
        Self {
            timestamp: point.timestamp,
            value: point.value,
        }
    }
}

/// Metric names query for discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricNamesQuery {
    /// Optional prefix to filter by
    #[serde(default)]
    pub prefix: Option<String>,

    /// Maximum number of metric names to return
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Response for metric names query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricNamesResponse {
    /// List of metric names
    pub results: Vec<String>,
}

/// Tag names query for discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagNamesQuery {
    /// Optional metric name to scope tags to
    #[serde(default)]
    pub metric: Option<MetricName>,

    /// Maximum number of tag names to return
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Response for tag names query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagNamesResponse {
    /// List of tag names
    pub results: Vec<String>,
}

/// Tag values query for discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagValuesQuery {
    /// Metric name to get tag values for
    pub metric: MetricName,

    /// Tag name to get values for
    pub tag_name: String,

    /// Optional prefix to filter values
    #[serde(default)]
    pub prefix: Option<String>,

    /// Maximum number of tag values to return
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Response for tag values query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagValuesResponse {
    /// List of tag values
    pub results: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::TimeUnit;

    #[test]
    fn test_query_request_validation() {
        let mut query = QueryRequest {
            start_relative: Some(RelativeTime::new(1, TimeUnit::Hours)),
            start_absolute: None,
            end_absolute: None,
            end_relative: None,
            metrics: vec![MetricQuery {
                name: MetricName::new("test.metric").unwrap(),
                tags: HashMap::new(),
                group_by: vec![],
                aggregators: vec![],
                limit: None,
            }],
            limit: None,
            cache_time: None,
        };

        assert!(query.validate_self().is_ok());

        // Test invalid query (no start time)
        query.start_relative = None;
        assert!(query.validate_self().is_err());
    }

    #[test]
    fn test_aggregator_validation() {
        let agg = Aggregator::Percentile {
            percentile: 95.0,
            sampling: None,
        };
        assert!(agg.validate_self().is_ok());

        let invalid_agg = Aggregator::Percentile {
            percentile: 150.0,
            sampling: None,
        };
        assert!(invalid_agg.validate_self().is_err());
    }

    #[test]
    fn test_time_range_calculation() {
        let query = QueryRequest {
            start_relative: Some(RelativeTime::new(1, TimeUnit::Hours)),
            start_absolute: None,
            end_absolute: None,
            end_relative: None,
            metrics: vec![],
            limit: None,
            cache_time: None,
        };

        let range = query.time_range().unwrap();
        assert!(range.duration_millis() > 0);
    }
}
