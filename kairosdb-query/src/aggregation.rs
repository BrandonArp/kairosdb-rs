//! Aggregation engine for processing time series data

use kairosdb_core::{
    datapoint::{DataPoint, DataPointValue},
    error::{KairosError, KairosResult},
    query::Aggregator,
    time::Timestamp,
};
use ordered_float::OrderedFloat;
use std::collections::HashMap;
use tracing::debug;

/// Aggregation engine that applies various aggregation functions to time series data
#[allow(dead_code)]
pub struct AggregationEngine;

#[allow(dead_code)]
impl AggregationEngine {
    /// Create a new aggregation engine
    pub fn new() -> Self {
        Self
    }

    /// Apply a series of aggregations to data points
    pub fn apply_aggregations(
        &self,
        data_points: &[DataPoint],
        aggregators: &[Aggregator],
    ) -> KairosResult<Vec<DataPoint>> {
        let mut result = data_points.to_vec();

        for aggregator in aggregators {
            result = self.apply_single_aggregation(result, aggregator)?;
        }

        Ok(result)
    }

    /// Apply a single aggregation to data points
    fn apply_single_aggregation(
        &self,
        data_points: Vec<DataPoint>,
        aggregator: &Aggregator,
    ) -> KairosResult<Vec<DataPoint>> {
        if data_points.is_empty() {
            return Ok(data_points);
        }

        match aggregator {
            Aggregator::Average { sampling } => self.apply_average(data_points, sampling),
            Aggregator::Sum { sampling } => self.apply_sum(data_points, sampling),
            Aggregator::Min { sampling } => self.apply_min(data_points, sampling),
            Aggregator::Max { sampling } => self.apply_max(data_points, sampling),
            Aggregator::Count { sampling } => self.apply_count(data_points, sampling),
            Aggregator::StandardDeviation { sampling } => self.apply_std_dev(data_points, sampling),
            Aggregator::Percentile {
                percentile,
                sampling,
            } => self.apply_percentile(data_points, *percentile, sampling),
            Aggregator::Rate { unit } => self.apply_rate(data_points, unit),
            Aggregator::Sampler { unit } => self.apply_sampler(data_points, unit),
        }
    }

    /// Apply average aggregation
    fn apply_average(
        &self,
        data_points: Vec<DataPoint>,
        sampling: &Option<kairosdb_core::query::Sampling>,
    ) -> KairosResult<Vec<DataPoint>> {
        let groups = self.group_by_sampling(data_points, sampling)?;
        let mut result = Vec::new();

        for (timestamp, points) in groups {
            if let Some(avg_value) = self.calculate_average(&points) {
                let mut avg_point = points[0].clone();
                avg_point.timestamp = timestamp;
                avg_point.value = avg_value;
                result.push(avg_point);
            }
        }

        result.sort_by_key(|p| p.timestamp);
        Ok(result)
    }

    /// Apply sum aggregation
    fn apply_sum(
        &self,
        data_points: Vec<DataPoint>,
        sampling: &Option<kairosdb_core::query::Sampling>,
    ) -> KairosResult<Vec<DataPoint>> {
        let groups = self.group_by_sampling(data_points, sampling)?;
        let mut result = Vec::new();

        for (timestamp, points) in groups {
            if let Some(sum_value) = self.calculate_sum(&points) {
                let mut sum_point = points[0].clone();
                sum_point.timestamp = timestamp;
                sum_point.value = sum_value;
                result.push(sum_point);
            }
        }

        result.sort_by_key(|p| p.timestamp);
        Ok(result)
    }

    /// Apply min aggregation
    fn apply_min(
        &self,
        data_points: Vec<DataPoint>,
        sampling: &Option<kairosdb_core::query::Sampling>,
    ) -> KairosResult<Vec<DataPoint>> {
        let groups = self.group_by_sampling(data_points, sampling)?;
        let mut result = Vec::new();

        for (timestamp, points) in groups {
            if let Some(min_point) = self.find_min(&points) {
                let mut result_point = min_point.clone();
                result_point.timestamp = timestamp;
                result.push(result_point);
            }
        }

        result.sort_by_key(|p| p.timestamp);
        Ok(result)
    }

    /// Apply max aggregation
    fn apply_max(
        &self,
        data_points: Vec<DataPoint>,
        sampling: &Option<kairosdb_core::query::Sampling>,
    ) -> KairosResult<Vec<DataPoint>> {
        let groups = self.group_by_sampling(data_points, sampling)?;
        let mut result = Vec::new();

        for (timestamp, points) in groups {
            if let Some(max_point) = self.find_max(&points) {
                let mut result_point = max_point.clone();
                result_point.timestamp = timestamp;
                result.push(result_point);
            }
        }

        result.sort_by_key(|p| p.timestamp);
        Ok(result)
    }

    /// Apply count aggregation
    fn apply_count(
        &self,
        data_points: Vec<DataPoint>,
        sampling: &Option<kairosdb_core::query::Sampling>,
    ) -> KairosResult<Vec<DataPoint>> {
        let groups = self.group_by_sampling(data_points, sampling)?;
        let mut result = Vec::new();

        for (timestamp, points) in groups {
            let mut count_point = points[0].clone();
            count_point.timestamp = timestamp;
            count_point.value = DataPointValue::Long(points.len() as i64);
            result.push(count_point);
        }

        result.sort_by_key(|p| p.timestamp);
        Ok(result)
    }

    /// Apply standard deviation aggregation
    fn apply_std_dev(
        &self,
        data_points: Vec<DataPoint>,
        sampling: &Option<kairosdb_core::query::Sampling>,
    ) -> KairosResult<Vec<DataPoint>> {
        let groups = self.group_by_sampling(data_points, sampling)?;
        let mut result = Vec::new();

        for (timestamp, points) in groups {
            if let Some(std_dev) = self.calculate_std_dev(&points) {
                let mut std_point = points[0].clone();
                std_point.timestamp = timestamp;
                std_point.value = DataPointValue::Double(OrderedFloat(std_dev));
                result.push(std_point);
            }
        }

        result.sort_by_key(|p| p.timestamp);
        Ok(result)
    }

    /// Apply percentile aggregation
    fn apply_percentile(
        &self,
        data_points: Vec<DataPoint>,
        percentile: f64,
        sampling: &Option<kairosdb_core::query::Sampling>,
    ) -> KairosResult<Vec<DataPoint>> {
        let groups = self.group_by_sampling(data_points, sampling)?;
        let mut result = Vec::new();

        for (timestamp, points) in groups {
            if let Some(percentile_value) = self.calculate_percentile(&points, percentile) {
                let mut perc_point = points[0].clone();
                perc_point.timestamp = timestamp;
                perc_point.value = percentile_value;
                result.push(perc_point);
            }
        }

        result.sort_by_key(|p| p.timestamp);
        Ok(result)
    }

    /// Apply rate aggregation (rate of change)
    fn apply_rate(&self, data_points: Vec<DataPoint>, unit: &str) -> KairosResult<Vec<DataPoint>> {
        if data_points.len() < 2 {
            return Ok(Vec::new());
        }

        let mut result = Vec::new();
        let unit_multiplier = self.get_time_unit_multiplier(unit)?;

        for i in 1..data_points.len() {
            let prev = &data_points[i - 1];
            let curr = &data_points[i];

            if let (Some(prev_val), Some(curr_val)) = (prev.value.as_f64(), curr.value.as_f64()) {
                let time_diff_ms =
                    curr.timestamp.timestamp_millis() - prev.timestamp.timestamp_millis();
                if time_diff_ms > 0 {
                    let value_diff = curr_val - prev_val;
                    let rate = (value_diff / time_diff_ms as f64) * unit_multiplier;

                    let mut rate_point = curr.clone();
                    rate_point.value = DataPointValue::Double(OrderedFloat(rate));
                    result.push(rate_point);
                }
            }
        }

        Ok(result)
    }

    /// Apply sampler aggregation (sampling data points)
    fn apply_sampler(
        &self,
        data_points: Vec<DataPoint>,
        unit: &str,
    ) -> KairosResult<Vec<DataPoint>> {
        // For now, just return the original data points
        // In a real implementation, this would sample based on the unit
        debug!(
            "Sampler aggregation with unit '{}' - returning original data",
            unit
        );
        Ok(data_points)
    }

    /// Group data points by sampling interval
    fn group_by_sampling(
        &self,
        data_points: Vec<DataPoint>,
        sampling: &Option<kairosdb_core::query::Sampling>,
    ) -> KairosResult<HashMap<Timestamp, Vec<DataPoint>>> {
        let mut groups = HashMap::new();

        if let Some(sampling) = sampling {
            let interval_ms = self.get_sampling_interval_ms(sampling)?;

            for point in data_points {
                // Round timestamp to sampling interval
                let rounded_ts = self.round_timestamp_to_interval(point.timestamp, interval_ms)?;
                groups
                    .entry(rounded_ts)
                    .or_insert_with(Vec::new)
                    .push(point);
            }
        } else {
            // No sampling - each point is its own group
            for point in data_points {
                groups.insert(point.timestamp, vec![point]);
            }
        }

        Ok(groups)
    }

    /// Calculate average of numeric data points
    fn calculate_average(&self, points: &[DataPoint]) -> Option<DataPointValue> {
        let numeric_values: Vec<f64> = points.iter().filter_map(|p| p.value.as_f64()).collect();

        if numeric_values.is_empty() {
            return None;
        }

        let sum: f64 = numeric_values.iter().sum();
        let avg = sum / numeric_values.len() as f64;
        Some(DataPointValue::Double(OrderedFloat(avg)))
    }

    /// Calculate sum of numeric data points
    fn calculate_sum(&self, points: &[DataPoint]) -> Option<DataPointValue> {
        let mut int_sum = 0i64;
        let mut float_sum = 0.0f64;
        let mut has_int = false;
        let mut has_float = false;

        for point in points {
            match &point.value {
                DataPointValue::Long(v) => {
                    int_sum += v;
                    has_int = true;
                }
                DataPointValue::Double(v) => {
                    float_sum += v.into_inner();
                    has_float = true;
                }
                _ => continue,
            }
        }

        if has_float {
            Some(DataPointValue::Double(OrderedFloat(
                float_sum + int_sum as f64,
            )))
        } else if has_int {
            Some(DataPointValue::Long(int_sum))
        } else {
            None
        }
    }

    /// Find minimum data point
    fn find_min<'a>(&self, points: &'a [DataPoint]) -> Option<&'a DataPoint> {
        points
            .iter()
            .min_by(|a, b| match (a.value.as_f64(), b.value.as_f64()) {
                (Some(a_val), Some(b_val)) => a_val
                    .partial_cmp(&b_val)
                    .unwrap_or(std::cmp::Ordering::Equal),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            })
    }

    /// Find maximum data point
    fn find_max<'a>(&self, points: &'a [DataPoint]) -> Option<&'a DataPoint> {
        points
            .iter()
            .max_by(|a, b| match (a.value.as_f64(), b.value.as_f64()) {
                (Some(a_val), Some(b_val)) => a_val
                    .partial_cmp(&b_val)
                    .unwrap_or(std::cmp::Ordering::Equal),
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (None, None) => std::cmp::Ordering::Equal,
            })
    }

    /// Calculate standard deviation
    fn calculate_std_dev(&self, points: &[DataPoint]) -> Option<f64> {
        let numeric_values: Vec<f64> = points.iter().filter_map(|p| p.value.as_f64()).collect();

        if numeric_values.len() < 2 {
            return None;
        }

        let mean: f64 = numeric_values.iter().sum::<f64>() / numeric_values.len() as f64;
        let variance: f64 = numeric_values
            .iter()
            .map(|value| {
                let diff = mean - value;
                diff * diff
            })
            .sum::<f64>()
            / (numeric_values.len() - 1) as f64;

        Some(variance.sqrt())
    }

    /// Calculate percentile value
    fn calculate_percentile(
        &self,
        points: &[DataPoint],
        percentile: f64,
    ) -> Option<DataPointValue> {
        let mut numeric_values: Vec<f64> = points.iter().filter_map(|p| p.value.as_f64()).collect();

        if numeric_values.is_empty() {
            return None;
        }

        numeric_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let index = ((percentile / 100.0) * (numeric_values.len() - 1) as f64).round() as usize;
        let index = index.min(numeric_values.len() - 1);

        Some(DataPointValue::Double(OrderedFloat(numeric_values[index])))
    }

    /// Get sampling interval in milliseconds
    fn get_sampling_interval_ms(
        &self,
        sampling: &kairosdb_core::query::Sampling,
    ) -> KairosResult<i64> {
        let multiplier = match sampling.unit.as_str() {
            "milliseconds" => 1,
            "seconds" => 1000,
            "minutes" => 60 * 1000,
            "hours" => 60 * 60 * 1000,
            "days" => 24 * 60 * 60 * 1000,
            _ => {
                return Err(KairosError::validation(format!(
                    "Unknown sampling unit: {}",
                    sampling.unit
                )))
            }
        };

        Ok(sampling.value * multiplier)
    }

    /// Get time unit multiplier for rate calculations
    fn get_time_unit_multiplier(&self, unit: &str) -> KairosResult<f64> {
        match unit {
            "milliseconds" => Ok(1.0),
            "seconds" => Ok(1000.0),
            "minutes" => Ok(60.0 * 1000.0),
            "hours" => Ok(60.0 * 60.0 * 1000.0),
            "days" => Ok(24.0 * 60.0 * 60.0 * 1000.0),
            _ => Err(KairosError::validation(format!(
                "Unknown time unit: {}",
                unit
            ))),
        }
    }

    /// Round timestamp to sampling interval
    fn round_timestamp_to_interval(
        &self,
        timestamp: Timestamp,
        interval_ms: i64,
    ) -> KairosResult<Timestamp> {
        let ts_ms = timestamp.timestamp_millis();
        let rounded_ms = (ts_ms / interval_ms) * interval_ms;
        Timestamp::from_millis(rounded_ms)
    }
}

impl Default for AggregationEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kairosdb_core::{datapoint::DataPoint, time::Timestamp};

    #[test]
    fn test_average_aggregation() {
        let engine = AggregationEngine::new();

        let points = vec![
            DataPoint::new_long("test", Timestamp::from_millis(1000).unwrap(), 10),
            DataPoint::new_long("test", Timestamp::from_millis(2000).unwrap(), 20),
            DataPoint::new_long("test", Timestamp::from_millis(3000).unwrap(), 30),
        ];

        let result = engine.apply_average(points, &None).unwrap();
        assert_eq!(result.len(), 3); // No sampling, so each point is its own group

        // With sampling
        let sampling = kairosdb_core::query::Sampling {
            value: 5,
            unit: "seconds".to_string(),
        };

        let points = vec![
            DataPoint::new_long("test", Timestamp::from_millis(1000).unwrap(), 10),
            DataPoint::new_long("test", Timestamp::from_millis(2000).unwrap(), 20),
            DataPoint::new_long("test", Timestamp::from_millis(6000).unwrap(), 30),
        ];

        let result = engine.apply_average(points, &Some(sampling)).unwrap();
        assert!(result.len() <= 2); // Should be grouped by 5-second intervals
    }

    #[test]
    fn test_percentile_calculation() {
        let engine = AggregationEngine::new();

        let points = vec![
            DataPoint::new_double("test", Timestamp::now(), 1.0),
            DataPoint::new_double("test", Timestamp::now(), 2.0),
            DataPoint::new_double("test", Timestamp::now(), 3.0),
            DataPoint::new_double("test", Timestamp::now(), 4.0),
            DataPoint::new_double("test", Timestamp::now(), 5.0),
        ];

        let result = engine.calculate_percentile(&points, 50.0);
        assert!(result.is_some());

        if let Some(DataPointValue::Double(val)) = result {
            assert_eq!(val.into_inner(), 3.0);
        } else {
            panic!("Expected double value");
        }
    }
}
