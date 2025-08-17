//! Time handling utilities for KairosDB

use chrono::{DateTime, Utc, TimeZone, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::error::{KairosError, KairosResult};

/// Timestamp representing a point in time
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Timestamp(DateTime<Utc>);

impl std::hash::Hash for Timestamp {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.timestamp_millis().hash(state);
    }
}

/// Time range for queries
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimeRange {
    /// Start time (inclusive)
    pub start: Timestamp,
    /// End time (exclusive) 
    pub end: Timestamp,
}

impl Timestamp {
    /// Get the current timestamp
    pub fn now() -> Self {
        Self(Utc::now())
    }
    
    /// Create from milliseconds since Unix epoch
    pub fn from_millis(millis: i64) -> KairosResult<Self> {
        match Utc.timestamp_millis_opt(millis) {
            chrono::LocalResult::Single(dt) => Ok(Self(dt)),
            _ => Err(KairosError::TimeRange(format!("Invalid timestamp: {}", millis))),
        }
    }
    
    /// Create from seconds since Unix epoch
    pub fn from_secs(secs: i64) -> KairosResult<Self> {
        match Utc.timestamp_opt(secs, 0) {
            chrono::LocalResult::Single(dt) => Ok(Self(dt)),
            _ => Err(KairosError::TimeRange(format!("Invalid timestamp: {}", secs))),
        }
    }
    
    /// Create from a DateTime<Utc>
    pub fn from_datetime(dt: DateTime<Utc>) -> Self {
        Self(dt)
    }
    
    /// Get milliseconds since Unix epoch
    pub fn timestamp_millis(&self) -> i64 {
        self.0.timestamp_millis()
    }
    
    /// Get seconds since Unix epoch  
    pub fn timestamp(&self) -> i64 {
        self.0.timestamp()
    }
    
    /// Get the underlying DateTime<Utc>
    pub fn datetime(&self) -> DateTime<Utc> {
        self.0
    }
    
    /// Format as ISO 8601 string
    pub fn to_rfc3339(&self) -> String {
        self.0.to_rfc3339()
    }
    
    /// Parse from ISO 8601 string
    pub fn from_rfc3339(s: &str) -> KairosResult<Self> {
        let dt = DateTime::parse_from_rfc3339(s)
            .map_err(|e| KairosError::TimeRange(format!("Invalid RFC3339 timestamp: {}", e)))?
            .with_timezone(&Utc);
        Ok(Self(dt))
    }
    
    /// Add duration in milliseconds
    pub fn add_millis(&self, millis: i64) -> KairosResult<Self> {
        let duration = chrono::Duration::milliseconds(millis);
        self.0.checked_add_signed(duration)
            .map(Self)
            .ok_or_else(|| KairosError::TimeRange("Timestamp overflow".to_string()))
    }
    
    /// Subtract duration in milliseconds
    pub fn sub_millis(&self, millis: i64) -> KairosResult<Self> {
        let duration = chrono::Duration::milliseconds(millis);
        self.0.checked_sub_signed(duration)
            .map(Self)
            .ok_or_else(|| KairosError::TimeRange("Timestamp underflow".to_string()))
    }
    
    /// Get the row time for Cassandra partitioning (rounded to 3-week boundaries)
    pub fn row_time(&self) -> Self {
        // KairosDB uses 3-week rows (1,814,400,000 ms)
        const THREE_WEEKS_MS: i64 = 3 * 7 * 24 * 60 * 60 * 1000;
        
        let millis = self.timestamp_millis();
        let row_millis = (millis / THREE_WEEKS_MS) * THREE_WEEKS_MS;
        
        Self::from_millis(row_millis).unwrap_or(*self)
    }
    
    /// Get the offset within the row for column naming
    pub fn row_offset(&self) -> i64 {
        const THREE_WEEKS_MS: i64 = 3 * 7 * 24 * 60 * 60 * 1000;
        
        let millis = self.timestamp_millis();
        let row_start = (millis / THREE_WEEKS_MS) * THREE_WEEKS_MS;
        millis - row_start
    }
}

impl TimeRange {
    /// Create a new time range
    pub fn new(start: Timestamp, end: Timestamp) -> KairosResult<Self> {
        if start >= end {
            return Err(KairosError::TimeRange(
                "Start time must be before end time".to_string()
            ));
        }
        
        Ok(Self { start, end })
    }
    
    /// Create a time range from milliseconds
    pub fn from_millis(start_ms: i64, end_ms: i64) -> KairosResult<Self> {
        let start = Timestamp::from_millis(start_ms)?;
        let end = Timestamp::from_millis(end_ms)?;
        Self::new(start, end)
    }
    
    /// Get the duration of this time range in milliseconds
    pub fn duration_millis(&self) -> i64 {
        self.end.timestamp_millis() - self.start.timestamp_millis()
    }
    
    /// Check if a timestamp falls within this range
    pub fn contains(&self, timestamp: Timestamp) -> bool {
        timestamp >= self.start && timestamp < self.end
    }
    
    /// Check if this range overlaps with another
    pub fn overlaps(&self, other: &TimeRange) -> bool {
        self.start < other.end && other.start < self.end
    }
    
    /// Get the intersection with another time range
    pub fn intersect(&self, other: &TimeRange) -> Option<TimeRange> {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        
        if start < end {
            Some(TimeRange { start, end })
        } else {
            None
        }
    }
    
    /// Split this time range into chunks of specified duration
    pub fn split_by_duration(&self, chunk_millis: i64) -> KairosResult<Vec<TimeRange>> {
        if chunk_millis <= 0 {
            return Err(KairosError::TimeRange("Chunk duration must be positive".to_string()));
        }
        
        let mut ranges = Vec::new();
        let mut current_start = self.start;
        
        while current_start < self.end {
            let current_end = current_start.add_millis(chunk_millis)?
                .min(self.end);
            
            ranges.push(TimeRange {
                start: current_start,
                end: current_end,
            });
            
            current_start = current_end;
        }
        
        Ok(ranges)
    }
    
    /// Get all row times that this range spans
    pub fn row_times(&self) -> Vec<Timestamp> {
        let mut row_times = Vec::new();
        let mut current = self.start.row_time();
        
        // Add row times until we exceed the end time
        while current.timestamp_millis() < self.end.timestamp_millis() {
            row_times.push(current);
            
            // Move to next row (3 weeks later)
            if let Ok(next) = current.add_millis(3 * 7 * 24 * 60 * 60 * 1000) {
                current = next;
            } else {
                break;
            }
        }
        
        row_times
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_rfc3339())
    }
}

impl fmt::Display for TimeRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{} - {})", self.start, self.end)
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(dt: DateTime<Utc>) -> Self {
        Self(dt)
    }
}

impl From<Timestamp> for DateTime<Utc> {
    fn from(ts: Timestamp) -> Self {
        ts.0
    }
}

/// Relative time specification for queries
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelativeTime {
    pub value: i64,
    pub unit: TimeUnit,
}

/// Time units for relative time specifications
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeUnit {
    Milliseconds,
    Seconds,
    Minutes,
    Hours,
    Days,
    Weeks,
    Months,
    Years,
}

impl RelativeTime {
    /// Create a new relative time
    pub fn new(value: i64, unit: TimeUnit) -> Self {
        Self { value, unit }
    }
    
    /// Convert to milliseconds (approximate for months/years)
    pub fn to_millis(&self) -> i64 {
        match self.unit {
            TimeUnit::Milliseconds => self.value,
            TimeUnit::Seconds => self.value * 1000,
            TimeUnit::Minutes => self.value * 60 * 1000,
            TimeUnit::Hours => self.value * 60 * 60 * 1000,
            TimeUnit::Days => self.value * 24 * 60 * 60 * 1000,
            TimeUnit::Weeks => self.value * 7 * 24 * 60 * 60 * 1000,
            TimeUnit::Months => self.value * 30 * 24 * 60 * 60 * 1000, // Approximate
            TimeUnit::Years => self.value * 365 * 24 * 60 * 60 * 1000, // Approximate
        }
    }
    
    /// Apply this relative time to a timestamp (subtract for "ago")
    pub fn before(&self, timestamp: Timestamp) -> KairosResult<Timestamp> {
        timestamp.sub_millis(self.to_millis())
    }
    
    /// Apply this relative time to a timestamp (add for "from now")
    pub fn after(&self, timestamp: Timestamp) -> KairosResult<Timestamp> {
        timestamp.add_millis(self.to_millis())
    }
}

impl fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            TimeUnit::Milliseconds => "milliseconds",
            TimeUnit::Seconds => "seconds", 
            TimeUnit::Minutes => "minutes",
            TimeUnit::Hours => "hours",
            TimeUnit::Days => "days",
            TimeUnit::Weeks => "weeks",
            TimeUnit::Months => "months",
            TimeUnit::Years => "years",
        };
        write!(f, "{}", s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_timestamp_creation() {
        let now = Timestamp::now();
        let from_millis = Timestamp::from_millis(now.timestamp_millis()).unwrap();
        
        assert_eq!(now.timestamp_millis(), from_millis.timestamp_millis());
    }
    
    #[test]
    fn test_timestamp_arithmetic() {
        let ts = Timestamp::from_millis(1000000).unwrap();
        let later = ts.add_millis(5000).unwrap();
        let earlier = ts.sub_millis(2000).unwrap();
        
        assert_eq!(later.timestamp_millis(), 1005000);
        assert_eq!(earlier.timestamp_millis(), 998000);
    }
    
    #[test]
    fn test_time_range() {
        let start = Timestamp::from_millis(1000).unwrap();
        let end = Timestamp::from_millis(2000).unwrap();
        let range = TimeRange::new(start, end).unwrap();
        
        assert_eq!(range.duration_millis(), 1000);
        assert!(range.contains(Timestamp::from_millis(1500).unwrap()));
        assert!(!range.contains(Timestamp::from_millis(2500).unwrap()));
    }
    
    #[test]
    fn test_relative_time() {
        let rel = RelativeTime::new(5, TimeUnit::Minutes);
        assert_eq!(rel.to_millis(), 5 * 60 * 1000);
        
        let now = Timestamp::now();
        let five_mins_ago = rel.before(now).unwrap();
        assert!(five_mins_ago < now);
    }
    
    #[test]
    fn test_row_time_calculation() {
        let ts = Timestamp::from_millis(1000000000).unwrap();
        let row_time = ts.row_time();
        let offset = ts.row_offset();
        
        // Row time should be aligned to 3-week boundary
        assert_eq!(row_time.timestamp_millis() % (3 * 7 * 24 * 60 * 60 * 1000), 0);
        assert_eq!(offset, ts.timestamp_millis() - row_time.timestamp_millis());
    }
}