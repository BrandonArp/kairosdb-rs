//! Metric name types and operations

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::error::{KairosError, KairosResult};

/// Metric name - a string identifier for a time series
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MetricName(String);

impl MetricName {
    /// Create a new metric name
    pub fn new<S: Into<String>>(name: S) -> KairosResult<Self> {
        let name = name.into();

        if name.is_empty() {
            return Err(KairosError::validation("Metric name cannot be empty"));
        }

        if name.len() > crate::MAX_METRIC_NAME_LENGTH {
            return Err(KairosError::validation(format!(
                "Metric name too long: {} > {}",
                name.len(),
                crate::MAX_METRIC_NAME_LENGTH
            )));
        }

        // Validate characters (basic validation - can be extended)
        if !name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '-')
        {
            return Err(KairosError::validation(
                "Metric name contains invalid characters",
            ));
        }

        Ok(Self(name))
    }

    /// Create without validation (for internal use)
    pub(crate) fn new_unchecked<S: Into<String>>(name: S) -> Self {
        Self(name.into())
    }

    /// Get the string representation
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Get the length in bytes
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the metric name is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Check if this metric name starts with a prefix
    pub fn starts_with(&self, prefix: &str) -> bool {
        self.0.starts_with(prefix)
    }

    /// Check if this metric name ends with a suffix
    pub fn ends_with(&self, suffix: &str) -> bool {
        self.0.ends_with(suffix)
    }

    /// Check if this metric name contains a substring
    pub fn contains(&self, pattern: &str) -> bool {
        self.0.contains(pattern)
    }

    /// Get the metric name components split by '.'
    pub fn components(&self) -> Vec<&str> {
        self.0.split('.').collect()
    }

    /// Get the parent metric name (everything before the last '.')
    pub fn parent(&self) -> Option<MetricName> {
        if let Some(pos) = self.0.rfind('.') {
            Some(Self::new_unchecked(&self.0[..pos]))
        } else {
            None
        }
    }

    /// Get the leaf component (everything after the last '.')
    pub fn leaf(&self) -> &str {
        if let Some(pos) = self.0.rfind('.') {
            &self.0[pos + 1..]
        } else {
            &self.0
        }
    }

    /// Create a child metric name by appending a component
    pub fn child<S: AsRef<str>>(&self, component: S) -> KairosResult<MetricName> {
        let child_name = format!("{}.{}", self.0, component.as_ref());
        Self::new(child_name)
    }
}

impl fmt::Display for MetricName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for MetricName {
    fn from(s: String) -> Self {
        let s_clone = s.clone();
        Self::new(s).unwrap_or_else(|_| Self::new_unchecked(s_clone))
    }
}

impl From<&str> for MetricName {
    fn from(s: &str) -> Self {
        Self::new(s).unwrap_or_else(|_| Self::new_unchecked(s))
    }
}

impl AsRef<str> for MetricName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl PartialEq<str> for MetricName {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for MetricName {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<String> for MetricName {
    fn eq(&self, other: &String) -> bool {
        &self.0 == other
    }
}

/// A collection of metric names with efficient storage and lookup
#[derive(Debug, Clone, Default)]
pub struct MetricNameSet {
    names: std::collections::HashSet<MetricName>,
}

impl MetricNameSet {
    /// Create a new empty set
    pub fn new() -> Self {
        Self {
            names: std::collections::HashSet::new(),
        }
    }

    /// Insert a metric name
    pub fn insert(&mut self, name: MetricName) -> bool {
        self.names.insert(name)
    }

    /// Check if the set contains a metric name
    pub fn contains(&self, name: &MetricName) -> bool {
        self.names.contains(name)
    }

    /// Remove a metric name
    pub fn remove(&mut self, name: &MetricName) -> bool {
        self.names.remove(name)
    }

    /// Get the number of metric names
    pub fn len(&self) -> usize {
        self.names.len()
    }

    /// Check if the set is empty
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// Iterate over metric names
    pub fn iter(&self) -> impl Iterator<Item = &MetricName> {
        self.names.iter()
    }

    /// Clear all metric names
    pub fn clear(&mut self) {
        self.names.clear();
    }

    /// Find metric names matching a pattern
    pub fn find_matching<P>(&self, predicate: P) -> Vec<&MetricName>
    where
        P: Fn(&MetricName) -> bool,
    {
        self.names.iter().filter(|name| predicate(name)).collect()
    }

    /// Find metric names with a specific prefix
    pub fn find_with_prefix(&self, prefix: &str) -> Vec<&MetricName> {
        self.find_matching(|name| name.starts_with(prefix))
    }

    /// Convert to a sorted vector
    pub fn to_sorted_vec(&self) -> Vec<MetricName> {
        let mut names: Vec<_> = self.names.iter().cloned().collect();
        names.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        names
    }
}

impl FromIterator<MetricName> for MetricNameSet {
    fn from_iter<I: IntoIterator<Item = MetricName>>(iter: I) -> Self {
        Self {
            names: iter.into_iter().collect(),
        }
    }
}

impl Extend<MetricName> for MetricNameSet {
    fn extend<I: IntoIterator<Item = MetricName>>(&mut self, iter: I) {
        self.names.extend(iter);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_name_creation() {
        let name = MetricName::new("test.metric").unwrap();
        assert_eq!(name.as_str(), "test.metric");
        assert_eq!(name.len(), 11);
        assert!(!name.is_empty());
    }

    #[test]
    fn test_metric_name_validation() {
        // Valid names
        assert!(MetricName::new("simple").is_ok());
        assert!(MetricName::new("with.dots").is_ok());
        assert!(MetricName::new("with_underscores").is_ok());
        assert!(MetricName::new("with-dashes").is_ok());
        assert!(MetricName::new("with123numbers").is_ok());

        // Invalid names
        assert!(MetricName::new("").is_err());
        assert!(MetricName::new("with spaces").is_err());
        assert!(MetricName::new("with@symbols").is_err());
    }

    #[test]
    fn test_metric_name_components() {
        let name = MetricName::new("system.cpu.usage").unwrap();

        assert_eq!(name.components(), vec!["system", "cpu", "usage"]);
        assert_eq!(name.leaf(), "usage");
        assert_eq!(name.parent().unwrap().as_str(), "system.cpu");

        let child = name.child("percent").unwrap();
        assert_eq!(child.as_str(), "system.cpu.usage.percent");
    }

    #[test]
    fn test_metric_name_set() {
        let mut set = MetricNameSet::new();

        let name1 = MetricName::new("metric1").unwrap();
        let name2 = MetricName::new("metric2").unwrap();

        assert!(set.insert(name1.clone()));
        assert!(set.insert(name2.clone()));
        assert!(!set.insert(name1.clone())); // Duplicate

        assert_eq!(set.len(), 2);
        assert!(set.contains(&name1));
        assert!(set.contains(&name2));

        let with_prefix = set.find_with_prefix("metric");
        assert_eq!(with_prefix.len(), 2);
    }

    #[test]
    fn test_metric_name_equality() {
        let name = MetricName::new("test.metric").unwrap();

        assert_eq!(name, "test.metric");
        assert_eq!(name, String::from("test.metric"));
        assert_eq!(name.as_str(), "test.metric");
    }
}
