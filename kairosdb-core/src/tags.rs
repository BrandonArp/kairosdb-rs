//! Tag key-value pairs for data point metadata

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use validator::Validate;

use crate::error::{KairosError, KairosResult};

/// Tag key - identifies a tag dimension
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TagKey(String);

/// Tag value - the value for a tag dimension
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TagValue(String);

/// A set of tag key-value pairs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Validate)]
pub struct TagSet {
    #[validate(length(max = 100))]
    tags: HashMap<TagKey, TagValue>,
}

impl TagKey {
    /// Create a new tag key
    pub fn new<S: Into<String>>(key: S) -> KairosResult<Self> {
        let key = key.into();
        
        if key.is_empty() {
            return Err(KairosError::validation("Tag key cannot be empty"));
        }
        
        if key.len() > crate::MAX_TAG_LENGTH {
            return Err(KairosError::validation(format!(
                "Tag key too long: {} > {}",
                key.len(),
                crate::MAX_TAG_LENGTH
            )));
        }
        
        // Validate characters - similar to metric names but more restrictive
        if !key.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') {
            return Err(KairosError::validation(
                "Tag key contains invalid characters"
            ));
        }
        
        Ok(Self(key))
    }
    
    /// Create without validation (for internal use)
    pub(crate) fn new_unchecked<S: Into<String>>(key: S) -> Self {
        Self(key.into())
    }
    
    /// Get the string representation
    pub fn as_str(&self) -> &str {
        &self.0
    }
    
    /// Get the length in bytes
    pub fn len(&self) -> usize {
        self.0.len()
    }
    
    /// Check if the tag key is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl TagValue {
    /// Create a new tag value
    pub fn new<S: Into<String>>(value: S) -> KairosResult<Self> {
        let value = value.into();
        
        if value.is_empty() {
            return Err(KairosError::validation("Tag value cannot be empty"));
        }
        
        if value.len() > crate::MAX_TAG_LENGTH {
            return Err(KairosError::validation(format!(
                "Tag value too long: {} > {}",
                value.len(),
                crate::MAX_TAG_LENGTH
            )));
        }
        
        Ok(Self(value))
    }
    
    /// Create without validation (for internal use)
    pub(crate) fn new_unchecked<S: Into<String>>(value: S) -> Self {
        Self(value.into())
    }
    
    /// Get the string representation
    pub fn as_str(&self) -> &str {
        &self.0
    }
    
    /// Get the length in bytes
    pub fn len(&self) -> usize {
        self.0.len()
    }
    
    /// Check if the tag value is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl TagSet {
    /// Create a new empty tag set
    pub fn new() -> Self {
        Self {
            tags: HashMap::new(),
        }
    }
    
    /// Create a tag set from a HashMap
    pub fn from_map(map: HashMap<String, String>) -> KairosResult<Self> {
        let mut tags = HashMap::new();
        
        for (key, value) in map {
            let tag_key = TagKey::new(key)?;
            let tag_value = TagValue::new(value)?;
            tags.insert(tag_key, tag_value);
        }
        
        if tags.len() > crate::MAX_TAGS_PER_POINT {
            return Err(KairosError::validation(format!(
                "Too many tags: {} > {}",
                tags.len(),
                crate::MAX_TAGS_PER_POINT
            )));
        }
        
        Ok(Self { tags })
    }
    
    /// Insert a tag key-value pair
    pub fn insert(&mut self, key: TagKey, value: TagValue) -> KairosResult<Option<TagValue>> {
        if self.tags.len() >= crate::MAX_TAGS_PER_POINT && !self.tags.contains_key(&key) {
            return Err(KairosError::validation("Too many tags"));
        }
        
        Ok(self.tags.insert(key, value))
    }
    
    /// Get a tag value by key
    pub fn get(&self, key: &str) -> Option<&TagValue> {
        let tag_key = TagKey::new_unchecked(key);
        self.tags.get(&tag_key)
    }
    
    /// Check if a tag key exists
    pub fn contains_key(&self, key: &str) -> bool {
        let tag_key = TagKey::new_unchecked(key);
        self.tags.contains_key(&tag_key)
    }
    
    /// Remove a tag by key
    pub fn remove(&mut self, key: &str) -> Option<TagValue> {
        let tag_key = TagKey::new_unchecked(key);
        self.tags.remove(&tag_key)
    }
    
    /// Get the number of tags
    pub fn len(&self) -> usize {
        self.tags.len()
    }
    
    /// Check if the tag set is empty
    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }
    
    /// Iterate over tag key-value pairs
    pub fn iter(&self) -> impl Iterator<Item = (&TagKey, &TagValue)> {
        self.tags.iter()
    }
    
    /// Get all tag keys
    pub fn keys(&self) -> impl Iterator<Item = &TagKey> {
        self.tags.keys()
    }
    
    /// Get all tag values
    pub fn values(&self) -> impl Iterator<Item = &TagValue> {
        self.tags.values()
    }
    
    /// Clear all tags
    pub fn clear(&mut self) {
        self.tags.clear();
    }
    
    /// Estimate the memory size of this tag set
    pub fn estimated_size(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let entries_size = self.tags.iter()
            .map(|(k, v)| k.len() + v.len() + std::mem::size_of::<(TagKey, TagValue)>())
            .sum::<usize>();
        base_size + entries_size
    }
    
    /// Create a formatted string representation for Cassandra row key
    pub fn to_cassandra_format(&self) -> String {
        if self.is_empty() {
            return String::new();
        }
        
        let mut pairs: Vec<_> = self.tags.iter().collect();
        pairs.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
        
        pairs.iter()
            .map(|(k, v)| format!("{}={}", k.as_str(), v.as_str()))
            .collect::<Vec<_>>()
            .join(":")
    }
    
    /// Parse from Cassandra format
    pub fn from_cassandra_format(s: &str) -> KairosResult<Self> {
        if s.is_empty() {
            return Ok(Self::new());
        }
        
        let mut tags = HashMap::new();
        
        for pair in s.split(':') {
            let parts: Vec<&str> = pair.split('=').collect();
            if parts.len() != 2 {
                return Err(KairosError::parse(format!("Invalid tag pair: {}", pair)));
            }
            
            let key = TagKey::new(parts[0])?;
            let value = TagValue::new(parts[1])?;
            tags.insert(key, value);
        }
        
        if tags.len() > crate::MAX_TAGS_PER_POINT {
            return Err(KairosError::validation("Too many tags"));
        }
        
        Ok(Self { tags })
    }
    
    /// Convert to a standard HashMap for serialization
    pub fn to_hash_map(&self) -> HashMap<String, String> {
        self.tags.iter()
            .map(|(k, v)| (k.as_str().to_string(), v.as_str().to_string()))
            .collect()
    }
}

impl Default for TagSet {
    fn default() -> Self {
        Self::new()
    }
}

// Implement common conversions
impl fmt::Display for TagKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for TagValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for TagKey {
    fn from(s: String) -> Self {
        let s_clone = s.clone();
        Self::new(s).unwrap_or_else(|_| Self::new_unchecked(s_clone))
    }
}

impl From<&str> for TagKey {
    fn from(s: &str) -> Self {
        Self::new(s).unwrap_or_else(|_| Self::new_unchecked(s))
    }
}

impl From<String> for TagValue {
    fn from(s: String) -> Self {
        let s_clone = s.clone();
        Self::new(s).unwrap_or_else(|_| Self::new_unchecked(s_clone))
    }
}

impl From<&str> for TagValue {
    fn from(s: &str) -> Self {
        Self::new(s).unwrap_or_else(|_| Self::new_unchecked(s))
    }
}

impl AsRef<str> for TagKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for TagValue {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

// Implement PartialEq with str types for convenient testing
impl PartialEq<&str> for TagValue {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<str> for TagValue {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<String> for TagValue {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl PartialEq<&str> for TagKey {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<str> for TagKey {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<String> for TagKey {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_tag_creation() {
        let key = TagKey::new("host").unwrap();
        let value = TagValue::new("server1").unwrap();
        
        assert_eq!(key.as_str(), "host");
        assert_eq!(value.as_str(), "server1");
    }
    
    #[test]
    fn test_tag_validation() {
        // Valid tags
        assert!(TagKey::new("host").is_ok());
        assert!(TagKey::new("host_name").is_ok());
        assert!(TagKey::new("host-name").is_ok());
        assert!(TagValue::new("server1").is_ok());
        
        // Invalid tags
        assert!(TagKey::new("").is_err());
        assert!(TagKey::new("host.name").is_err()); // dots not allowed in keys
        assert!(TagValue::new("").is_err());
    }
    
    #[test]
    fn test_tag_set_operations() {
        let mut tags = TagSet::new();
        
        let key = TagKey::new("host").unwrap();
        let value = TagValue::new("server1").unwrap();
        
        tags.insert(key, value).unwrap();
        
        assert_eq!(tags.len(), 1);
        assert!(tags.contains_key("host"));
        assert_eq!(tags.get("host").unwrap().as_str(), "server1");
        
        tags.remove("host");
        assert!(tags.is_empty());
    }
    
    #[test]
    fn test_cassandra_format() {
        let mut tags = TagSet::new();
        tags.insert(TagKey::new("host").unwrap(), TagValue::new("server1").unwrap()).unwrap();
        tags.insert(TagKey::new("region").unwrap(), TagValue::new("us-east-1").unwrap()).unwrap();
        
        let formatted = tags.to_cassandra_format();
        // Should be sorted by key
        assert_eq!(formatted, "host=server1:region=us-east-1");
        
        let parsed = TagSet::from_cassandra_format(&formatted).unwrap();
        assert_eq!(parsed, tags);
    }
    
    #[test]
    fn test_hash_map_conversion() {
        let mut map = HashMap::new();
        map.insert("host".to_string(), "server1".to_string());
        map.insert("region".to_string(), "us-east-1".to_string());
        
        let tags = TagSet::from_map(map.clone()).unwrap();
        let converted = tags.to_hash_map();
        
        assert_eq!(converted, map);
    }
}