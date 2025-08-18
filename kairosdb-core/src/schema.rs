//! Cassandra schema definitions for KairosDB

use crate::error::{KairosError, KairosResult};

/// Cassandra keyspace and table schema for KairosDB
pub struct KairosSchema {
    pub keyspace: String,
    pub replication_factor: u32,
}

impl Default for KairosSchema {
    fn default() -> Self {
        Self {
            keyspace: "kairosdb".to_string(),
            replication_factor: 1,
        }
    }
}

impl KairosSchema {
    /// Create a new schema configuration
    pub fn new(keyspace: String, replication_factor: u32) -> Self {
        Self {
            keyspace,
            replication_factor,
        }
    }

    /// Generate CQL for creating the keyspace
    pub fn create_keyspace_cql(&self) -> String {
        format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': {}}};",
            self.keyspace,
            self.replication_factor
        )
    }

    /// Generate CQL for creating the data_points table
    pub fn create_data_points_table_cql(&self) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {}.data_points (
                key blob,
                column1 blob,
                value blob,
                PRIMARY KEY (key, column1)
            ) WITH COMPACT STORAGE
            AND compression = {{'sstable_compression': 'LZ4Compressor'}}
            AND compaction = {{'class': 'LeveledCompactionStrategy'}}
            AND gc_grace_seconds = 864000;",
            self.keyspace
        )
    }

    /// Generate CQL for creating the row_key_index table
    pub fn create_row_key_index_table_cql(&self) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {}.row_key_index (
                key blob,
                column1 blob,
                value blob,
                PRIMARY KEY (key, column1)
            ) WITH COMPACT STORAGE
            AND compression = {{'sstable_compression': 'LZ4Compressor'}}
            AND compaction = {{'class': 'SizeTieredCompactionStrategy'}}
            AND gc_grace_seconds = 864000;",
            self.keyspace
        )
    }

    /// Generate CQL for creating the string_index table
    pub fn create_string_index_table_cql(&self) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {}.string_index (
                key blob,
                column1 varchar,
                value blob,
                PRIMARY KEY (key, column1)
            ) WITH compression = {{'sstable_compression': 'LZ4Compressor'}}
            AND compaction = {{'class': 'SizeTieredCompactionStrategy'}}
            AND gc_grace_seconds = 864000;",
            self.keyspace
        )
    }

    /// Generate all CQL statements for schema creation
    pub fn create_schema_cql(&self) -> Vec<String> {
        vec![
            self.create_keyspace_cql(),
            self.create_data_points_table_cql(),
            self.create_row_key_index_table_cql(),
            self.create_string_index_table_cql(),
        ]
    }

    /// Validate schema configuration
    pub fn validate(&self) -> KairosResult<()> {
        if self.keyspace.is_empty() {
            return Err(KairosError::schema("Keyspace name cannot be empty"));
        }

        if self.replication_factor == 0 {
            return Err(KairosError::schema(
                "Replication factor must be greater than 0",
            ));
        }

        // Validate keyspace name (basic validation)
        if !self
            .keyspace
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_')
        {
            return Err(KairosError::schema("Invalid keyspace name"));
        }

        Ok(())
    }
}

/// Wrapper for index key bytes
#[derive(Debug, Clone)]
pub struct IndexKey {
    pub bytes: Vec<u8>,
}

impl IndexKey {
    pub fn to_bytes(&self) -> Vec<u8> {
        self.bytes.clone()
    }
}

/// Wrapper for index column bytes  
#[derive(Debug, Clone)]
pub struct IndexColumn {
    pub bytes: Vec<u8>,
}

impl IndexColumn {
    pub fn to_bytes(&self) -> Vec<u8> {
        self.bytes.clone()
    }
}

/// Wrapper for index value bytes
#[derive(Debug, Clone)]
pub struct IndexValue {
    pub bytes: Vec<u8>,
}

impl IndexValue {
    pub fn to_bytes(&self) -> Vec<u8> {
        self.bytes.clone()
    }
}

/// Index types for the string_index table
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IndexType {
    /// Index for metric names
    MetricNames,
    /// Index for tag names
    TagNames,
    /// Index for tag values
    TagValues,
}

impl IndexType {
    /// Get the string representation for use as index key
    pub fn as_str(&self) -> &'static str {
        match self {
            IndexType::MetricNames => "metric_names",
            IndexType::TagNames => "tag_names",
            IndexType::TagValues => "tag_values",
        }
    }

    /// Get the index key bytes for Cassandra
    pub fn key_bytes(&self) -> Vec<u8> {
        self.as_str().as_bytes().to_vec()
    }
}

/// Row key index entry for efficient querying
#[derive(Debug, Clone)]
pub struct RowKeyIndexEntry {
    /// The metric name
    pub metric_name: String,
    /// The data type
    pub data_type: String,
    /// The row time timestamp
    pub row_time: i64,
    /// The tags in Cassandra format
    pub tags: String,
}

impl RowKeyIndexEntry {
    /// Create from a RowKey
    pub fn from_row_key(row_key: &crate::cassandra::RowKey) -> Self {
        Self {
            metric_name: row_key.metric.as_str().to_string(),
            data_type: row_key.data_type.clone(),
            row_time: row_key.row_time.timestamp_millis(),
            tags: row_key.tags.clone(),
        }
    }

    /// Create the index key for this entry
    pub fn index_key(&self) -> Vec<u8> {
        // Use metric name + data type as the partition key
        format!("{}:{}", self.metric_name, self.data_type).into_bytes()
    }

    /// Get the key as bytes for Cassandra
    pub fn key(&self) -> IndexKey {
        IndexKey {
            bytes: self.index_key(),
        }
    }

    /// Get the column as bytes for Cassandra
    pub fn column(&self) -> IndexColumn {
        IndexColumn {
            bytes: self.index_column(),
        }
    }

    /// Get the value as bytes for Cassandra
    pub fn value(&self) -> IndexValue {
        IndexValue {
            bytes: self.index_value(),
        }
    }

    /// Create the index column for this entry
    pub fn index_column(&self) -> Vec<u8> {
        // Use row time + tags as the clustering column
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.row_time.to_be_bytes());
        bytes.extend_from_slice(self.tags.as_bytes());
        bytes
    }

    /// Create the index value for this entry
    pub fn index_value(&self) -> Vec<u8> {
        // The value is the actual row key bytes
        let mut bytes = Vec::new();

        // Encode the full row key
        let metric_bytes = self.metric_name.as_bytes();
        bytes.extend_from_slice(&(metric_bytes.len() as u32).to_be_bytes());
        bytes.extend_from_slice(metric_bytes);

        let type_bytes = self.data_type.as_bytes();
        bytes.extend_from_slice(&(type_bytes.len() as u32).to_be_bytes());
        bytes.extend_from_slice(type_bytes);

        bytes.extend_from_slice(&self.row_time.to_be_bytes());

        let tags_bytes = self.tags.as_bytes();
        bytes.extend_from_slice(&(tags_bytes.len() as u32).to_be_bytes());
        bytes.extend_from_slice(tags_bytes);

        bytes
    }
}

/// String index entry for discovery queries
#[derive(Debug, Clone)]
pub struct StringIndexEntry {
    /// The index type
    pub index_type: IndexType,
    /// The string value being indexed
    pub value: String,
    /// Associated metadata (e.g., metric name for tag indices)
    pub metadata: Option<String>,
}

impl StringIndexEntry {
    /// Create a metric name index entry
    pub fn metric_name(name: &str) -> Self {
        Self {
            index_type: IndexType::MetricNames,
            value: name.to_string(),
            metadata: None,
        }
    }

    /// Create a tag name index entry
    pub fn tag_name(name: &str) -> Self {
        Self {
            index_type: IndexType::TagNames,
            value: name.to_string(),
            metadata: None,
        }
    }

    /// Create a tag value index entry
    pub fn tag_value(value: &str) -> Self {
        Self {
            index_type: IndexType::TagValues,
            value: value.to_string(),
            metadata: None,
        }
    }

    /// Create the index key for this entry
    pub fn index_key(&self) -> Vec<u8> {
        self.index_type.key_bytes()
    }

    /// Get the key as bytes for Cassandra
    pub fn key(&self) -> IndexKey {
        IndexKey {
            bytes: self.index_key(),
        }
    }

    /// Create the index column for this entry
    pub fn index_column(&self) -> String {
        match &self.metadata {
            Some(meta) => format!("{}:{}", meta, self.value),
            None => self.value.clone(),
        }
    }

    /// Create the index value for this entry (empty for most cases)
    pub fn index_value(&self) -> Vec<u8> {
        Vec::new() // Empty value, presence indicates existence
    }
}

/// Schema migration utilities
pub struct SchemaMigrator {
    schema: KairosSchema,
}

impl SchemaMigrator {
    /// Create a new schema migrator
    pub fn new(schema: KairosSchema) -> Self {
        Self { schema }
    }

    /// Check if the schema needs to be created or updated
    pub fn needs_migration(&self) -> bool {
        // In a real implementation, this would check the current schema version
        // against the expected version
        true // For now, always assume migration is needed
    }

    /// Get the migration steps required
    pub fn migration_steps(&self) -> Vec<String> {
        self.schema.create_schema_cql()
    }

    /// Validate the migration steps
    pub fn validate_migration(&self) -> KairosResult<()> {
        self.schema.validate()?;

        // Additional validation for migration
        let steps = self.migration_steps();
        if steps.is_empty() {
            return Err(KairosError::schema("No migration steps generated"));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let schema = KairosSchema::default();
        assert!(schema.validate().is_ok());

        let cql_statements = schema.create_schema_cql();
        assert_eq!(cql_statements.len(), 4); // keyspace + 3 tables

        // Check keyspace creation
        assert!(cql_statements[0].contains("CREATE KEYSPACE"));
        assert!(cql_statements[0].contains("kairosdb"));
    }

    #[test]
    fn test_schema_validation() {
        let mut schema = KairosSchema::default();
        assert!(schema.validate().is_ok());

        // Test empty keyspace
        schema.keyspace = String::new();
        assert!(schema.validate().is_err());

        // Test zero replication factor
        schema.keyspace = "test".to_string();
        schema.replication_factor = 0;
        assert!(schema.validate().is_err());
    }

    #[test]
    fn test_index_types() {
        assert_eq!(IndexType::MetricNames.as_str(), "metric_names");
        assert_eq!(IndexType::TagNames.as_str(), "tag_names");
        assert_eq!(IndexType::TagValues.as_str(), "tag_values");
    }

    #[test]
    fn test_row_key_index_entry() {
        let entry = RowKeyIndexEntry {
            metric_name: "test.metric".to_string(),
            data_type: "kairos_long".to_string(),
            row_time: 1234567890000,
            tags: "host=server1".to_string(),
        };

        let key = entry.index_key();
        let column = entry.index_column();
        let value = entry.index_value();

        assert!(!key.is_empty());
        assert!(!column.is_empty());
        assert!(!value.is_empty());
    }

    #[test]
    fn test_string_index_entry() {
        let entry = StringIndexEntry {
            index_type: IndexType::MetricNames,
            value: "test.metric".to_string(),
            metadata: None,
        };

        let key = entry.index_key();
        let column = entry.index_column();

        assert_eq!(key, IndexType::MetricNames.key_bytes());
        assert_eq!(column, "test.metric");
    }

    #[test]
    fn test_schema_migrator() {
        let schema = KairosSchema::default();
        let migrator = SchemaMigrator::new(schema);

        assert!(migrator.needs_migration());
        assert!(migrator.validate_migration().is_ok());

        let steps = migrator.migration_steps();
        assert!(!steps.is_empty());
    }
}
