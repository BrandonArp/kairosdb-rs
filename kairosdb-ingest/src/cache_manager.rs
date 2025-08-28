//! Cache manager for index deduplication using hybrid cache
//!
//! Manages dual foyer hybrid caches (in-memory + disk) with rotation to track which indexes have
//! already been written, avoiding redundant writes to Cassandra. Unlike bloom filters, this
//! provides precise tracking without false positives.

use foyer::{Cache, CacheBuilder};
use parking_lot::RwLock;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, trace};

use crate::cache_metrics::CacheMetrics;
use crate::config::CacheConfig;

/// Cache value type - just a marker that the index exists
type CacheValue = bool;

/// Dual cache state for rotation
struct CacheState {
    /// Primary cache (currently active)
    primary: Cache<String, CacheValue>,
    /// Secondary cache (used during overlap period) 
    secondary: Option<Cache<String, CacheValue>>,
    /// When the primary cache was created
    primary_created: Instant,
    /// When the secondary cache was created (if any)
    secondary_created: Option<Instant>,
    /// Current generation for primary cache (for directory naming)
    primary_generation: u64,
    /// Current generation for secondary cache
    secondary_generation: u64,
    /// Configuration
    config: CacheConfig,
}

impl std::fmt::Debug for CacheState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheState")
            .field("primary_created", &self.primary_created)
            .field("secondary_created", &self.secondary_created)
            .field("primary_generation", &self.primary_generation)
            .field("secondary_generation", &self.secondary_generation)
            .field("config", &self.config)
            .field("has_secondary", &self.secondary.is_some())
            .finish()
    }
}

impl CacheState {
    async fn new(config: CacheConfig) -> Result<Self, CacheError> {
        let generation = generate_generation();
        let primary = create_hybrid_cache(&config, generation).await?;

        Ok(Self {
            primary,
            secondary: None,
            primary_created: Instant::now(),
            secondary_created: None,
            primary_generation: generation,
            secondary_generation: 0,
            config,
        })
    }

    /// Check if we need to start the overlap period (create secondary cache)
    fn should_start_overlap(&self) -> bool {
        let elapsed = self.primary_created.elapsed();
        let rotation_interval = Duration::from_secs(self.config.rotation_interval_seconds);
        let overlap_period = Duration::from_secs(self.config.overlap_period_seconds);
        let overlap_start = rotation_interval - overlap_period;

        elapsed >= overlap_start && self.secondary.is_none()
    }

    /// Check if we need to rotate (make secondary primary)
    fn should_rotate(&self) -> bool {
        let elapsed = self.primary_created.elapsed();
        let rotation_interval = Duration::from_secs(self.config.rotation_interval_seconds);
        elapsed >= rotation_interval && self.secondary.is_some()
    }

    /// Start the overlap period by creating secondary cache
    async fn start_overlap(&mut self) -> Result<(), CacheError> {
        if self.secondary.is_none() {
            let generation = generate_generation();
            self.secondary = Some(create_hybrid_cache(&self.config, generation).await?);
            self.secondary_created = Some(Instant::now());
            self.secondary_generation = generation;

            debug!(
                "Started cache overlap period. Primary generation: {}, Secondary generation: {}",
                self.primary_generation, self.secondary_generation
            );
        }
        Ok(())
    }

    /// Rotate caches (secondary becomes primary)
    fn rotate(&mut self) {
        if let Some(secondary) = self.secondary.take() {
            // Clean up old primary cache directory
            let old_generation = self.primary_generation;
            
            self.primary = secondary;
            self.primary_created = self.secondary_created.unwrap();
            self.primary_generation = self.secondary_generation;
            self.secondary = None;
            self.secondary_created = None;

            debug!(
                "Rotated caches. New primary generation: {}, cleaning up generation: {}",
                self.primary_generation, old_generation
            );
            
            // TODO: Schedule cleanup of old cache directory when we add disk support
            debug!("Would cleanup cache directory for generation: {}", old_generation);
        }
    }

    /// Check if an item exists in any active cache
    fn contains(&self, item: &str) -> bool {
        let primary_contains = self.primary.contains(item);

        // During overlap period, check both caches
        if let Some(ref secondary) = self.secondary {
            primary_contains || secondary.contains(item)
        } else {
            primary_contains
        }
    }

    /// Insert an item into all active caches
    fn insert(&self, item: &str) {
        // Insert into primary cache
        self.primary.insert(item.to_string(), true);

        // During overlap period, insert into both caches
        if let Some(ref secondary) = self.secondary {
            secondary.insert(item.to_string(), true);
        }
    }
}

/// Thread-safe cache manager for index deduplication
#[derive(Clone)]
pub struct CacheManager {
    state: Arc<RwLock<Option<CacheState>>>,
    config: CacheConfig,
    metrics: Arc<CacheMetrics>,
}

impl CacheManager {
    /// Create a new cache manager with default configuration
    pub async fn new() -> Result<Self, CacheError> {
        Self::with_config(CacheConfig::default()).await
    }

    /// Create a new cache manager with custom configuration
    pub async fn with_config(config: CacheConfig) -> Result<Self, CacheError> {
        // Ensure cache directory exists
        tokio::fs::create_dir_all(&config.disk_cache_dir).await
            .map_err(|e| CacheError::DirectoryCreation { 
                path: config.disk_cache_dir.clone(), 
                error: e.to_string() 
            })?;
        
        let metrics = Arc::new(CacheMetrics::new().map_err(|e| CacheError::CacheBuild(e.to_string()))?);
        let state = CacheState::new(config.clone()).await?;
        
        // Record initial cache generation
        metrics.record_cache_generation();
        
        Ok(Self {
            state: Arc::new(RwLock::new(Some(state))),
            config,
            metrics,
        })
    }

    /// Check if an index entry should be written (not in cache)
    /// Returns true if the item should be written to Cassandra
    pub async fn should_write_index(&self, index_key: &str) -> bool {
        let start_time = Instant::now();
        
        // First, perform maintenance if needed
        if let Err(e) = self.maybe_maintain().await {
            debug!("Cache maintenance failed: {}", e);
            // If maintenance fails, be conservative and write the index
            return true;
        }

        // Check if item exists in any active cache
        let exists = {
            let state_guard = self.state.read();
            if let Some(state) = state_guard.as_ref() {
                state.contains(index_key)
            } else {
                false // If no state, write the index
            }
        };

        if exists {
            trace!("Index entry found in cache, skipping write: {}", index_key);
            self.metrics.record_cache_hit();
            self.metrics.record_cache_operation_duration(start_time.elapsed());
            false // Don't write, already exists in cache
        } else {
            // Insert into active caches and return true to write
            {
                let state_guard = self.state.read();
                if let Some(state) = state_guard.as_ref() {
                    state.insert(index_key);
                }
            }
            trace!("Index entry not in cache, will write: {}", index_key);
            self.metrics.record_cache_miss();
            self.metrics.record_cache_operation_duration(start_time.elapsed());
            true // Write to Cassandra
        }
    }

    /// Perform maintenance: check for overlap start or rotation
    async fn maybe_maintain(&self) -> Result<(), CacheError> {
        let start_time = Instant::now();
        let mut needs_maintenance = false;
        let should_start_overlap;
        let should_rotate;
        
        // Check what maintenance is needed
        {
            let state_guard = self.state.read();
            if let Some(state) = state_guard.as_ref() {
                should_start_overlap = state.should_start_overlap();
                should_rotate = state.should_rotate();
                needs_maintenance = should_start_overlap || should_rotate;
            } else {
                return Ok(());
            }
        }

        if needs_maintenance {
            // Get write lock for maintenance
            // Handle start overlap case - need to avoid holding lock across await
            if should_start_overlap {
                // Clone the necessary data, drop the lock, then await
                let config = {
                    let state_guard = self.state.read();
                    state_guard.as_ref().map(|s| s.config.clone())
                };
                
                if let Some(config) = config {
                    let generation = generate_generation();
                    let new_cache = create_hybrid_cache(&config, generation).await?;
                    
                    // Reacquire write lock for minimal time
                    let mut state_guard = self.state.write();
                    if let Some(state) = state_guard.as_mut() {
                        state.secondary = Some(new_cache);
                        state.secondary_created = Some(Instant::now());
                        state.secondary_generation = generation;
                        
                        debug!(
                            "Started cache overlap period. Primary generation: {}, Secondary generation: {}",
                            state.primary_generation, state.secondary_generation
                        );
                    }
                    self.metrics.record_overlap_period_start();
                }
            } else if should_rotate {
                let mut state_guard = self.state.write();
                if let Some(state) = state_guard.as_mut() {
                    state.rotate();
                    self.metrics.record_cache_rotation();
                    self.metrics.record_cache_generation();
                }
            }
            self.metrics.record_maintenance_duration(start_time.elapsed());
        }

        Ok(())
    }

    /// Get statistics about the caches
    pub async fn get_stats(&self) -> CacheStats {
        let state_guard = self.state.read();
        let stats = if let Some(state) = state_guard.as_ref() {
            let primary_age = state.primary_created.elapsed();
            let secondary_age = state.secondary_created.map(|created| created.elapsed());

            CacheStats {
                primary_age_seconds: primary_age.as_secs(),
                secondary_age_seconds: secondary_age.map(|age| age.as_secs()),
                in_overlap_period: state.secondary.is_some(),
                primary_generation: state.primary_generation,
                secondary_generation: state.secondary_generation,
                memory_capacity: state.config.memory_capacity,
                disk_capacity: state.config.disk_capacity,
                primary_memory_usage: 0, // TODO: Implement when foyer stats API is available
                primary_disk_usage: 0,
                secondary_memory_usage: None,
                secondary_disk_usage: None,
                primary_hit_ratio: 0.0,
                secondary_hit_ratio: None,
            }
        } else {
            CacheStats::empty()
        };
        
        // Update Prometheus metrics
        self.metrics.update_from_stats(&stats);
        
        stats
    }
}

/// Statistics about cache state
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub primary_age_seconds: u64,
    pub secondary_age_seconds: Option<u64>,
    pub in_overlap_period: bool,
    pub primary_generation: u64,
    pub secondary_generation: u64,
    pub memory_capacity: u64,
    pub disk_capacity: u64,
    pub primary_memory_usage: u64,
    pub primary_disk_usage: u64,
    pub secondary_memory_usage: Option<u64>,
    pub secondary_disk_usage: Option<u64>,
    pub primary_hit_ratio: f64,
    pub secondary_hit_ratio: Option<f64>,
}

impl CacheStats {
    fn empty() -> Self {
        Self {
            primary_age_seconds: 0,
            secondary_age_seconds: None,
            in_overlap_period: false,
            primary_generation: 0,
            secondary_generation: 0,
            memory_capacity: 0,
            disk_capacity: 0,
            primary_memory_usage: 0,
            primary_disk_usage: 0,
            secondary_memory_usage: None,
            secondary_disk_usage: None,
            primary_hit_ratio: 0.0,
            secondary_hit_ratio: None,
        }
    }

    pub fn total_memory_usage(&self) -> u64 {
        self.primary_memory_usage + self.secondary_memory_usage.unwrap_or(0)
    }

    pub fn total_disk_usage(&self) -> u64 {
        self.primary_disk_usage + self.secondary_disk_usage.unwrap_or(0)
    }
}

/// Error types for cache operations
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("Failed to create cache directory {path}: {error}")]
    DirectoryCreation { path: String, error: String },
    
    #[error("Failed to build hybrid cache: {0}")]
    CacheBuild(String),

    #[error("Cache operation failed: {0}")]
    CacheOperation(String),
}

/// Create a hybrid cache with specific configuration and generation
async fn create_hybrid_cache(
    config: &CacheConfig,
    generation: u64,
) -> Result<Cache<String, CacheValue>, CacheError> {
    let cache_dir = format!("{}/cache_{}", config.disk_cache_dir, generation);
    
    // Ensure the specific generation directory exists
    tokio::fs::create_dir_all(&cache_dir).await
        .map_err(|e| CacheError::DirectoryCreation { 
            path: cache_dir.clone(), 
            error: e.to_string() 
        })?;

    info!("Creating hybrid cache (memory-only for now) with generation: {}", generation);

    // For now, use memory-only cache (can add disk later when API is clearer)
    Ok(CacheBuilder::new(config.memory_capacity as usize).build())
}

/// Generate a unique generation number for cache directories
fn generate_generation() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

// TODO: Cleanup old cache directory when we add disk support
#[allow(dead_code)]
async fn cleanup_cache_directory(path: &str) -> Result<(), std::io::Error> {
    if Path::new(path).exists() {
        tokio::fs::remove_dir_all(path).await?;
        debug!("Cleaned up cache directory: {}", path);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use tempfile::TempDir;

    async fn test_cache_manager() -> CacheManager {
        let temp_dir = TempDir::new().unwrap();
        let config = CacheConfig {
            memory_capacity: 1024 * 1024, // 1MB
            disk_capacity: 10 * 1024 * 1024, // 10MB
            disk_cache_dir: temp_dir.path().to_string_lossy().to_string(),
            rotation_interval: Duration::from_millis(100),
            overlap_period: Duration::from_millis(50),
        };

        CacheManager::with_config(config).await.unwrap()
    }

    #[tokio::test]
    async fn test_cache_manager_basic() {
        let manager = test_cache_manager().await;

        // First time should return true (not in cache)
        assert!(manager.should_write_index("metric.test").await);

        // Second time should return false (now in cache)
        assert!(!manager.should_write_index("metric.test").await);
    }

    #[tokio::test]
    async fn test_cache_manager_rotation() {
        let manager = test_cache_manager().await;

        // Add item to primary
        assert!(manager.should_write_index("test.metric").await);

        // Wait for overlap period to start
        tokio::time::sleep(Duration::from_millis(60)).await;
        let _ = manager.maybe_maintain().await; // Trigger maintenance to start overlap
        let stats = manager.get_stats().await;
        assert!(stats.in_overlap_period);

        // Wait for full rotation
        tokio::time::sleep(Duration::from_millis(60)).await;
        let _ = manager.maybe_maintain().await; // Trigger maintenance to complete rotation
        let stats = manager.get_stats().await;
        assert!(!stats.in_overlap_period);
    }

    #[tokio::test]
    async fn test_different_generations() {
        let manager = test_cache_manager().await;
        let initial_stats = manager.get_stats().await;

        // Trigger rotation
        tokio::time::sleep(Duration::from_millis(60)).await;
        let _ = manager.maybe_maintain().await; // Start overlap

        tokio::time::sleep(Duration::from_millis(60)).await;
        let _ = manager.maybe_maintain().await; // Complete rotation

        let final_stats = manager.get_stats().await;

        // Generations should be different after rotation
        assert_ne!(initial_stats.primary_generation, final_stats.primary_generation);
    }

    #[tokio::test]
    async fn test_cache_stats() {
        let manager = test_cache_manager().await;
        let stats = manager.get_stats().await;

        // Check that stats are reasonable
        assert!(stats.memory_capacity > 0);
        assert!(stats.disk_capacity > 0);
        assert_eq!(stats.total_memory_usage(), stats.primary_memory_usage);
        assert_eq!(stats.total_disk_usage(), stats.primary_disk_usage);
        assert!(stats.secondary_memory_usage.is_none());

        // Memory should be reasonable
        assert!(stats.memory_capacity == 1024 * 1024);
        assert!(stats.disk_capacity == 10 * 1024 * 1024);
    }

    #[tokio::test]
    async fn test_cache_stats_during_overlap() {
        let manager = test_cache_manager().await;
        let initial_stats = manager.get_stats().await;

        // Initially only primary cache
        assert!(initial_stats.secondary_memory_usage.is_none());
        let initial_memory = initial_stats.total_memory_usage();

        // Add item to trigger maintenance and start overlap
        assert!(manager.should_write_index("test.metric").await);
        tokio::time::sleep(Duration::from_millis(60)).await;
        let _ = manager.maybe_maintain().await;

        let overlap_stats = manager.get_stats().await;
        assert!(overlap_stats.in_overlap_period);
        assert!(overlap_stats.secondary_memory_usage.is_some());

        // Total memory should be higher during overlap
        assert!(overlap_stats.total_memory_usage() >= initial_memory);
        assert_eq!(
            overlap_stats.total_memory_usage(),
            overlap_stats.primary_memory_usage + overlap_stats.secondary_memory_usage.unwrap()
        );
    }
}