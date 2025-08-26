//! Bloom filter manager for index deduplication
//!
//! Manages dual bloom filters with rotation to track which indexes have
//! already been written, avoiding redundant writes to Cassandra.

use parking_lot::RwLock;
use probabilistic_collections::bloom::ScalableBloomFilter;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, trace};

/// Configuration for bloom filter behavior
#[derive(Debug, Clone)]
pub struct BloomConfig {
    /// Expected number of items (metric names, tag names, tag values)
    pub expected_items: u64,
    /// Target false positive rate (1/1,000,000 = 0.000001)
    pub false_positive_rate: f64,
    /// Filter rotation interval (30 minutes)
    pub rotation_interval: Duration,
    /// Overlap period where both filters are active (10 minutes)
    pub overlap_period: Duration,
}

impl Default for BloomConfig {
    fn default() -> Self {
        Self {
            expected_items: 1_000_000,                       // 1M unique index entries
            false_positive_rate: 0.000001,                   // 1 in 1 million
            rotation_interval: Duration::from_secs(30 * 60), // 30 minutes
            overlap_period: Duration::from_secs(10 * 60),    // 10 minutes
        }
    }
}

/// Dual bloom filter state for rotation
struct BloomState {
    /// Primary bloom filter (currently active)
    primary: ScalableBloomFilter<String>,
    /// Secondary bloom filter (used during overlap period)
    secondary: Option<ScalableBloomFilter<String>>,
    /// When the primary filter was created
    primary_created: Instant,
    /// When the secondary filter was created (if any)
    secondary_created: Option<Instant>,
    /// Current hash seed for primary filter
    primary_seed: u64,
    /// Current hash seed for secondary filter
    secondary_seed: u64,
    /// Configuration
    config: BloomConfig,
}

impl std::fmt::Debug for BloomState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomState")
            .field("primary_created", &self.primary_created)
            .field("secondary_created", &self.secondary_created)
            .field("primary_seed", &self.primary_seed)
            .field("secondary_seed", &self.secondary_seed)
            .field("config", &self.config)
            .field("has_secondary", &self.secondary.is_some())
            .finish()
    }
}

impl BloomState {
    fn new(config: BloomConfig) -> Self {
        let seed = generate_random_seed();
        let primary = create_bloom_filter(&config, seed);

        Self {
            primary,
            secondary: None,
            primary_created: Instant::now(),
            secondary_created: None,
            primary_seed: seed,
            secondary_seed: 0,
            config,
        }
    }

    /// Check if we need to start the overlap period (create secondary filter)
    fn should_start_overlap(&self) -> bool {
        let elapsed = self.primary_created.elapsed();
        let overlap_start = self.config.rotation_interval - self.config.overlap_period;

        elapsed >= overlap_start && self.secondary.is_none()
    }

    /// Check if we need to rotate (make secondary primary)
    fn should_rotate(&self) -> bool {
        let elapsed = self.primary_created.elapsed();
        elapsed >= self.config.rotation_interval && self.secondary.is_some()
    }

    /// Start the overlap period by creating secondary filter
    fn start_overlap(&mut self) {
        if self.secondary.is_none() {
            let seed = generate_random_seed();
            self.secondary = Some(create_bloom_filter(&self.config, seed));
            self.secondary_created = Some(Instant::now());
            self.secondary_seed = seed;

            debug!(
                "Started bloom filter overlap period. Primary seed: {}, Secondary seed: {}",
                self.primary_seed, self.secondary_seed
            );
        }
    }

    /// Rotate filters (secondary becomes primary)
    fn rotate(&mut self) {
        if let Some(secondary) = self.secondary.take() {
            self.primary = secondary;
            self.primary_created = self.secondary_created.unwrap();
            self.primary_seed = self.secondary_seed;
            self.secondary = None;
            self.secondary_created = None;

            debug!(
                "Rotated bloom filters. New primary seed: {}",
                self.primary_seed
            );
        }
    }

    /// Check if an item exists in any active filter
    fn contains(&self, item: &str) -> bool {
        let primary_contains = self.primary.contains(item);

        // During overlap period, check both filters
        if let Some(ref secondary) = self.secondary {
            primary_contains || secondary.contains(item)
        } else {
            primary_contains
        }
    }

    /// Insert an item into all active filters
    fn insert(&mut self, item: &str) {
        self.primary.insert(item);

        // During overlap period, insert into both filters
        if let Some(ref mut secondary) = self.secondary {
            secondary.insert(item);
        }
    }
}

/// Thread-safe bloom filter manager for index deduplication
#[derive(Clone)]
pub struct BloomManager {
    state: Arc<RwLock<BloomState>>,
}

impl BloomManager {
    /// Create a new bloom filter manager with default configuration
    pub fn new() -> Self {
        Self::with_config(BloomConfig::default())
    }

    /// Create a new bloom filter manager with custom configuration
    pub fn with_config(config: BloomConfig) -> Self {
        let state = BloomState::new(config);
        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Check if an index entry should be written (not in bloom filter)
    /// Returns true if the item should be written to Cassandra
    pub fn should_write_index(&self, index_key: &str) -> bool {
        // First, perform maintenance if needed
        self.maybe_maintain();

        // Check if item exists in any active filter
        let exists = {
            let state = self.state.read();
            state.contains(index_key)
        };

        if exists {
            trace!(
                "Index entry found in bloom filter, skipping write: {}",
                index_key
            );
            false // Don't write, probably already exists
        } else {
            // Insert into active filters and return true to write
            {
                let mut state = self.state.write();
                state.insert(index_key);
            }
            trace!("Index entry not in bloom filter, will write: {}", index_key);
            true // Write to Cassandra
        }
    }

    /// Perform maintenance: check for overlap start or rotation
    fn maybe_maintain(&self) {
        let mut state = self.state.write();

        if state.should_start_overlap() {
            state.start_overlap();
        } else if state.should_rotate() {
            state.rotate();
        }
    }

    /// Get basic statistics about the bloom filters (without expensive calculations)
    pub fn get_stats(&self) -> BloomStats {
        self.get_stats_with_options(false)
    }

    /// Get detailed statistics about the bloom filters with optional expensive calculations
    pub fn get_stats_with_options(&self, include_ones_count: bool) -> BloomStats {
        let state = self.state.read();
        let primary_age = state.primary_created.elapsed();
        let secondary_age = state.secondary_created.map(|created| created.elapsed());

        // Calculate memory usage for bloom filters
        let primary_memory_bytes = calculate_bloom_memory_bytes(&state.primary);
        let secondary_memory_bytes = if let Some(ref secondary) = state.secondary {
            Some(calculate_bloom_memory_bytes(secondary))
        } else {
            None
        };
        let total_memory_bytes = primary_memory_bytes + secondary_memory_bytes.unwrap_or(0);

        // Calculate ones count only if requested (expensive operation)
        let (primary_ones_count, secondary_ones_count) = if include_ones_count {
            let primary_ones = Some(state.primary.count_ones() as u64);
            let secondary_ones = if let Some(ref secondary) = state.secondary {
                Some(secondary.count_ones() as u64)
            } else {
                None
            };
            (primary_ones, secondary_ones)
        } else {
            (None, None)
        };

        BloomStats {
            primary_age_seconds: primary_age.as_secs(),
            secondary_age_seconds: secondary_age.map(|age| age.as_secs()),
            in_overlap_period: state.secondary.is_some(),
            primary_seed: state.primary_seed,
            secondary_seed: state.secondary_seed,
            expected_items: state.config.expected_items,
            false_positive_rate: state.config.false_positive_rate,
            primary_memory_bytes,
            secondary_memory_bytes,
            total_memory_bytes,
            primary_ones_count,
            secondary_ones_count,
        }
    }
}

impl Default for BloomManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about bloom filter state
#[derive(Debug, Clone)]
pub struct BloomStats {
    pub primary_age_seconds: u64,
    pub secondary_age_seconds: Option<u64>,
    pub in_overlap_period: bool,
    pub primary_seed: u64,
    pub secondary_seed: u64,
    pub expected_items: u64,
    pub false_positive_rate: f64,
    /// Estimated memory usage in bytes for the primary bloom filter
    pub primary_memory_bytes: u64,
    /// Estimated memory usage in bytes for the secondary bloom filter (if active)
    pub secondary_memory_bytes: Option<u64>,
    /// Total estimated memory usage in bytes
    pub total_memory_bytes: u64,
    /// Number of set bits in primary bloom filter (expensive to calculate)
    pub primary_ones_count: Option<u64>,
    /// Number of set bits in secondary bloom filter (expensive to calculate)
    pub secondary_ones_count: Option<u64>,
}

/// Create a bloom filter with specific configuration and hash seed
fn create_bloom_filter(config: &BloomConfig, _seed: u64) -> ScalableBloomFilter<String> {
    // Create bloom filter with calculated optimal size and hash count
    // Note: Using default hasher, seed is used for future rotation logic
    const INITIAL_BITS: usize = (1 << 10) * 8 * 5; // Start with 5MB bit array
                                                   // const INITIAL_BITS: usize = (1 << 20) * 8 * 5; // Start with 5MB bit array
    ScalableBloomFilter::new(INITIAL_BITS, config.false_positive_rate, 1.2, 1.0)
}

/// Calculate actual memory usage of a bloom filter using the len() method
/// This gets the real number of bits in the bloom filter
fn calculate_bloom_memory_bytes(bloom_filter: &ScalableBloomFilter<String>) -> u64 {
    use std::mem;

    // Get the size of the BloomFilter struct itself (stack allocation)
    let struct_size = mem::size_of_val(bloom_filter) as u64;

    // Get the actual number of bits in the bloom filter
    let num_bits = bloom_filter.len() as u64;

    // Convert bits to bytes (round up to nearest byte)
    let bit_vector_bytes = (num_bits + 7) / 8; // Equivalent to ceiling division

    // Total memory = struct size + bit vector + some overhead for Vec metadata
    struct_size + bit_vector_bytes + 64
}

/// Generate a random seed for hash functions
fn generate_random_seed() -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};

    let mut hasher = DefaultHasher::new();
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_bloom_manager_basic() {
        let manager = BloomManager::new();

        // First time should return true (not in filter)
        assert!(manager.should_write_index("metric.test"));

        // Second time should return false (now in filter)
        assert!(!manager.should_write_index("metric.test"));
    }

    #[test]
    fn test_bloom_manager_rotation() {
        let config = BloomConfig {
            rotation_interval: Duration::from_millis(100),
            overlap_period: Duration::from_millis(50),
            ..Default::default()
        };

        let manager = BloomManager::with_config(config);

        // Add item to primary
        assert!(manager.should_write_index("test.metric"));

        // Wait for overlap period to start
        sleep(Duration::from_millis(60));
        manager.maybe_maintain(); // Trigger maintenance to start overlap
        let stats = manager.get_stats();
        assert!(stats.in_overlap_period);

        // Wait for full rotation
        sleep(Duration::from_millis(60));
        manager.maybe_maintain(); // Trigger maintenance to complete rotation
        let stats = manager.get_stats();
        assert!(!stats.in_overlap_period);
    }

    #[test]
    fn test_different_seeds() {
        let config = BloomConfig {
            rotation_interval: Duration::from_millis(100),
            overlap_period: Duration::from_millis(50),
            ..Default::default()
        };

        let manager = BloomManager::with_config(config);
        let initial_stats = manager.get_stats();

        // Trigger rotation
        sleep(Duration::from_millis(60));
        manager.maybe_maintain(); // Start overlap

        sleep(Duration::from_millis(60));
        manager.maybe_maintain(); // Complete rotation

        let final_stats = manager.get_stats();

        // Seeds should be different after rotation
        assert_ne!(initial_stats.primary_seed, final_stats.primary_seed);
    }

    #[test]
    fn test_bloom_memory_calculation() {
        let manager = BloomManager::new();
        let stats = manager.get_stats();

        // Check that memory calculations are reasonable
        assert!(stats.primary_memory_bytes > 0);
        assert!(stats.total_memory_bytes > 0);
        assert_eq!(stats.total_memory_bytes, stats.primary_memory_bytes);
        assert!(stats.secondary_memory_bytes.is_none());

        // Memory should be reasonable for a scalable bloom filter starting at 100k bits
        assert!(stats.primary_memory_bytes < 10_000_000); // Less than 10MB
    }

    #[test]
    fn test_bloom_memory_during_overlap() {
        let config = BloomConfig {
            rotation_interval: Duration::from_millis(100),
            overlap_period: Duration::from_millis(50),
            expected_items: 1000, // Smaller for testing
            false_positive_rate: 0.01,
        };

        let manager = BloomManager::with_config(config);
        let initial_stats = manager.get_stats();

        // Initially only primary filter
        assert!(initial_stats.secondary_memory_bytes.is_none());
        let initial_memory = initial_stats.total_memory_bytes;

        // Add item to trigger maintenance and start overlap
        assert!(manager.should_write_index("test.metric"));
        sleep(Duration::from_millis(60));
        manager.maybe_maintain();

        let overlap_stats = manager.get_stats();
        assert!(overlap_stats.in_overlap_period);
        assert!(overlap_stats.secondary_memory_bytes.is_some());

        // Total memory should be roughly double during overlap
        assert!(overlap_stats.total_memory_bytes > initial_memory);
        assert_eq!(
            overlap_stats.total_memory_bytes,
            overlap_stats.primary_memory_bytes + overlap_stats.secondary_memory_bytes.unwrap()
        );
    }

    #[test]
    fn test_bloom_ones_count() {
        let manager = BloomManager::new();

        // Get stats without ones count (fast)
        let stats_no_ones = manager.get_stats();
        assert!(stats_no_ones.primary_ones_count.is_none());
        assert!(stats_no_ones.secondary_ones_count.is_none());

        // Get stats with ones count (expensive)
        let stats_with_ones = manager.get_stats_with_options(true);
        assert!(stats_with_ones.primary_ones_count.is_some());
        assert!(stats_with_ones.secondary_ones_count.is_none()); // No secondary initially

        // Add some items to set bits in the bloom filter
        assert!(manager.should_write_index("test.metric.1"));
        assert!(manager.should_write_index("test.metric.2"));
        assert!(manager.should_write_index("test.metric.3"));

        let stats_after_adds = manager.get_stats_with_options(true);
        let ones_count = stats_after_adds.primary_ones_count.unwrap();

        // Should have some bits set after adding items
        assert!(ones_count > 0);
        // Should be reasonable number for a few items
        assert!(ones_count < 1000); // Shouldn't be too high for just 3 items
    }
}
