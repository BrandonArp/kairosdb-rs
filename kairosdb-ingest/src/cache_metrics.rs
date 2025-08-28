//! Cache metrics for monitoring hybrid cache performance
//!
//! Provides comprehensive Prometheus metrics for the foyer hybrid cache
//! including memory usage, disk usage, hit/miss ratios, and rotation events.

use crate::cache_manager::CacheStats;
use prometheus::{register_counter, register_gauge, register_histogram, Counter, Gauge, Histogram};

/// Comprehensive cache metrics for monitoring performance
#[derive(Debug, Clone)]
pub struct CacheMetrics {
    // Memory usage metrics
    pub memory_usage_bytes: Gauge,
    pub memory_capacity_bytes: Gauge,
    pub memory_utilization_ratio: Gauge,

    // Disk usage metrics
    pub disk_usage_bytes: Gauge,
    pub disk_capacity_bytes: Gauge,
    pub disk_utilization_ratio: Gauge,

    // Total cache usage
    pub total_memory_usage_bytes: Gauge,
    pub total_disk_usage_bytes: Gauge,

    // Cache performance metrics
    pub hit_ratio_primary: Gauge,
    pub hit_ratio_secondary: Gauge,
    pub hit_ratio_total: Gauge,

    // Cache operations
    pub cache_hits_total: Counter,
    pub cache_misses_total: Counter,
    pub cache_inserts_total: Counter,
    pub cache_evictions_total: Counter,

    // Cache lifecycle metrics
    pub rotation_events_total: Counter,
    pub overlap_periods_total: Counter,
    pub cache_generations_total: Counter,

    // Cache age metrics
    pub primary_cache_age_seconds: Gauge,
    pub secondary_cache_age_seconds: Gauge,
    pub overlap_period_active: Gauge,

    // Cache operation latency
    pub cache_operation_duration: Histogram,
    pub cache_maintenance_duration: Histogram,
}

impl CacheMetrics {
    /// Create new cache metrics with Prometheus registration
    pub fn new() -> prometheus::Result<Self> {
        let memory_usage_bytes = register_gauge!(
            "kairosdb_cache_memory_usage_bytes",
            "Current memory usage of primary cache in bytes"
        )?;

        let memory_capacity_bytes = register_gauge!(
            "kairosdb_cache_memory_capacity_bytes",
            "Configured memory capacity of cache in bytes"
        )?;

        let memory_utilization_ratio = register_gauge!(
            "kairosdb_cache_memory_utilization_ratio",
            "Memory utilization ratio (0.0 to 1.0)"
        )?;

        let disk_usage_bytes = register_gauge!(
            "kairosdb_cache_disk_usage_bytes",
            "Current disk usage of primary cache in bytes"
        )?;

        let disk_capacity_bytes = register_gauge!(
            "kairosdb_cache_disk_capacity_bytes",
            "Configured disk capacity of cache in bytes"
        )?;

        let disk_utilization_ratio = register_gauge!(
            "kairosdb_cache_disk_utilization_ratio",
            "Disk utilization ratio (0.0 to 1.0)"
        )?;

        let total_memory_usage_bytes = register_gauge!(
            "kairosdb_cache_total_memory_usage_bytes",
            "Total memory usage across all active caches in bytes"
        )?;

        let total_disk_usage_bytes = register_gauge!(
            "kairosdb_cache_total_disk_usage_bytes",
            "Total disk usage across all active caches in bytes"
        )?;

        let hit_ratio_primary = register_gauge!(
            "kairosdb_cache_hit_ratio_primary",
            "Hit ratio for primary cache (0.0 to 1.0)"
        )?;

        let hit_ratio_secondary = register_gauge!(
            "kairosdb_cache_hit_ratio_secondary",
            "Hit ratio for secondary cache during overlap periods (0.0 to 1.0)"
        )?;

        let hit_ratio_total = register_gauge!(
            "kairosdb_cache_hit_ratio_total",
            "Combined hit ratio across all active caches (0.0 to 1.0)"
        )?;

        let cache_hits_total =
            register_counter!("kairosdb_cache_hits_total", "Total number of cache hits")?;

        let cache_misses_total = register_counter!(
            "kairosdb_cache_misses_total",
            "Total number of cache misses"
        )?;

        let cache_inserts_total = register_counter!(
            "kairosdb_cache_inserts_total",
            "Total number of cache insertions"
        )?;

        let cache_evictions_total = register_counter!(
            "kairosdb_cache_evictions_total",
            "Total number of cache evictions"
        )?;

        let rotation_events_total = register_counter!(
            "kairosdb_cache_rotation_events_total",
            "Total number of cache rotation events"
        )?;

        let overlap_periods_total = register_counter!(
            "kairosdb_cache_overlap_periods_total",
            "Total number of cache overlap periods started"
        )?;

        let cache_generations_total = register_counter!(
            "kairosdb_cache_generations_total",
            "Total number of cache generations created"
        )?;

        let primary_cache_age_seconds = register_gauge!(
            "kairosdb_cache_primary_age_seconds",
            "Age of primary cache in seconds"
        )?;

        let secondary_cache_age_seconds = register_gauge!(
            "kairosdb_cache_secondary_age_seconds",
            "Age of secondary cache in seconds (when active)"
        )?;

        let overlap_period_active = register_gauge!(
            "kairosdb_cache_overlap_period_active",
            "Whether cache is currently in overlap period (1=active, 0=inactive)"
        )?;

        let cache_operation_duration = register_histogram!(
            "kairosdb_cache_operation_duration_seconds",
            "Duration of cache operations in seconds",
            vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
        )?;

        let cache_maintenance_duration = register_histogram!(
            "kairosdb_cache_maintenance_duration_seconds",
            "Duration of cache maintenance operations in seconds",
            vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
        )?;

        Ok(Self {
            memory_usage_bytes,
            memory_capacity_bytes,
            memory_utilization_ratio,
            disk_usage_bytes,
            disk_capacity_bytes,
            disk_utilization_ratio,
            total_memory_usage_bytes,
            total_disk_usage_bytes,
            hit_ratio_primary,
            hit_ratio_secondary,
            hit_ratio_total,
            cache_hits_total,
            cache_misses_total,
            cache_inserts_total,
            cache_evictions_total,
            rotation_events_total,
            overlap_periods_total,
            cache_generations_total,
            primary_cache_age_seconds,
            secondary_cache_age_seconds,
            overlap_period_active,
            cache_operation_duration,
            cache_maintenance_duration,
        })
    }

    /// Update all metrics from cache stats
    pub fn update_from_stats(&self, stats: &CacheStats) {
        // Memory metrics
        self.memory_usage_bytes
            .set(stats.primary_memory_usage as f64);
        self.memory_capacity_bytes.set(stats.memory_capacity as f64);

        if stats.memory_capacity > 0 {
            let utilization = stats.primary_memory_usage as f64 / stats.memory_capacity as f64;
            self.memory_utilization_ratio.set(utilization);
        }

        // Disk metrics
        self.disk_usage_bytes.set(stats.primary_disk_usage as f64);
        self.disk_capacity_bytes.set(stats.disk_capacity as f64);

        if stats.disk_capacity > 0 {
            let utilization = stats.primary_disk_usage as f64 / stats.disk_capacity as f64;
            self.disk_utilization_ratio.set(utilization);
        }

        // Total usage across all caches
        self.total_memory_usage_bytes
            .set(stats.total_memory_usage() as f64);
        self.total_disk_usage_bytes
            .set(stats.total_disk_usage() as f64);

        // Hit ratios
        self.hit_ratio_primary.set(stats.primary_hit_ratio);
        if let Some(secondary_hit_ratio) = stats.secondary_hit_ratio {
            self.hit_ratio_secondary.set(secondary_hit_ratio);
        }

        // Calculate total hit ratio (weighted by cache usage)
        if stats.in_overlap_period {
            if let Some(secondary_hit_ratio) = stats.secondary_hit_ratio {
                // During overlap, average the hit ratios
                let total_hit_ratio = (stats.primary_hit_ratio + secondary_hit_ratio) / 2.0;
                self.hit_ratio_total.set(total_hit_ratio);
            } else {
                self.hit_ratio_total.set(stats.primary_hit_ratio);
            }
        } else {
            self.hit_ratio_total.set(stats.primary_hit_ratio);
        }

        // Age metrics
        self.primary_cache_age_seconds
            .set(stats.primary_age_seconds as f64);
        if let Some(secondary_age) = stats.secondary_age_seconds {
            self.secondary_cache_age_seconds.set(secondary_age as f64);
        } else {
            self.secondary_cache_age_seconds.set(0.0);
        }

        // Overlap period status
        self.overlap_period_active
            .set(if stats.in_overlap_period { 1.0 } else { 0.0 });
    }

    /// Record a cache hit
    pub fn record_cache_hit(&self) {
        self.cache_hits_total.inc();
    }

    /// Record a cache miss (which results in an insert)
    pub fn record_cache_miss(&self) {
        self.cache_misses_total.inc();
        self.cache_inserts_total.inc();
    }

    /// Record a cache rotation event
    pub fn record_cache_rotation(&self) {
        self.rotation_events_total.inc();
    }

    /// Record start of overlap period
    pub fn record_overlap_period_start(&self) {
        self.overlap_periods_total.inc();
    }

    /// Record creation of new cache generation
    pub fn record_cache_generation(&self) {
        self.cache_generations_total.inc();
    }

    /// Record cache operation timing
    pub fn record_cache_operation_duration(&self, duration: std::time::Duration) {
        self.cache_operation_duration
            .observe(duration.as_secs_f64());
    }

    /// Record cache maintenance timing
    pub fn record_maintenance_duration(&self, duration: std::time::Duration) {
        self.cache_maintenance_duration
            .observe(duration.as_secs_f64());
    }
}

impl Default for CacheMetrics {
    fn default() -> Self {
        Self::new().expect("Failed to register cache metrics")
    }
}
