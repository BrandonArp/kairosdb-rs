//! HistogramKeyUtility for compressing bucket keys
//!
//! This module provides exact compatibility with the Java HistogramKeyUtility
//! for efficient storage of histogram bucket keys using precision-based compression.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

/// Utility for working with histogram keys of a given precision.
/// Direct port of Java HistogramKeyUtility from kairosdb-extensions.
#[derive(Debug)]
pub struct HistogramKeyUtility {
    precision: u8,
    truncate_mask: u64,
    pack_mask: u64,
    shift: u8,
}

// Global cache of HistogramKeyUtility instances by precision
static KEY_UTILITY_MAP: LazyLock<Mutex<HashMap<u8, HistogramKeyUtility>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

impl HistogramKeyUtility {
    const MANTISSA_BITS: u8 = 52;
    const EXPONENT_BITS: u8 = 11;

    /// Create a new HistogramKeyUtility for the given precision
    fn new(precision: u8) -> Self {
        // Keep the sign bit, exponent bits, and top `precision` mantissa bits
        // Zero out the bottom (52 - precision) mantissa bits
        let mantissa_keep_bits = precision;
        let mantissa_zero_bits = Self::MANTISSA_BITS - mantissa_keep_bits;

        // Create mask that preserves sign + exponent + top mantissa bits
        // Format: SEEEEEEEEEEMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
        // We want: SEEEEEEEEEE[precision bits]00000000000000000000000
        let truncate_mask = !((1u64 << mantissa_zero_bits) - 1);

        let pack_mask = (1u64 << (precision + Self::EXPONENT_BITS + 1)) - 1;
        let shift = Self::MANTISSA_BITS - precision;

        Self {
            precision,
            truncate_mask,
            pack_mask,
            shift,
        }
    }

    /// Return the HistogramKeyUtility for the given precision.
    /// Uses a global cache to ensure singleton behavior per precision.
    pub fn get_instance(precision: u8) -> HistogramKeyUtility {
        let mut map = KEY_UTILITY_MAP.lock().unwrap();

        if let Some(utility) = map.get(&precision) {
            // Clone the utility (it's small and contains only primitive data)
            HistogramKeyUtility {
                precision: utility.precision,
                truncate_mask: utility.truncate_mask,
                pack_mask: utility.pack_mask,
                shift: utility.shift,
            }
        } else {
            let utility = Self::new(precision);
            let cloned = HistogramKeyUtility {
                precision: utility.precision,
                truncate_mask: utility.truncate_mask,
                pack_mask: utility.pack_mask,
                shift: utility.shift,
            };
            map.insert(precision, utility);
            cloned
        }
    }

    /// Truncate a f64 key at given precision and represent it as a u64.
    pub fn truncate_to_long(&self, val: f64) -> u64 {
        val.to_bits() & self.truncate_mask
    }

    /// Truncate a f64 key at a given precision and represent it as a f64.
    pub fn truncate_to_double(&self, val: f64) -> f64 {
        f64::from_bits(self.truncate_to_long(val))
    }

    /// Compute the largest magnitude (absolute value) of the bin the provided
    /// value will be placed in.
    ///
    /// If the provided value is positive, the bound will be an inclusive upper
    /// bound, and if the provided value is negative it will be an inclusive
    /// lower bound.
    pub fn bin_inclusive_bound(&self, val: f64) -> f64 {
        let mut bound = val.to_bits();
        bound >>= self.shift;
        bound = bound.wrapping_add(1);
        bound <<= self.shift;
        bound = bound.wrapping_sub(1);
        f64::from_bits(bound)
    }

    /// Pack a f64 key for storage as a u64. In addition to truncation, this also
    /// shifts the value to optimize its size under varint encoding.
    pub fn pack(&self, val: f64) -> u64 {
        let truncated = self.truncate_to_long(val);
        let shifted = truncated >> (Self::MANTISSA_BITS - self.precision);
        shifted & self.pack_mask
    }

    /// Unpack a f64 key from storage as a u64.
    pub fn unpack(&self, packed: u64) -> f64 {
        f64::from_bits(packed << (Self::MANTISSA_BITS - self.precision))
    }

    /// Get the precision for this utility
    pub fn precision(&self) -> u8 {
        self.precision
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_singleton_behavior() {
        let util1 = HistogramKeyUtility::get_instance(7);
        let util2 = HistogramKeyUtility::get_instance(7);

        assert_eq!(util1.precision, util2.precision);
        assert_eq!(util1.truncate_mask, util2.truncate_mask);
        assert_eq!(util1.pack_mask, util2.pack_mask);
        assert_eq!(util1.shift, util2.shift);
    }

    #[test]
    fn test_pack_unpack_roundtrip() {
        let utility = HistogramKeyUtility::get_instance(7);
        let original = 123.456;

        let packed = utility.pack(original);
        let unpacked = utility.unpack(packed);

        // Should be close after truncation
        let truncated = utility.truncate_to_double(original);
        assert_eq!(unpacked, truncated);
    }

    #[test]
    fn test_small_values_pack_unpack() {
        let utility = HistogramKeyUtility::get_instance(7);

        // Test the values from our failing test
        let values = [0.1, 0.5, 1.0, 5.0];

        for &val in &values {
            let truncated = utility.truncate_to_double(val);
            let packed = utility.pack(val);
            let unpacked = utility.unpack(packed);

            assert_eq!(
                unpacked, truncated,
                "Pack/unpack should match truncated value for {}",
                val
            );
            assert!(
                unpacked > 0.0,
                "Value {} should not become zero after pack/unpack",
                val
            );
        }
    }

    #[test]
    fn test_wide_range_values_pack_unpack() {
        let utility = HistogramKeyUtility::get_instance(7);

        // Test a wide range of values including very small and very large
        let values = [
            0.001,
            0.01,
            0.1,
            0.5,
            1.0,
            5.0,
            10.0,
            100.0,
            1000.0,
            1e-10,
            1e-5,
            1e5,
            1e10,
            std::f64::consts::PI,
            std::f64::consts::E,
            f64::MIN_POSITIVE,
            1e-100,
            1e100,
        ];

        for &val in &values {
            let truncated = utility.truncate_to_double(val);
            let packed = utility.pack(val);
            let unpacked = utility.unpack(packed);

            // The unpacked value should match the truncated value
            assert_eq!(
                unpacked, truncated,
                "Pack/unpack should match truncated value for {}",
                val
            );

            // For positive values, unpacked should also be positive
            if val > 0.0 {
                assert!(
                    unpacked > 0.0,
                    "Positive value {} should remain positive after pack/unpack",
                    val
                );
            }

            // Verify the unpacked value maintains the same order of magnitude (approximately)
            if (1e-50..=1e50).contains(&val) {
                let ratio = unpacked / val;
                assert!(
                    ratio > 1e-3 && ratio < 1e3,
                    "Pack/unpack should preserve order of magnitude for {} (ratio: {})",
                    val,
                    ratio
                );
            }
        }
    }

    #[test]
    fn test_truncation() {
        let utility = HistogramKeyUtility::get_instance(7);
        let value = 123.456789;

        let truncated = utility.truncate_to_double(value);
        let truncated_long = utility.truncate_to_long(value);

        assert_eq!(truncated, f64::from_bits(truncated_long));
    }

    #[test]
    fn test_bin_inclusive_bound() {
        let utility = HistogramKeyUtility::get_instance(7);

        let positive_val = 10.5;
        let bound = utility.bin_inclusive_bound(positive_val);
        assert!(bound >= positive_val);

        let negative_val = -10.5;
        let neg_bound = utility.bin_inclusive_bound(negative_val);
        assert!(neg_bound <= negative_val);
    }

    #[test]
    fn test_different_precisions() {
        let util_7 = HistogramKeyUtility::get_instance(7);
        let util_10 = HistogramKeyUtility::get_instance(10);

        assert_ne!(util_7.precision, util_10.precision);
        assert_ne!(util_7.truncate_mask, util_10.truncate_mask);
        assert_ne!(util_7.pack_mask, util_10.pack_mask);
    }
}
