//! Deterministic seeded pseudo-random number generator for sans-IO state machines.

use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::{Rng as _, SeedableRng};

/// Deterministic seeded RNG backed by `ChaCha8Rng`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rng {
    inner: ChaCha8Rng,
}

impl Rng {
    /// Creates a deterministic RNG from a 64-bit seed.
    pub fn from_seed(seed: u64) -> Self {
        Self {
            inner: ChaCha8Rng::seed_from_u64(seed),
        }
    }

    /// Forks a deterministic child RNG by drawing its seed from `self`.
    #[must_use]
    pub fn fork(&mut self) -> Self {
        Self::from_seed(self.next_u64())
    }

    /// Draws the next pseudo-random `u64`.
    pub fn next_u64(&mut self) -> u64 {
        self.inner.next_u64()
    }

    /// Draws the next pseudo-random `u32`.
    pub fn next_u32(&mut self) -> u32 {
        self.inner.next_u32()
    }

    /// Fills `dest` with deterministic pseudo-random bytes.
    pub fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.inner.fill_bytes(dest);
    }

    /// Draws a uniformly distributed integer in `0..bound` (returns `0` when `bound <= 1`).
    pub fn below(&mut self, bound: u64) -> u64 {
        if bound <= 1 {
            return 0;
        }
        let threshold = bound.wrapping_neg() % bound;
        loop {
            let candidate = self.next_u64();
            if candidate >= threshold {
                return candidate % bound;
            }
        }
    }

    /// Draws a uniformly distributed integer in `low..=high_inclusive`.
    pub fn range_inclusive(&mut self, low: u64, high_inclusive: u64) -> u64 {
        assert!(
            low <= high_inclusive,
            "invalid range {low}..={high_inclusive}"
        );
        let span = high_inclusive - low;
        if span == u64::MAX {
            return self.next_u64();
        }
        low + self.below(span + 1)
    }

    /// Returns true with probability `numerator / denominator`.
    pub fn chance(&mut self, numerator: u32, denominator: u32) -> bool {
        assert!(denominator > 0, "denominator must be non-zero");
        if numerator >= denominator {
            return true;
        }
        self.below(u64::from(denominator)) < u64::from(numerator)
    }

    /// Shuffles `slice` in place using the Fisher-Yates algorithm.
    pub fn shuffle<T>(&mut self, slice: &mut [T]) {
        for i in (1..slice.len()).rev() {
            let bound = (i as u64).saturating_add(1);
            let j = usize::try_from(self.below(bound)).unwrap_or(0);
            slice.swap(i, j);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_seeds_produce_identical_streams() {
        let mut a = Rng::from_seed(0xDEAD_BEEF_CAFE_BABE);
        let mut b = Rng::from_seed(0xDEAD_BEEF_CAFE_BABE);

        for _ in 0..256 {
            assert_eq!(a.next_u64(), b.next_u64());
            assert_eq!(a.next_u32(), b.next_u32());
            assert_eq!(a.below(97), b.below(97));
            assert_eq!(a.range_inclusive(1000, 2000), b.range_inclusive(1000, 2000));
        }

        let mut bytes_a = [0u8; 64];
        let mut bytes_b = [0u8; 64];
        a.fill_bytes(&mut bytes_a);
        b.fill_bytes(&mut bytes_b);
        assert_eq!(bytes_a, bytes_b);
    }

    #[test]
    fn forked_rng_is_deterministic_and_independent() {
        let mut root1 = Rng::from_seed(42);
        let mut child1 = root1.fork();

        let mut root2 = Rng::from_seed(42);
        let mut child2 = root2.fork();

        assert_eq!(child1.next_u64(), child2.next_u64());
        assert_eq!(root1.next_u64(), root2.next_u64());
        assert_ne!(child1.next_u64(), root1.next_u64());
    }

    #[test]
    fn bounds_and_shuffle_behave_deterministically() {
        let mut rng = Rng::from_seed(7);
        assert_eq!(rng.below(0), 0);
        assert_eq!(rng.below(1), 0);
        assert_eq!(rng.range_inclusive(5, 5), 5);
        assert!(!rng.chance(0, 10));
        assert!(rng.chance(10, 10));

        for _ in 0..512 {
            let v = rng.range_inclusive(10, 20);
            assert!((10..=20).contains(&v));
        }

        let mut v1: Vec<u32> = (0..16).collect();
        let mut v2 = v1.clone();
        let mut r1 = Rng::from_seed(99);
        let mut r2 = Rng::from_seed(99);
        r1.shuffle(&mut v1);
        r2.shuffle(&mut v2);
        assert_eq!(v1, v2);
        assert_ne!(v1, (0..16).collect::<Vec<u32>>());
    }
}
