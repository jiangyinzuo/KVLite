use std::cmp::max;

const BITS_PER_KEY: usize = 10;

/// K =~ ln(2) * BITS_PER_KEY = 6
const K: u8 = 6;

pub struct BloomFilter(pub(crate) Vec<u8>);

impl BloomFilter {
    pub fn create_filter(num_keys: usize) -> BloomFilter {
        let dst: Vec<u8> = vec![0; Self::calc_bytes(num_keys)];
        debug_assert_eq!(dst.len(), dst.capacity());
        BloomFilter(dst)
    }

    /// Compute bloom filter size (in both bits and bytes)
    /// For small n, we can see a very high false positive rate.  Fix it
    /// by enforcing a minimum bloom filter length.
    #[inline]
    pub(crate) fn calc_bytes(num_keys: usize) -> usize {
        let bits = max(num_keys * BITS_PER_KEY, 64);
        (bits + 7) / 8
    }

    pub fn len(&self) -> u32 {
        self.0.len() as u32
    }
}

impl BloomFilter {
    pub fn add(&mut self, mut h: u32) {
        let delta = (h >> 17) | (h << 15); // rotate right 17 bits
        for _ in 0..K {
            h = h.wrapping_add(delta);
            let bit_pos = h % (self.len() * 8);
            self.0[(bit_pos / 8) as usize] |= 1 << (bit_pos % 8);
        }
    }

    pub fn may_contain(&self, mut h: u32) -> bool {
        let delta = (h >> 17) | (h << 15); // rotate right 17 bits
        for _ in 0..K {
            h = h.wrapping_add(delta);
            let bit_pos = h % (self.len() * 8);
            if (self.0[(bit_pos / 8) as usize] & (1 << (bit_pos % 8))) == 0 {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use crate::filter::bloom_filter::BloomFilter;
    use crate::filter::SEED;
    use crate::hash::murmur_hash;

    #[test]
    fn test_contain_key() {
        let mut filter = BloomFilter::create_filter(10);
        for i in 0..10 {
            let key = format!("kkkey{}", i);
            let h = murmur_hash(key.as_bytes(), SEED);
            filter.add(h);
        }
        for i in 0..10 {
            let key = format!("kkkey{}", i);
            let h = murmur_hash(key.as_bytes(), SEED);
            assert!(filter.may_contain(h));
        }
        let h = murmur_hash("fweaefewaf9".as_bytes(), SEED);
        assert!(!filter.may_contain(h));
    }

    #[test]
    fn test_false_positive1() {
        let mut rng = rand::thread_rng();

        let mut filter = BloomFilter::create_filter(10000);
        let rand_keys = rand::seq::index::sample(&mut rng, usize::MAX, 20000);
        for i in 0..10000 {
            let h = murmur_hash(&rand_keys.index(i).to_le_bytes(), SEED);
            filter.add(h);
        }
        for i in 0..10000 {
            let h = murmur_hash(&rand_keys.index(i).to_le_bytes(), SEED);
            assert!(filter.may_contain(h));
        }

        let mut false_pos_count = 0;
        for i in 10000..20000 {
            let h = murmur_hash(&rand_keys.index(i).to_le_bytes(), SEED);
            if filter.may_contain(h) {
                false_pos_count += 1;
            }
        }
        assert!(
            false_pos_count < 200,
            "false positive rate: {}/10000",
            false_pos_count
        );
    }

    #[test]
    fn test_false_positive2() {
        let mut filter = BloomFilter::create_filter(10000);
        for i in 0..10000 {
            let h = murmur_hash(format!("key{}", i).as_bytes(), SEED);
            filter.add(h);
        }
        for i in 0..10000 {
            let h = murmur_hash(format!("key{}", i).as_bytes(), SEED);
            debug_assert!(filter.may_contain(h));
        }

        let mut false_pos_count = 0;
        for i in 10100..20100 {
            let h = murmur_hash(format!("key{}", i).as_bytes(), SEED);
            if filter.may_contain(h) {
                false_pos_count += 1;
            }
        }
        assert!(
            false_pos_count < 200,
            "false positive rate: {}/10000",
            false_pos_count
        );
    }
}
