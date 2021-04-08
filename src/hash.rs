//! Implementation of murmur hash: [https://sites.google.com/site/murmurhash/]

pub fn murmur_hash(key: &[u8], seed: u32) -> u32 {
    // 'M' and 'R' are mixing constants generated offline.
    // They're not really 'magic', they just happen to work well.
    const M: u32 = 0x5bd1e995;
    const R: i32 = 24;

    let mut len = key.len();

    // Initialize the hash to a 'random' value
    let mut h: u32 = seed ^ len as u32;

    let mut data = key.as_ptr();
    // Mix 4 bytes at a time into the hash
    while len >= 4 {
        let mut k = unsafe { *(data as *const u32) };
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);

        h = h.wrapping_mul(M);
        h ^= k;
        unsafe {
            data = data.add(4);
        }
        len -= 4;
    }

    // Handle the last few bytes of the input array
    unsafe {
        if len >= 3 {
            h ^= (data.add(2).read() as u32) << 16;
        }
        if len >= 2 {
            h ^= (data.add(1).read() as u32) << 8;
        }
        if len >= 1 {
            h ^= data.add(0).read() as u32;
            h = h.wrapping_mul(M);
        }
    }

    // Do a few final mixes of the hash to ensure the last few
    // bytes are well-incorporated.
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

#[cfg(test)]
mod tests {
    use crate::hash::murmur_hash;

    #[test]
    fn test_hash() {
        let h1 = murmur_hash("hello".as_bytes(), 0xbc9f1d34);
        let h2 = murmur_hash("hellp".as_bytes(), 0xbc9f1d34);
        let h3 = murmur_hash(String::from("hello").as_bytes(), 0xbc9f1d34);
        assert_eq!(h1, h3);
        assert!(hamming_distance(h1, h2) >= 16);

        let key = [0x23, 0xc9, 0x00, 0x00];
        let h3 = murmur_hash(&key, 0xf123cf13);
        assert_eq!(h3, 4037331841);
    }

    fn hamming_distance(n1: u32, n2: u32) -> u32 {
        let mut n = n1 ^ n2;
        let mut res = 0;
        while n > 0 {
            if (n & 1) == 1 {
                res += 1;
            }
            n >>= 1;
        }
        res
    }
}
