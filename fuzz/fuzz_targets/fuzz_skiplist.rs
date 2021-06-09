#![no_main]

use kvlite::collections::skip_list::skipmap::SkipMap;
use libfuzzer_sys::arbitrary;
use libfuzzer_sys::fuzz_target;

#[derive(arbitrary::Arbitrary, Debug)]
enum SkipMapMethod {
    Insert {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Remove {
        key: Vec<u8>,
    },
    RangeGet {
        key_start: Vec<u8>,
        key_end: Vec<u8>,
    },
}

fuzz_target!(|methods: Vec<SkipMapMethod>| {
    let mut skip_map = SkipMap::<Vec<u8>, Vec<u8>>::default();

    use SkipMapMethod::*;
    for method in methods {
        match method {
            Insert { key, value } => {
                let old_len = skip_map.len();
                if skip_map.insert(key.clone(), value.clone()) {
                    assert_eq!(old_len, skip_map.len());
                } else {
                    assert_eq!(old_len + 1, skip_map.len());
                }
                unsafe {
                    assert_eq!((*skip_map.find_first_ge(&key, None)).entry.value, value);
                }
            }
            Remove { key } => {
                skip_map.remove(key);
            }
            RangeGet {
                mut key_start,
                mut key_end,
            } => {
                if key_start > key_end {
                    std::mem::swap(&mut key_start, &mut key_end);
                }
                let mut res = SkipMap::<Vec<u8>, Vec<u8>>::default();
                skip_map.range_get(&key_start, &key_end, &mut res);
                for kv in res {
                    assert!(key_start <= kv.0);
                    assert!(kv.0 <= key_end);
                }
            }
        }
    }
    let mut last_key = Vec::new();
    for (key, _) in skip_map {
        assert!(last_key <= key);
        last_key = key;
    }
});
