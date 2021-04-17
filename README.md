# KVLite
[![Build Status](https://travis-ci.com/ChiangYintso/KVLite.svg?branch=main)](https://travis-ci.com/ChiangYintso/KVLite)
[![codecov](https://codecov.io/gh/ChiangYintso/KVLite/branch/main/graph/badge.svg?token=VVR3RGGX5M)](https://codecov.io/gh/ChiangYintso/KVLite)  

A toy key-value storage for DB study

## Examples

`get`, `set` and `remove` command
```rust
use kvlite::memory::SkipMapMemTable;
use kvlite::KVLite;
use tempfile::TempDir;

fn main() {
    let temp_dir = TempDir::new().unwrap();
    let db = KVLite::<SkipMapMemTable>::open(temp_dir.path()).unwrap();
    db.set("hello".to_string(), "value1".to_string()).unwrap();

    println!("{:?}", db.get(&"hello".into()).unwrap()); // Some("value1")
    db.remove("hello".to_string()).unwrap();
    assert!(db.get(&"hello".to_string()).unwrap().is_none());
}
```

## Run tests 
```shell
RUST_LOG=debug RUSTFLAGS="-Z sanitizer=leak" cargo test --target x86_64-unknown-linux-gnu
```

## Performance

Unbuntu20.04  
Linux version 5.4.0-70-generic  
Intel(R) Core(TM) i5-8265U CPU @ 1.60GHz

|            |  insert | query |
| ---------- |---------|-------|
| 10^5       |  522ms  |  32ms |
| 10^6       |  5.07s  | 4.38s |

## References

- [LevelDB](https://github.com/google/leveldb)
- [kvs in PingCAP talent plan](https://github.com/pingcap/talent-plan)
- [wickdb](https://github.com/Fullstop000/wickdb)
