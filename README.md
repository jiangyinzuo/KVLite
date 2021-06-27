# KVLite
[![Build Status](https://travis-ci.com/ChiangYintso/KVLite.svg?branch=main)](https://travis-ci.com/ChiangYintso/KVLite)
[![codecov](https://codecov.io/gh/ChiangYintso/KVLite/branch/main/graph/badge.svg?token=VVR3RGGX5M)](https://codecov.io/gh/ChiangYintso/KVLite)  

A toy key-value storage for DB study

## Examples

`get`, `set` and `remove` command
```rust
use kvlite::db::key_types::InternalKey;
use kvlite::db::no_transaction_db::NoTransactionDB;
use kvlite::db::options::WriteOptions;
use kvlite::db::DB;
use kvlite::memory::SkipMapMemTable;
use kvlite::wal::simple_wal::SimpleWriteAheadLog;
use tempfile::TempDir;

fn main() {
    let temp_dir = TempDir::new().unwrap();
    let db = NoTransactionDB::<
        InternalKey,
        InternalKey,
        SkipMapMemTable<InternalKey>,
        SimpleWriteAheadLog,
    >::open(temp_dir.path())
        .unwrap();
    let write_option = WriteOptions { sync: false };
    let hello = Vec::from("hello");
    let value = Vec::from("value1");
    db.set(&write_option, hello.clone(), value).unwrap();

    println!("{:?}", db.get(&"hello".into()).unwrap()); // Some("value1")
    db.remove(&write_option, hello).unwrap();
    assert!(db.get(&"hello".into()).unwrap().is_none());
}
```

## Run tests 
```shell
RUST_LOG=debug RUSTFLAGS="-Z sanitizer=leak" cargo test --target x86_64-unknown-linux-gnu
```

## Performance

### Setup

    KVLite: version 0.1.0  
    Date: 2021-06-27T06:57:24.256792406  
    CPU: 8 * Intel(R) Core(TM) i5-8265U CPU @ 1.60GHz  
    CPU Cache: 6144 KB  
    Keys: 16 bytes each  
    Values: 100 bytes each  
    Entries: 1000000  
    RawSize: 110.626220703125 MB (estimated)

### Performance

    fill_seq: 20.449749898635243 MB/s
    fill_random_sync: 0.100139876092273 MB/s) (10000 ops)
    fill_random: 19.997378658308623 MB/s
    read_seq: 19.812293545262236 MB/s (632106 of 1000000 found)

## References

- [LevelDB](https://github.com/google/leveldb)
- [kvs in PingCAP talent plan](https://github.com/pingcap/talent-plan)
- [wickdb](https://github.com/Fullstop000/wickdb)
