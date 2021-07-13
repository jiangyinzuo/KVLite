# KVLite
[![Build Status](https://travis-ci.com/ChiangYintso/KVLite.svg?branch=main)](https://travis-ci.com/ChiangYintso/KVLite)
[![codecov](https://codecov.io/gh/ChiangYintso/KVLite/branch/main/graph/badge.svg?token=VVR3RGGX5M)](https://codecov.io/gh/ChiangYintso/KVLite)  

A toy key-value storage for DB study
## Build

```shell
cargo build --release --features "snappy_compression"
```

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

### Use snappy compression algorithm

    KVLite: version 0.1.0
    Date: 2021-07-13T13:10:53.138800941
    CPU: 8 * Intel(R) Core(TM) i5-8265U CPU @ 1.60GHz
    CPU Cache: 6144 KB
    Keys: 16 bytes each
    Values: 100 bytes each
    Entries: 1000000
    RawSize: 110.626220703125 MB (estimated)
    Use system default memory allocator
    Use snappy compression algorithm
    -------------------------------------------------
    fill_seq: 44.64303430330684 MB/s | file size: 135410416
    read_seq: 38.77295238624774 MB/s (1000000 of 1000000 found)
    fill_random_sync: 0.13073022188512345 MB/s) (10000 ops) | file size: 1240000
    fill_random: 39.78769821447858 MB/s | file size: 99588198
    read_random: 226000.5000893866 reads per second (631685 of 1000000 found)

### No compression

    KVLite: version 0.1.0
    Date: 2021-07-13T13:08:40.787929916
    CPU: 8 * Intel(R) Core(TM) i5-8265U CPU @ 1.60GHz
    CPU Cache: 6144 KB
    Keys: 16 bytes each
    Values: 100 bytes each
    Entries: 1000000
    RawSize: 110.626220703125 MB (estimated)
    Use system default memory allocator
    No compression algorithm
    -------------------------------------------------
    fill_seq: 44.520253951884015 MB/s | file size: 166520748
    read_seq: 37.4738606837932 MB/s (1000000 of 1000000 found)
    fill_random_sync: 0.13020515270604827 MB/s) (10000 ops) | file size: 1240000
    fill_random: 39.43130443949237 MB/s | file size: 104770916
    read_random: 233108.48758588755 reads per second (631821 of 1000000 found)

## References

- [LevelDB](https://github.com/google/leveldb)
- [kvs in PingCAP talent plan](https://github.com/pingcap/talent-plan)
- [wickdb](https://github.com/Fullstop000/wickdb)
