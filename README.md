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
    Date: 2021-06-28T07:11:24.708327460
    CPU: 8 * Intel(R) Core(TM) i5-8265U CPU @ 1.60GHz
    CPU Cache: 6144 KB
    Keys: 16 bytes each
    Values: 100 bytes each
    Entries: 1000000
    RawSize: 110.626220703125 MB (estimated)
    Use snappy compression algorithm
    -------------------------------------------------
    fill_seq: 20.492037118445587 MB/s | file size: 116808372
    read_seq: 38.62436076515437 MB/s (1000000 of 1000000 found)
    fill_random_sync: 0.10348371876006464 MB/s) (10000 ops) | file size: 1240000
    fill_random: 19.806122446689113 MB/s | file size: 89302457
    read_random: 198573.51774638818 reads per second (630599 of 1000000 found)

### No compression

    KVLite: version 0.1.0
    Date: 2021-06-28T07:10:34.525498395
    CPU: 8 * Intel(R) Core(TM) i5-8265U CPU @ 1.60GHz
    CPU Cache: 6144 KB
    Keys: 16 bytes each
    Values: 100 bytes each
    Entries: 1000000
    RawSize: 110.626220703125 MB (estimated)
    -------------------------------------------------
    fill_seq: 20.557259403829693 MB/s | file size: 130044972
    read_seq: 42.80777698543364 MB/s (1000000 of 1000000 found)
    fill_random_sync: 0.10423079411706664 MB/s) (10000 ops) | file size: 1240000
    fill_random: 19.400202427874216 MB/s | file size: 102008205
    read_random: 162310.122156893 reads per second (632384 of 1000000 found)


## References

- [LevelDB](https://github.com/google/leveldb)
- [kvs in PingCAP talent plan](https://github.com/pingcap/talent-plan)
- [wickdb](https://github.com/Fullstop000/wickdb)
