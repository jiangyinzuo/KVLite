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
    Date: 2021-07-13T12:14:06.152110049
    CPU: 8 * Intel(R) Core(TM) i5-8265U CPU @ 1.60GHz
    CPU Cache: 6144 KB
    Keys: 16 bytes each
    Values: 100 bytes each
    Entries: 1000000
    RawSize: 110.626220703125 MB (estimated)
    Use snappy compression algorithm
    -------------------------------------------------
    fill_seq: 45.044436638625484 MB/s | file size: 135931250
    read_seq: 32.88061709861111 MB/s (1000000 of 1000000 found)
    fill_random_sync: 0.13417676723048427 MB/s) (10000 ops) | file size: 1240000
    fill_random: 38.63624951671868 MB/s | file size: 155038153
    read_random: 235909.89359239663 reads per second (631777 of 1000000 found)

### No compression

    KVLite: version 0.1.0
    Date: 2021-07-13T12:12:53.960645016
    CPU: 8 * Intel(R) Core(TM) i5-8265U CPU @ 1.60GHz
    CPU Cache: 6144 KB
    Keys: 16 bytes each
    Values: 100 bytes each
    Entries: 1000000
    RawSize: 110.626220703125 MB (estimated)
    No compression algorithm
    -------------------------------------------------
    fill_seq: 44.71897356778016 MB/s | file size: 164353964
    read_seq: 41.80858361930294 MB/s (1000000 of 1000000 found)
    fill_random_sync: 0.12768166120122879 MB/s) (10000 ops) | file size: 1240000
    fill_random: 35.40766444700086 MB/s | file size: 123891417
    read_random: 225942.19170514427 reads per second (630738 of 1000000 found)

## References

- [LevelDB](https://github.com/google/leveldb)
- [kvs in PingCAP talent plan](https://github.com/pingcap/talent-plan)
- [wickdb](https://github.com/Fullstop000/wickdb)
