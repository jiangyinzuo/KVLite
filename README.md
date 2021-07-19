# KVLite
[![Build Status](https://travis-ci.com/ChiangYintso/KVLite.svg?branch=main)](https://travis-ci.com/ChiangYintso/KVLite)
[![codecov](https://codecov.io/gh/ChiangYintso/KVLite/branch/main/graph/badge.svg?token=VVR3RGGX5M)](https://codecov.io/gh/ChiangYintso/KVLite)  

A toy key-value storage for DB study
## Build

```shell
cargo build --release --features "snappy_compression"
```

## Examples

see `/examples`

## Run tests 
```shell
RUST_LOG=debug RUSTFLAGS="-Z sanitizer=leak" cargo test --target x86_64-unknown-linux-gnu
```

## Performance

### Use snappy compression algorithm

    KVLite: version 0.1.0
    Date: 2021-07-19T16:03:10.161110194
    CPU: 8 * Intel(R) Core(TM) i5-8265U CPU @ 1.60GHz
    CPU Cache: 6144 KB
    Keys: 16 bytes each
    Values: 100 bytes each
    Entries: 1000000
    RawSize: 110.626220703125 MB (estimated)
    Use system default memory allocator
    Use snappy compression algorithm
    -------------------------------------------------
    fill_seq: 65.99137920154317 MB/s | file size: 117962025
    read_seq: 545.4534480532583 MB/s (1000000 of 1000000 found)
    fill_random_sync: 0.10728091068326079 MB/s) (10000 ops) | file size: 1240000
    fill_random: 41.788200062823 MB/s | file size: 98338764
    read_random: 246199.28336031933 reads per second (632443 of 1000000 found)

### No compression

    KVLite: version 0.1.0
    Date: 2021-07-19T16:00:02.526203242
    CPU: 8 * Intel(R) Core(TM) i5-8265U CPU @ 1.60GHz
    CPU Cache: 6144 KB
    Keys: 16 bytes each
    Values: 100 bytes each
    Entries: 1000000
    RawSize: 110.626220703125 MB (estimated)
    Use system default memory allocator
    No compression algorithm
    -------------------------------------------------
    fill_seq: 63.87387531471826 MB/s | file size: 130046026
    read_seq: 625.4276292143252 MB/s (1000000 of 1000000 found)
    fill_random_sync: 0.10877451308420993 MB/s) (10000 ops) | file size: 1240000
    fill_random: 42.21374494305283 MB/s | file size: 104648452
    read_random: 245721.01528415605 reads per second (632574 of 1000000 found)

## References

- [LevelDB](https://github.com/google/leveldb)
- [kvs in PingCAP talent plan](https://github.com/pingcap/talent-plan)
- [wickdb](https://github.com/Fullstop000/wickdb)
