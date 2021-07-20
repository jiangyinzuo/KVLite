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
    Date: 2021-07-20T03:13:10.975323388
    CPU: 8 * Intel(R) Core(TM) i5-8265U CPU @ 1.60GHz
    CPU Cache: 6144 KB
    Keys: 16 bytes each
    Values: 100 bytes each
    Entries: 1000000
    RawSize: 110.626220703125 MB (estimated)
    Use system default memory allocator
    Use snappy compression algorithm
    -------------------------------------------------
    fill_seq            :      1.767 micros/op     62.604 MB/s | file size: 113853798
    read_seq            :    580.254 MB/s (1000000 of 1000000 found)
    fill_random_sync    :   1098.047 micros/op      0.101 MB/s | file size: 1240000  (10000 ops)
    fill_random         :      2.578 micros/op     42.911 MB/s | file size: 96048869
    read_random         : 245514.353 reads per second (633076 of 1000000 found)
    overwrite           :      2.747 micros/op     40.278 MB/s | file size: 196794071

### Use Jemalloc

    KVLite: version 0.1.0
    Date: 2021-07-20T03:15:41.872893199
    CPU: 8 * Intel(R) Core(TM) i5-8265U CPU @ 1.60GHz
    CPU Cache: 6144 KB
    Keys: 16 bytes each
    Values: 100 bytes each
    Entries: 1000000
    RawSize: 110.626220703125 MB (estimated)
    Use jemalloc
    No compression algorithm
    -------------------------------------------------
    fill_seq            :      1.624 micros/op     68.139 MB/s | file size: 130046026
    read_seq            :    594.012 MB/s (1000000 of 1000000 found)
    fill_random_sync    :   1094.695 micros/op      0.101 MB/s | file size: 1240000  (10000 ops)
    fill_random         :      2.053 micros/op     53.895 MB/s | file size: 103494362
    read_random         : 361354.763 reads per second (631783 of 1000000 found)
    overwrite           :      2.178 micros/op     50.803 MB/s | file size: 163332454

### No compression

    KVLite: version 0.1.0
    Date: 2021-07-20T03:12:08.584243849
    CPU: 8 * Intel(R) Core(TM) i5-8265U CPU @ 1.60GHz
    CPU Cache: 6144 KB
    Keys: 16 bytes each
    Values: 100 bytes each
    Entries: 1000000
    RawSize: 110.626220703125 MB (estimated)
    Use system default memory allocator
    No compression algorithm
    -------------------------------------------------
    fill_seq            :      1.685 micros/op     65.639 MB/s | file size: 130046026
    read_seq            :    643.674 MB/s (1000000 of 1000000 found)
    fill_random_sync    :   1074.981 micros/op      0.103 MB/s | file size: 1240000  (10000 ops)
    fill_random         :      2.625 micros/op     42.149 MB/s | file size: 119826300
    read_random         : 250493.885 reads per second (631575 of 1000000 found)
    overwrite           :      2.729 micros/op     40.541 MB/s | file size: 165017016

## References

- [LevelDB](https://github.com/google/leveldb)
- [kvs in PingCAP talent plan](https://github.com/pingcap/talent-plan)
- [wickdb](https://github.com/Fullstop000/wickdb)
