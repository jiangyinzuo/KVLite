# KVLite
[![build](https://github.com/ChiangYintso/KVLite/actions/workflows/ci.yml/badge.svg)](https://github.com/ChiangYintso/KVLite/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/ChiangYintso/KVLite/branch/main/graph/badge.svg?token=VVR3RGGX5M)](https://codecov.io/gh/ChiangYintso/KVLite)  
A toy key-value storage for DB study

## Run tests 
```shell
RUST_LOG=debug RUSTFLAGS="-Z sanitizer=leak" cargo test --target x86_64-unknown-linux-gnu
```

## References
- [kvs in PingCAP talent plan](https://github.com/pingcap/talent-plan)
- [wickdb](https://github.com/Fullstop000/wickdb)
