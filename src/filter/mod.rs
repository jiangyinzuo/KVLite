pub mod bloom_filter;

pub type DefaultBloomFilter = filters_rs::BlockedBloomFilter;

pub const SEED: u32 = 0xc7b4e193;
