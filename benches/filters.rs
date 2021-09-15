#![feature(test)]
extern crate kvlite;
extern crate test;

use kvlite::filter::bloom_filter::BloomFilter;
use rand::Rng;
use test::Bencher;

const NUM_KEYS: usize = 1000000;

fn create_hashes() -> Vec<u32> {
    let mut hashes = vec![0; NUM_KEYS];
    let mut rng = rand::thread_rng();
    rng.fill(hashes.as_mut_slice());
    hashes
}

#[bench]
fn bench_bbf(b: &mut Bencher) {
    let hashes = create_hashes();
    let mut bbf = filters_rs::BlockedBloomFilter::create_filter(NUM_KEYS);
    b.iter(|| {
        for &h in &hashes {
            bbf.add(h);
        }
        for &h in &hashes {
            assert!(bbf.may_contain(h));
        }
    })
}

#[bench]
fn bench_bf(b: &mut Bencher) {
    let hashes = create_hashes();
    let mut bf = BloomFilter::create_filter(NUM_KEYS);
    b.iter(|| {
        for &h in &hashes {
            bf.add(h);
        }
        for &h in &hashes {
            assert!(bf.may_contain(h));
        }
    })
}
