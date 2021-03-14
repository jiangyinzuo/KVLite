#![feature(test)]
extern crate kvlite;
extern crate test;

use kvlite::collections::skiplist::SkipMap;
use kvlite::collections::treap::TreapMap;
use std::collections::BTreeMap;
use test::Bencher;

#[bench]
fn bench_treap_map_insert(b: &mut Bencher) {
    b.iter(|| {
        let mut treap: TreapMap<i32, i32> = TreapMap::new();
        for i in 0..5000i32 {
            treap.insert(i, rand::random());
        }
        for i in 0..5000i32 {
            treap.insert(i, rand::random());
        }
        treap
    })
}

#[bench]
fn bench_btree_map_insert(b: &mut Bencher) {
    b.iter(|| {
        let mut btree: BTreeMap<i32, i32> = BTreeMap::new();
        for i in 0..5000i32 {
            btree.insert(i, rand::random());
        }
        for i in 0..5000i32 {
            btree.insert(i, rand::random());
        }
        btree
    })
}

#[bench]
fn bench_skip_map_insert(b: &mut Bencher) {
    b.iter(|| {
        let skip_map: SkipMap<i32, i32> = SkipMap::new();
        for i in 0..5000i32 {
            skip_map.insert(i, rand::random());
        }
        for i in 0..5000i32 {
            skip_map.insert(i, rand::random());
        }
        skip_map
    })
}

#[bench]
fn bench_treap_map_get(b: &mut Bencher) {
    let mut treap = BTreeMap::new();
    for i in 0..5000i32 {
        treap.insert(i, i);
    }
    b.iter(|| {
        for i in 0..5005i32 {
            treap.get(&i);
        }
    })
}

#[bench]
fn bench_btree_map_get(b: &mut Bencher) {
    let mut btree = BTreeMap::new();
    for i in 0..5000i32 {
        btree.insert(i, i);
    }
    b.iter(|| {
        for i in 0..5005i32 {
            btree.get(&i);
        }
    })
}

#[bench]
fn bench_treap_map_remove(b: &mut Bencher) {
    let mut treap = BTreeMap::new();
    for i in 0..5000i32 {
        treap.insert(i, i);
    }
    b.iter(|| {
        for i in 0..5005i32 {
            treap.remove(&i);
        }
    })
}

#[bench]
fn bench_btree_map_remove(b: &mut Bencher) {
    let mut btree = BTreeMap::new();
    for i in 0..5000i32 {
        btree.insert(i, i);
    }
    b.iter(|| {
        for i in 0..5005i32 {
            btree.remove(&i);
        }
    })
}
