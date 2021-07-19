#![feature(test)]
extern crate kvlite;
extern crate test;

use kvlite::db::key_types::InternalKey;
use kvlite::memory::{
    BTreeMemTable, MemTable, MrSwSkipMapMemTable, MutexSkipMapMemTable, SkipMapMemTable,
};
use std::sync::Arc;
use test::Bencher;

const SIZE: u32 = 10000;

#[bench]
fn btree_map_insert(b: &mut Bencher) {
    b.iter(|| {
        let mut btree = BTreeMemTable::default();
        table_set(&mut btree);
        btree
    });
}

#[bench]
fn skip_map_insert(b: &mut Bencher) {
    b.iter(|| {
        let mut skip_map = MutexSkipMapMemTable::default();
        table_set(&mut skip_map);
        skip_map
    });
}

#[bench]
fn btree_map_get(b: &mut Bencher) {
    b.iter(|| {
        let mut btree = BTreeMemTable::default();
        table_set(&mut btree);
        table_get(&mut btree);
        btree
    });
}

#[bench]
fn skip_map_get(b: &mut Bencher) {
    b.iter(|| {
        let mut skip_map = MutexSkipMapMemTable::default();
        table_set(&mut skip_map);
        table_get(&mut skip_map);
        skip_map
    });
}

#[bench]
fn skip_map_mixed(b: &mut Bencher) {
    b.iter(|| {
        let skip_map = Arc::new(MutexSkipMapMemTable::default());
        let skip_map2 = skip_map.clone();
        let handle1 = std::thread::spawn(move || table_get_set(skip_map));
        let handle2 = std::thread::spawn(move || table_get_set(skip_map2));
        handle1.join().unwrap();
        handle2.join().unwrap();
    })
}

#[bench]
fn mrsw_skip_map_mixed(b: &mut Bencher) {
    b.iter(|| {
        let mrsw_skip_map = Arc::new(MrSwSkipMapMemTable::default());
        let mrsw2_skip_map = mrsw_skip_map.clone();
        let handle1 = std::thread::spawn(move || table_get_set(mrsw_skip_map));
        let handle2 = std::thread::spawn(move || table_get_set(mrsw2_skip_map));
        handle1.join().unwrap();
        handle2.join().unwrap();
    })
}

fn table_get_set(mem_table: Arc<impl MemTable<InternalKey, InternalKey>>) {
    for i in 0..SIZE {
        mem_table
            .set(Vec::from(i.to_be_bytes()), Vec::from(i.to_be_bytes()))
            .unwrap();
        mem_table.get(&Vec::from(i.to_be_bytes())).unwrap();
    }
    for i in 0..SIZE {
        mem_table
            .set(Vec::from(i.to_be_bytes()), Vec::from((i + 1).to_be_bytes()))
            .unwrap();
        mem_table.get(&Vec::from(i.to_be_bytes())).unwrap();
    }
}

fn table_set(mem_table: &mut impl MemTable<InternalKey, InternalKey>) {
    for i in 0..SIZE {
        mem_table
            .set(Vec::from(i.to_be_bytes()), Vec::from(i.to_be_bytes()))
            .unwrap();
    }
    for i in 0..SIZE {
        mem_table
            .set(Vec::from(i.to_be_bytes()), Vec::from((i + 1).to_be_bytes()))
            .unwrap();
    }
}

fn table_get(mem_table: &mut impl MemTable<InternalKey, InternalKey>) {
    for _ in 0..10 {
        for j in 0..SIZE + 10 {
            mem_table.get(&Vec::from(j.to_be_bytes())).unwrap();
        }
    }
}
