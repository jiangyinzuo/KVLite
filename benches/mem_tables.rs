#![feature(test)]
extern crate kvlite;
extern crate test;

use kvlite::db::key_types::UserKey;
use kvlite::memory::{BTreeMemTable, MemTable, SkipMapMemTable};
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
        let mut skip_map = SkipMapMemTable::default();
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
        let mut skip_map = SkipMapMemTable::default();
        table_set(&mut skip_map);
        table_get(&mut skip_map);
        skip_map
    });
}

fn table_set(mem_table: &mut impl MemTable<UserKey>) {
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

fn table_get(mem_table: &mut impl MemTable<UserKey>) {
    for _ in 0..10 {
        for j in 0..SIZE + 10 {
            mem_table.get(&Vec::from(j.to_be_bytes())).unwrap();
        }
    }
}
