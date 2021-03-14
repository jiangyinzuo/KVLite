#![feature(test)]
extern crate kvlite;
extern crate test;

use kvlite::memory::{BTreeMemTable, MemTable, SkipMapMemTable};
use test::Bencher;

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

fn table_set(mem_table: &mut impl MemTable) {
    for i in 0..250i32 {
        mem_table.set(format!("{}", i), format!("{}", i)).unwrap();
    }
    for i in 0..250i32 {
        mem_table
            .set(format!("{}", i), format!("{}", i + 1))
            .unwrap();
    }
}

fn table_get(mem_table: &mut impl MemTable) {
    for _ in 0..10 {
        for j in 0..260 {
            mem_table.get(&format!("{}", j)).unwrap();
        }
    }
}
