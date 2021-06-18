#![feature(test)]
extern crate kvlite;
extern crate layout;
extern crate test;

use layout::dsm::setup_column_major;
use layout::nsm::{setup_column_group, setup_row_major};
use layout::pax::setup_pax;
use test::Bencher;

#[bench]
fn row_major(b: &mut Bencher) {
    let (_temp_dir, mut nsms) = setup_row_major::<4>();
    b.iter(|| {
        let mut res = 0;
        for nsm in &mut nsms {
            res += nsm.read_sum();
        }
        assert_eq!(799980000, res);
    })
}

#[bench]
fn row_major_mmap(b: &mut Bencher) {
    let (_temp_dir, mut nsms) = setup_row_major::<4>();
    b.iter(|| {
        let mut res = 0;
        for nsm in &mut nsms {
            res += nsm.mmap_read_sum();
        }
        assert_eq!(799980000, res);
    })
}

#[bench]
fn column_group(b: &mut Bencher) {
    let (_temp_dir, mut cgs) = setup_column_group();
    b.iter(|| {
        let mut res = 0;
        for cg in &mut cgs[0..2usize] {
            res += cg.read_sum();
        }
        assert_eq!(799980000, res);
    })
}

#[bench]
fn column_group_mmap(b: &mut Bencher) {
    let (_temp_dir, mut cgs) = setup_column_group();
    b.iter(|| {
        let mut res = 0;
        for cg in &mut cgs[0..2usize] {
            res += cg.mmap_read_sum();
        }
        assert_eq!(799980000, res);
    })
}

#[bench]
fn column_major(b: &mut Bencher) {
    let (_temp_dir, mut dsms) = setup_column_major();
    b.iter(|| {
        let res = dsms[0].read_sum();
        assert_eq!(799980000, res);
    })
}

#[bench]
fn column_major_mmap(b: &mut Bencher) {
    let (_temp_dir, mut dsms) = setup_column_major();
    b.iter(|| {
        let res = dsms[0].mmap_read_sum();
        assert_eq!(799980000, res);
    })
}

#[bench]
fn pax(b: &mut Bencher) {
    let (_temp_dir, mut paxes) = setup_pax();
    b.iter(|| {
        let mut res = 0;
        for pax in &mut paxes {
            res += pax.read_sum();
        }
        assert_eq!(799980000, res);
    })
}

#[bench]
fn pax_mmap(b: &mut Bencher) {
    let (_temp_dir, mut paxes) = setup_pax();
    b.iter(|| {
        let mut res = 0;
        for pax in &mut paxes {
            res += pax.mmap_read_sum();
        }
        assert_eq!(799980000, res);
    })
}
