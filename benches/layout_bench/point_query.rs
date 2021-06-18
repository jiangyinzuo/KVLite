#![feature(test)]
extern crate kvlite;
extern crate layout;
extern crate test;

use layout::dsm::setup_column_major;
use layout::nsm::{setup_column_group, setup_row_major};
use layout::pax::setup_pax;
use test::Bencher;

const MAX_PK: u64 = 10000;

#[bench]
fn row_major(b: &mut Bencher) {
    let (_temp_dir, mut nsms) = setup_row_major::<4>();
    b.iter(|| {
        for pk in 0..MAX_PK {
            let res = nsms[0].single_query(pk);
            for i in res {
                assert_eq!(i, pk);
            }
        }
    })
}

#[bench]
fn row_major_mmap(b: &mut Bencher) {
    let (_temp_dir, mut nsms) = setup_row_major::<4>();
    b.iter(|| {
        for pk in 0..MAX_PK {
            let res = nsms[0].mmap_single_query(pk);
            for i in res {
                assert_eq!(i, pk);
            }
        }
    })
}

#[bench]
fn column_group(b: &mut Bencher) {
    let (_temp_dir, mut cgs) = setup_column_group();
    b.iter(|| {
        let mut count = 0;
        for pk in 0..MAX_PK {
            for i in [0, 2] {
                let res = cgs[i].single_query(pk);
                for i in res {
                    assert_eq!(i, pk);
                    count += 1;
                }
            }
        }
        assert_eq!(count, MAX_PK * 4);
    })
}

#[bench]
fn column_group_mmap(b: &mut Bencher) {
    let (_temp_dir, mut cgs) = setup_column_group();
    b.iter(|| {
        let mut count = 0;
        for pk in 0..MAX_PK {
            for i in [0, 2] {
                let res = cgs[i].mmap_single_query(pk);
                for i in res {
                    assert_eq!(i, pk);
                    count += 1;
                }
            }
        }
        assert_eq!(count, MAX_PK * 4);
    })
}

#[bench]
fn column_major(b: &mut Bencher) {
    let (_temp_dir, mut dsms) = setup_column_major();
    b.iter(|| {
        let mut count = 0;
        for pk in 0..MAX_PK {
            for dsm in &mut dsms {
                let value = dsm.single_query(pk);
                assert_eq!(value, pk);
                count += 1;
            }
        }
        assert_eq!(count, MAX_PK * 4);
    })
}

#[bench]
fn column_major_mmap(b: &mut Bencher) {
    let (_temp_dir, mut dsms) = setup_column_major();
    b.iter(|| {
        let mut count = 0;
        for pk in 0..MAX_PK {
            for dsm in &mut dsms {
                let value = dsm.mmap_single_query(pk);
                assert_eq!(value, pk);
                count += 1;
            }
        }
        assert_eq!(count, MAX_PK * 4);
    })
}

#[bench]
fn pax(b: &mut Bencher) {
    let (_temp_dir, mut paxes) = setup_pax();
    b.iter(|| {
        let mut count = 0;
        for pk in 0..MAX_PK {
            let res = paxes[0].single_query(pk);
            for i in res {
                assert_eq!(i, pk);
                count += 1;
            }
        }
        assert_eq!(count, MAX_PK * 4);
    })
}

#[bench]
fn pax_mmap(b: &mut Bencher) {
    let (_temp_dir, mut paxes) = setup_pax();
    b.iter(|| {
        let mut count = 0;
        for pk in 0..MAX_PK {
            let res = paxes[0].mmap_single_query(pk);
            for i in res {
                assert_eq!(i, pk);
                count += 1;
            }
        }
        assert_eq!(count, MAX_PK * 4);
    })
}
