#![feature(test)]
extern crate test;
use test::Bencher;

use kvlite::collections::skip_list::arena::Arena;
use kvlite::collections::skip_list::MemoryAllocator;
use std::alloc::{GlobalAlloc, Layout};

const ALLOC_NUM: usize = 1000;

#[bench]
fn bench_std_malloc(b: &mut Bencher) {
    let mut ptrs = vec![];
    b.iter(|| {
        for _ in 0..ALLOC_NUM {
            unsafe {
                ptrs.push(std::alloc::alloc(Layout::new::<[u8; 10]>()));
            }
        }
    });
    for p in ptrs {
        unsafe { std::alloc::dealloc(p, Layout::new::<[u8; 10]>()) }
    }
}

#[bench]
fn bench_jemalloc(b: &mut Bencher) {
    let allocator = jemallocator::Jemalloc::default();
    let mut ptrs = vec![];
    b.iter(|| {
        for _ in 0..ALLOC_NUM {
            unsafe {
                ptrs.push(allocator.alloc(Layout::new::<[u8; 10]>()));
            }
        }
    });
    for p in ptrs {
        unsafe { allocator.dealloc(p, Layout::new::<[u8; 10]>()) }
    }
}

#[bench]
fn bench_arena(b: &mut Bencher) {
    let mut allocator = Arena::default();
    let mut ptrs = vec![];
    b.iter(|| {
        for _ in 0..ALLOC_NUM {
            ptrs.push(allocator.allocate_with_layout(Layout::new::<[u8; 10]>()));
        }
    });
}
