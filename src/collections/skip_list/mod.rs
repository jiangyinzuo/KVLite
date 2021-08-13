pub mod arena;
pub mod inline_skiplist;
pub mod skipmap;

use rand::Rng;
use std::alloc::Layout;
use std::num::NonZeroUsize;

pub const MAX_LEVEL: usize = 12;

fn rand_level() -> usize {
    let mut rng = rand::thread_rng();
    let mut level = 0;
    while level < MAX_LEVEL {
        let number = rng.gen_range(1..=4);
        if number == 1 {
            level += 1;
        } else {
            break;
        }
    }
    level
}

pub trait MemoryAllocator: Default {
    /// Return a pointer to a newly allocated memory block with `layout`.
    fn allocate_with_layout(&mut self, layout: Layout) -> *mut u8;

    /// Return a pointer to a newly allocated memory block of `bytes` bytes.
    fn allocate(&mut self, bytes: NonZeroUsize) -> *mut u8;
}
