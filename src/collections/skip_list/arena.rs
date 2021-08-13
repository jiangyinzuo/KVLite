use crate::collections::skip_list::MemoryAllocator;
use std::alloc::Layout;
use std::mem::size_of;
use std::num::NonZeroUsize;

const BLOCK_SIZE: usize = 4096;

pub struct Arena {
    // Allocation state
    alloc_ptr: *mut u8,
    alloc_bytes_remaining: usize,

    // Array of new[] allocated memory blocks
    blocks: Vec<(*mut u8, usize)>,

    memory_usage: usize,
}

impl Default for Arena {
    fn default() -> Self {
        Arena {
            alloc_ptr: std::ptr::null_mut(),
            alloc_bytes_remaining: 0,
            blocks: Vec::with_capacity(2),
            memory_usage: 0,
        }
    }
}

impl MemoryAllocator for Arena {
    fn allocate_with_layout(&mut self, layout: Layout) -> *mut u8 {
        let offset = layout.align() - (self.alloc_ptr as usize & (layout.align() - 1));
        let bytes = (layout.size() + layout.align() - 1) & !(layout.align() - 1);
        unsafe {
            self.allocate(NonZeroUsize::new(bytes + offset).unwrap())
                .add(offset)
        }
    }

    fn allocate(&mut self, bytes: NonZeroUsize) -> *mut u8 {
        let bytes = bytes.get();
        if bytes <= self.alloc_bytes_remaining {
            let result = self.alloc_ptr;
            unsafe {
                self.alloc_ptr = self.alloc_ptr.add(bytes);
            }
            self.alloc_bytes_remaining -= bytes;
            result
        } else if bytes > BLOCK_SIZE / 4 {
            // Object is more than a quarter of our block size.  Allocate it separately
            // to avoid wasting too much space in leftover bytes.
            self.allocate_new_block(bytes)
        } else {
            // We waste the remaining space in the current block.
            self.alloc_ptr = self.allocate_new_block(BLOCK_SIZE);
            self.alloc_bytes_remaining = BLOCK_SIZE;

            let result = self.alloc_ptr;
            unsafe {
                self.alloc_ptr = self.alloc_ptr.add(bytes);
                self.alloc_bytes_remaining -= bytes;
            }
            result
        }
    }
}

impl Arena {
    #[inline]
    pub fn get_memory_usage(&self) -> usize {
        self.memory_usage
    }

    fn allocate_new_block(&mut self, bytes: usize) -> *mut u8 {
        let alloc_ptr = unsafe { std::alloc::alloc(Self::layout_by_bytes(bytes)) };
        self.blocks.push((alloc_ptr, bytes));
        self.memory_usage += bytes + size_of::<*mut u8>();
        alloc_ptr
    }

    #[inline]
    fn layout_by_bytes(bytes: usize) -> Layout {
        Layout::from_size_align(
            std::mem::size_of::<u8>() * bytes,
            std::mem::align_of::<u8>(),
        )
        .unwrap()
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        for &(ptr, bytes) in &self.blocks {
            unsafe {
                std::alloc::dealloc(ptr, Self::layout_by_bytes(bytes));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::collections::skip_list::arena::{Arena, BLOCK_SIZE};
    use crate::collections::skip_list::MemoryAllocator;
    use std::alloc::Layout;
    use std::num::NonZeroUsize;

    #[test]
    fn arena_test() {
        let mut arena = Arena::default();
        unsafe {
            let mut base = arena.allocate(NonZeroUsize::new_unchecked(1));
            assert!(arena.get_memory_usage() > 0);
            (*base) = 3;
            base = arena.allocate(NonZeroUsize::new_unchecked(2));
            (*base.add(1)) = 12u8;
            for size in [
                BLOCK_SIZE / 4 - 1,
                BLOCK_SIZE / 4,
                BLOCK_SIZE / 4 + 1,
                BLOCK_SIZE / 2,
                BLOCK_SIZE,
                BLOCK_SIZE * 2,
            ] {
                for _ in 0..10 {
                    base = arena.allocate(NonZeroUsize::new_unchecked(size));
                    (*base.add(BLOCK_SIZE / 4)) = 111u8;
                }
            }
        }
    }

    #[test]
    fn layout_test() {
        let mut arena = Arena::default();
        let layout = Layout::from_size_align(5, 4).unwrap();
        assert_eq!(arena.allocate_with_layout(layout) as usize % 4, 0);
        let layout = Layout::from_size_align(19, 2).unwrap();
        assert_eq!(arena.allocate_with_layout(layout) as usize % 2, 0);
        arena.allocate_with_layout(layout);
        let layout = Layout::from_size_align(22, 8).unwrap();
        assert_eq!(arena.allocate_with_layout(layout) as usize % 8, 0);
    }
}
