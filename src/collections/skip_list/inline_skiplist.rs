use std::sync::atomic::{AtomicPtr, Ordering};

#[repr(C)]
pub struct InlineNode {
    // next[0] is the lowest level link (level 0).  Higher levels are
    // stored _earlier_, so level 1 is at next[-1]
    next: [AtomicPtr<InlineNode>; 1],
}

impl InlineNode {
    #[inline]
    pub fn key(&self) -> *const u8 {
        unsafe { (self.next.get_unchecked(1)) as *const AtomicPtr<InlineNode> as *const _ }
    }

    pub fn get_next(&self, n: usize) -> *const Self {
        unsafe { (*(self.next.as_ptr().sub(n))).load(Ordering::Acquire) }
    }
}
