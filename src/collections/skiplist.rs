use crate::collections::Entry;
use rand::Rng;
use std::alloc::Layout;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

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

#[repr(C)]
pub struct Node<K: Ord + Default, V: Default> {
    pub entry: Entry<K, V>,
    /// ranges [0, `MAX_LEVEL`]
    level: usize,
    /// the actual size is `level + 1`
    next: [AtomicPtr<Self>; 0],
}

impl<K: Ord + Default, V: Default> Node<K, V> {
    fn head() -> *mut Node<K, V> {
        Self::new_with_level(K::default(), V::default(), MAX_LEVEL)
    }

    fn new_with_level(key: K, value: V, level: usize) -> *mut Node<K, V> {
        let pointers_size = (level + 1) * std::mem::size_of::<AtomicPtr<Self>>();
        let layout = Layout::from_size_align(
            std::mem::size_of::<Self>() + pointers_size,
            std::mem::align_of::<Self>(),
        )
        .unwrap();
        unsafe {
            let node_ptr = std::alloc::alloc(layout) as *mut Self;
            let node = &mut *node_ptr;
            std::ptr::write(&mut node.entry, Entry { key, value });
            std::ptr::write(&mut node.level, level);
            std::ptr::write_bytes(node.next.as_mut_ptr(), 0, level + 1);
            node_ptr
        }
    }

    fn get_layout(&self) -> Layout {
        let pointers_size = (self.level + 1) * std::mem::size_of::<AtomicPtr<Self>>();

        Layout::from_size_align(
            std::mem::size_of::<Self>() + pointers_size,
            std::mem::align_of::<Self>(),
        )
        .unwrap()
    }

    #[inline]
    fn get_next(&self, level: usize) -> *mut Self {
        unsafe { self.next.get_unchecked(level).load(Ordering::Acquire) }
    }

    #[inline]
    fn set_next(&self, level: usize, node: *mut Self) {
        unsafe {
            self.next
                .get_unchecked(level)
                .store(node, Ordering::Release);
        }
    }
}

unsafe fn drop_node<K: Ord + Default, V: Default>(node: *mut Node<K, V>) {
    let layout = (*node).get_layout();
    std::ptr::drop_in_place(node as *mut Node<K, V>);
    std::alloc::dealloc(node as *mut u8, layout);
}

/// # NOTICE:
///
/// Concurrent insertion is not thread safe but concurrent reading with a
/// single writer is safe.
pub struct SkipMap<K: Ord + Default, V: Default> {
    head: *const Node<K, V>,
    cur_max_level: AtomicUsize,
    len: AtomicUsize,
}

unsafe impl<K: Ord + Default, V: Default> Send for SkipMap<K, V> {}
unsafe impl<K: Ord + Default, V: Default> Sync for SkipMap<K, V> {}

impl<K: Ord + Default, V: Default> SkipMap<K, V> {
    pub fn new() -> SkipMap<K, V> {
        SkipMap {
            head: Node::head(),
            cur_max_level: AtomicUsize::default(),
            len: AtomicUsize::default(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// # Safety
    /// node should be null or initialized
    pub unsafe fn node_lt_key(node: *mut Node<K, V>, key: &K) -> bool {
        !node.is_null() && (*node).entry.key.lt(key)
    }

    /// # Safety
    /// node should be null or initialized
    pub unsafe fn node_eq_key(node: *mut Node<K, V>, key: &K) -> bool {
        !node.is_null() && (*node).entry.key.eq(key)
    }

    pub fn find_first_ge(
        &self,
        key: &K,
        mut prev_nodes: Option<&mut [*const Node<K, V>]>,
    ) -> *mut Node<K, V> {
        let mut level = self.cur_max_level.load(Ordering::Acquire);
        let mut node = self.head;
        loop {
            unsafe {
                let next = (*node).get_next(level);
                if Self::node_lt_key(next, key) {
                    node = next
                } else {
                    if let Some(ref mut p) = prev_nodes {
                        debug_assert_eq!(p.len(), MAX_LEVEL + 1);
                        p[level] = node;
                    }
                    if level == 0 {
                        return next;
                    }
                    level -= 1;
                }
            }
        }
    }

    /// return whether `key` has already exist.
    pub fn insert(&self, key: K, value: V) -> bool {
        let mut prev_nodes = [self.head; MAX_LEVEL + 1];
        let node = self.find_first_ge(&key, Some(&mut prev_nodes));
        let has_key = unsafe { Self::node_eq_key(node, &key) };
        if has_key {
            unsafe {
                (*node).entry.value = value;
            }
        } else {
            self.insert_before(prev_nodes, key, value);
        }
        has_key
    }

    fn insert_before(&self, prev_nodes: [*const Node<K, V>; MAX_LEVEL + 1], key: K, value: V) {
        #[cfg(debug_assertions)]
        {
            for (level, prev) in prev_nodes.iter().enumerate() {
                unsafe {
                    debug_assert!((**prev).entry.key.le(&key));
                    Self::node_lt_key((**prev).get_next(level), &key);
                }
            }
        }

        let level = rand_level();
        if level > self.cur_max_level.load(Ordering::Acquire) {
            self.cur_max_level.store(level, Ordering::Release);
        }

        let new_node = Node::new_with_level(key, value, level);
        unsafe {
            for i in 0..=level {
                // set next of new_node first to ensure concurrent read is correct.
                (*new_node).set_next(i, (*(prev_nodes[i])).get_next(i));
                (*(prev_nodes[i])).set_next(i, new_node);
            }
        }

        self.len.fetch_add(1, Ordering::SeqCst);
    }

    /// Remove `key`, return whether `key` exists
    pub fn remove(&self, key: K) -> bool {
        let mut prev_nodes = [self.head; MAX_LEVEL + 1];
        let node = self.find_first_ge(&key, Some(&mut prev_nodes));
        let has_key = unsafe { Self::node_eq_key(node, &key) };
        if has_key {
            unsafe {
                for i in 0..=(*node).level {
                    (*prev_nodes[i]).set_next(i, (*node).get_next(i))
                }
                self.len.fetch_sub(1, Ordering::SeqCst);
                drop_node(node);
            }
            true
        } else {
            false
        }
    }

    pub fn iter(&self) -> Iter<K, V> {
        unsafe {
            Iter {
                node: (*self.head).get_next(0),
            }
        }
    }
}

impl<K: Ord + Default, V: Default> Default for SkipMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord + Default, V: Default> Drop for SkipMap<K, V> {
    fn drop(&mut self) {
        let mut node = self.head;

        unsafe {
            while !node.is_null() {
                let next_node = (*node).get_next(0);
                drop_node(node as *mut Node<K, V>);
                node = next_node;
            }
        }
    }
}

/// Iteration over the contents of a SkipMap
pub struct Iter<K: Ord + Default, V: Default> {
    node: *const Node<K, V>,
}

impl<K: Ord + Default, V: Default> Iterator for Iter<K, V> {
    type Item = *const Node<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.node.is_null() {
            None
        } else {
            let n = self.node;
            unsafe {
                self.node = (*self.node).get_next(0);
            }
            Some(n)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::collections::skiplist::SkipMap;

    #[test]
    fn test_insert() {
        let skip_map: SkipMap<i32, String> = SkipMap::new();
        for i in 0..40 {
            skip_map.insert(i, "temp".into());
        }
        for i in 0..100 {
            skip_map.insert(i, format!("value{}", i));
        }
        debug_assert_eq!(100, skip_map.len());
        for i in 0..100 {
            let node = skip_map.find_first_ge(&i, None);
            unsafe {
                assert_eq!(format!("value{}", i), (*node).entry.value);
            }
        }

        let mut count = 0;
        for node in skip_map.iter() {
            unsafe {
                assert_eq!(format!("value{}", count), (*node).entry.value);
            }
            count += 1;
        }
        assert_eq!(count, skip_map.len());
    }

    #[test]
    fn test_remove() {
        let skip_map: SkipMap<i32, String> = SkipMap::new();
        for i in 0..100 {
            skip_map.insert(i, format!("value{}", i));
        }
        for i in 1..99 {
            assert!(skip_map.remove(i));
        }
        assert_eq!(2, skip_map.len());
        let value = [0, 99];
        for (node, v) in skip_map.iter().zip(value.iter()) {
            unsafe {
                assert_eq!((*node).entry.key, *v);
            }
        }
        assert!(skip_map.remove(0));
        assert!(skip_map.remove(99));
        assert!(!skip_map.remove(0));
        assert_eq!(skip_map.len(), 0);
    }
}
