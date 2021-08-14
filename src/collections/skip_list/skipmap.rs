use crate::collections::skip_list::{rand_level, MemoryAllocator, MAX_LEVEL};
use crate::collections::Entry;
use std::alloc::Layout;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};

pub type SrSwSkipMap<K, V> = SkipMap<K, V, { SrSw }>;
pub type MrMwSkipMap<K, V> = SkipMap<K, V, { MrMw }>;
pub type MrSwSkipMap<K, V> = SkipMap<K, V, { MrSw }>;

const LOCK_MASK: usize = 1 << (std::mem::size_of::<usize>() * 8 - 1);

#[repr(i8)]
#[derive(Eq, PartialEq)]
pub enum ReadWriteMode {
    SrSw,
    MrSw,
    MrMw,
}

use crate::collections::skip_list::arena::Arena;
use std::cell::Cell;
use std::thread::sleep;
use std::time::Duration;
use ReadWriteMode::*;

#[repr(C)]
pub struct Node<K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> {
    pub entry: Entry<K, V>,

    /// 1bit(inserted) | 63bit(level)
    /// level ranges [0, `MAX_LEVEL`]
    bit_field: usize,
    /// the actual size is `level + 1`
    next: [*mut Self; 0],
}

impl<K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> Node<K, V, { RW_MODE }> {
    #[inline]
    fn head() -> *mut Node<K, V, { RW_MODE }> {
        Self::new_with_level(K::default(), V::default(), MAX_LEVEL)
    }

    fn new_with_level(key: K, value: V, level: usize) -> *mut Node<K, V, { RW_MODE }> {
        let pointers_size = (level + 1) * std::mem::size_of::<*mut Self>();
        let layout = Layout::from_size_align(
            std::mem::size_of::<Self>() + pointers_size,
            std::mem::align_of::<Self>(),
        )
        .unwrap();
        unsafe {
            let node_ptr = std::alloc::alloc(layout) as *mut Self;
            let node = &mut *node_ptr;
            std::ptr::write(&mut node.entry, Entry { key, value });
            std::ptr::write(&mut node.bit_field, level);
            std::ptr::write_bytes(node.next.as_mut_ptr(), 0, level + 1);
            node_ptr
        }
    }

    fn get_level(&self) -> usize {
        match RW_MODE {
            SrSw => self.bit_field,
            MrSw => unsafe { std::intrinsics::atomic_load_acq(&self.bit_field) },
            MrMw => unsafe { std::intrinsics::atomic_load_acq(&self.bit_field) & (!LOCK_MASK) },
        }
    }

    fn get_layout(&self) -> Layout {
        let pointers_size = (self.get_level() + 1) * std::mem::size_of::<*mut Self>();

        Layout::from_size_align(
            std::mem::size_of::<Self>() + pointers_size,
            std::mem::align_of::<Self>(),
        )
        .unwrap()
    }

    #[inline]
    pub fn get_next(&self, level: usize) -> *mut Self {
        unsafe {
            let p = self.next.get_unchecked(level);
            match RW_MODE {
                ReadWriteMode::MrSw | ReadWriteMode::MrMw => std::intrinsics::atomic_load_acq(p),
                ReadWriteMode::SrSw => *p,
            }
        }
    }

    fn lock_insertion(&self) {
        let level = self.get_level();
        unsafe {
            let mut count = 0;
            let p = &self.bit_field as *const usize as *mut usize;
            debug_assert!(!p.is_null());
            while !(std::intrinsics::atomic_cxchg_acq(p, level, level ^ LOCK_MASK)).1 {
                count += 1;
                if count == 100 {
                    count = 0;
                    warn!(
                        "too many competitors, thread sleeping... {}, {}",
                        level, self.bit_field
                    );
                    sleep(Duration::from_micros(
                        (rand::random::<u64>() & 0xff) + 100u64,
                    ))
                }
            }
        }
        debug_assert!(self.bit_field >= LOCK_MASK);
    }

    fn unlock_insertion(&self) {
        unsafe {
            debug_assert!(self.bit_field >= LOCK_MASK);
            std::intrinsics::atomic_xor_rel(&self.bit_field as *const _ as *mut _, LOCK_MASK);
        }
    }

    #[inline]
    fn set_next(&mut self, level: usize, node: *mut Self) {
        unsafe {
            let p = self.next.get_unchecked_mut(level);
            match RW_MODE {
                ReadWriteMode::MrSw | ReadWriteMode::MrMw => {
                    std::intrinsics::atomic_store_rel(p, node)
                }
                ReadWriteMode::SrSw => *p = node,
            }
        }
    }

    /// # Safety
    /// node s
    /// hould be null or initialized
    pub unsafe fn node_cmp(node: *mut Node<K, V, RW_MODE>, key: &K) -> std::cmp::Ordering {
        if node.is_null() {
            return std::cmp::Ordering::Greater;
        }
        (*node).entry.key.cmp(key)
    }

    /// # Example
    ///
    /// ```rust
    /// use kvlite::collections::skip_list::skipmap::{SrSwSkipMap, ReadWriteMode, Node};
    /// let mut skip_map: SrSwSkipMap<i32, i32> = SrSwSkipMap::new();
    /// for i in 1..10 {
    ///     skip_map.insert(i, i + 1);
    /// }
    /// let node = skip_map.find_first_ge(&3, None);
    /// unsafe {
    ///     let node = Node::find_first_ge_from_node(node, &7);
    ///     assert_eq!((*node).entry.value, 8);
    /// }
    /// ```
    /// # Safety
    /// `node` should be a part of skip-map and should not be nullptr
    pub unsafe fn find_first_ge_from_node(
        mut node: *mut Node<K, V, RW_MODE>,
        key: &K,
    ) -> *mut Node<K, V, RW_MODE> {
        debug_assert!(!node.is_null());
        if (*node).entry.key.eq(key) {
            return node;
        }
        let mut level = (*node).get_level();
        loop {
            let next = (*node).get_next(level);
            match Self::node_cmp(next, key) {
                std::cmp::Ordering::Greater => {
                    if level == 0 {
                        return next;
                    }
                    level -= 1;
                }
                std::cmp::Ordering::Equal => {
                    return next;
                }
                std::cmp::Ordering::Less => {
                    node = next;
                }
            }
        }
    }

    /// # Example
    ///
    /// ```rust
    /// use kvlite::collections::skip_list::skipmap::{SrSwSkipMap, ReadWriteMode, Node};
    /// let mut skip_map: SrSwSkipMap<i32, i32> = SrSwSkipMap::new();
    /// for i in 1..10 {
    ///     skip_map.insert(i, i + 1);
    /// }
    /// let node = skip_map.find_first_ge(&3, None);
    /// unsafe {
    ///     let node = Node::find_last_le_from_node(node, &9);
    ///     assert_eq!((*node).entry.value, 10);
    ///     let node2 = Node::find_last_le_from_node(node, &-123);
    ///     assert_eq!(node, node2);
    /// }
    /// ```
    ///
    /// # Safety
    /// `node` should be a part of skip-map and should not be nullptr
    pub unsafe fn find_last_le_from_node(
        mut node: *mut Node<K, V, RW_MODE>,
        key: &K,
    ) -> *mut Node<K, V, RW_MODE> {
        debug_assert!(!node.is_null());
        if (*node).entry.key.eq(key) {
            return node;
        }
        let mut level = (*node).get_level();
        loop {
            let next = (*node).get_next(level);
            match Self::node_cmp(next, key) {
                std::cmp::Ordering::Greater => {
                    if level == 0 {
                        return node;
                    }
                    level -= 1;
                }
                std::cmp::Ordering::Equal => {
                    return next;
                }
                std::cmp::Ordering::Less => {
                    node = next;
                }
            }
        }
    }
}

unsafe fn drop_node<K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode>(
    node: *mut Node<K, V, RW_MODE>,
) {
    let layout = (*node).get_layout();
    std::ptr::drop_in_place(node as *mut Node<K, V, RW_MODE>);
    std::alloc::dealloc(node as *mut u8, layout);
}

/// Map that allows duplicate keys, based on skip list
///
/// # NOTICE:
///
/// SkipMap is not thread-safe.
pub struct SkipMap<K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> {
    dummy_head: *const Node<K, V, { RW_MODE }>,
    tail_lock: AtomicBool,
    tail: AtomicPtr<Node<K, V, { RW_MODE }>>,
    cur_max_level: AtomicUsize,
    len: AtomicUsize,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

unsafe impl<K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> Send
    for SkipMap<K, V, RW_MODE>
{
}

unsafe impl<K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> Sync
    for SkipMap<K, V, RW_MODE>
{
}

impl<SK: Ord + Default, V: Default> SkipMap<SK, V, { SrSw }> {
    /// Remove all the `key` in map, return whether `key` exists
    pub fn remove(&mut self, key: SK) -> bool {
        let mut prev_nodes = [self.dummy_head as *mut _; MAX_LEVEL + 1];
        let mut node = self.find_first_ge(&key, Some(&mut prev_nodes));
        let has_key = unsafe { Self::node_eq_key(node, &key) };
        if has_key {
            unsafe {
                while !node.is_null() && Self::node_eq_key(node, &key) {
                    let next_node = (*node).get_next(0);
                    for i in 0..=(*node).get_level() {
                        (*prev_nodes[i]).set_next(i, (*node).get_next(i))
                    }
                    self.len.fetch_sub(1, Ordering::Release);
                    if next_node.is_null() {
                        self.tail
                            .store(*prev_nodes.get_unchecked(0) as *mut _, Ordering::SeqCst);
                    }

                    // default allocator needs manually drop
                    drop_node(node);
                    node = next_node;
                }
            }
            true
        } else {
            false
        }
    }
}

impl<SK: Ord + Default, V: Default> SkipMap<SK, V, { MrMw }> {
    /// return whether `key` has already exist.
    #[inline]
    pub fn insert_single_writer(&self, key: SK, mut value: V) -> Option<V> {
        self.insert_inner::<true>(key, value)
    }

    #[inline]
    pub fn merge_single_writer(&self, other: SrSwSkipMap<SK, V>) {
        self.merge_inner::<true>(other)
    }
}

impl<SK: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> SkipMap<SK, V, RW_MODE> {
    pub fn new() -> SkipMap<SK, V, RW_MODE> {
        let mut dummy_head = Node::head();
        SkipMap {
            dummy_head,
            tail_lock: AtomicBool::new(false),
            tail: AtomicPtr::default(),
            cur_max_level: AtomicUsize::default(),
            len: AtomicUsize::default(),
            _key: PhantomData,
            _value: PhantomData,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// # Safety
    /// node should be null or initialized
    pub unsafe fn node_lt_key(node: *mut Node<SK, V, RW_MODE>, key: &SK) -> bool {
        !node.is_null() && (*node).entry.key.lt(key)
    }

    /// # Safety
    /// node should be null or initialized
    pub unsafe fn node_eq_key(node: *mut Node<SK, V, RW_MODE>, key: &SK) -> bool {
        !node.is_null() && (*node).entry.key.eq(key)
    }

    /// Return the first node `N` whose key is greater or equal than given `key`.
    /// if `prev_nodes` is `Some(...)`, it will be assigned to all the previous nodes of `N`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kvlite::collections::skip_list::skipmap::{SrSwSkipMap, ReadWriteMode};
    /// let mut skip_map: SrSwSkipMap<i32, i32> = SrSwSkipMap::new();
    /// assert!(skip_map.find_first_ge(&1, None).is_null());
    /// skip_map.insert(3, 3);
    /// assert!(skip_map.find_first_ge(&5, None).is_null());
    /// ```
    pub fn find_first_ge(
        &self,
        key: &SK,
        mut prev_nodes: Option<&mut [*mut Node<SK, V, RW_MODE>; MAX_LEVEL + 1]>,
    ) -> *mut Node<SK, V, RW_MODE> {
        let mut level = self.cur_max_level.load(Ordering::Acquire);
        let mut node = self.dummy_head as *mut Node<SK, V, RW_MODE>;
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

    fn update_first_ge(
        &self,
        key: &SK,
        prev_nodes: &mut [*mut Node<SK, V, RW_MODE>; MAX_LEVEL + 1],
    ) {
        for (l, prev_node) in prev_nodes.iter_mut().enumerate() {
            let mut next_node;
            unsafe {
                while {
                    next_node = (**prev_node).get_next(l);
                    Self::node_lt_key(next_node, key)
                } {
                    *prev_node = next_node;
                }
            }
        }
    }

    /// Return the last node whose key is less than or equal to `key`,
    /// if does not exist, return nullptr.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kvlite::collections::skip_list::skipmap::{SrSwSkipMap, ReadWriteMode};
    /// let mut skip_map: SrSwSkipMap<i32, i32> = SrSwSkipMap::new();
    /// assert!(skip_map.find_last_le(&1).is_null());
    /// skip_map.insert(3, 3);
    /// skip_map.insert(7, 7);
    ///
    /// let node = skip_map.find_last_le(&7);
    /// unsafe {
    ///     assert_eq!((*node).entry.key, 7);
    /// }
    ///
    /// let node = skip_map.find_last_le(&6);
    /// unsafe {
    ///     assert_eq!((*node).entry.key, 3);
    /// }
    /// ```
    pub fn find_last_le(&self, key: &SK) -> *mut Node<SK, V, RW_MODE> {
        let mut level = self.cur_max_level.load(Ordering::Acquire);
        let mut node = self.dummy_head as *mut Node<SK, V, RW_MODE>;

        let result = loop {
            let next = unsafe { (*node).get_next(level) };
            match unsafe { Node::node_cmp(next, key) } {
                std::cmp::Ordering::Equal => return next,
                std::cmp::Ordering::Less => {
                    node = next;
                }
                std::cmp::Ordering::Greater => {
                    if level == 0 {
                        break node;
                    }
                    level -= 1;
                }
            }
        };
        if result == self.dummy_head as *mut _ {
            std::ptr::null_mut()
        } else {
            result
        }
    }

    pub fn get_clone(&self, key: &SK) -> Option<V>
    where
        V: Clone,
    {
        let node = self.find_first_ge(key, None);
        unsafe {
            if node.is_null() || (*node).entry.key.ne(key) {
                None
            } else {
                Some((*node).entry.value.clone())
            }
        }
    }

    pub fn range_get<UK>(&self, key_start: &SK, key_end: &SK, kvs: &mut SrSwSkipMap<UK, V>)
    where
        SK: Clone + Into<UK>,
        UK: Ord + Default,
        V: Clone,
    {
        let mut node = self.find_first_ge(key_start, None);
        unsafe {
            while !node.is_null() && (*node).entry.key.le(key_end) {
                kvs.insert(
                    (*node).entry.key.clone().into(),
                    (*node).entry.value.clone(),
                );
                node = (*node).get_next(0);
            }
        }
    }

    #[inline]
    pub fn merge(&self, other: SrSwSkipMap<SK, V>) {
        self.merge_inner::<false>(other)
    }

    fn merge_inner<const INSURE_SINGLE_WRITER: bool>(&self, other: SrSwSkipMap<SK, V>) {
        // todo: optimize the time complexity
        for n in other.into_ptr_iter() {
            unsafe {
                let kv: Entry<SK, V> = std::mem::take(&mut (*n).entry);
                self.insert_inner::<INSURE_SINGLE_WRITER>(kv.key, kv.value);
            }
        }
    }

    fn lock_tail_insertion(&self) {
        let mut count = 0;
        while self
            .tail_lock
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            count += 1;
            if count == 100 {
                count = 0;
                warn!("to many competitors in tail_insertion");
                sleep(Duration::from_micros(
                    (rand::random::<u64>() & 0xff) + 100u64,
                ));
            }
        }
    }

    fn unlock_tail_insertion(&self) {
        let old_value = self.tail_lock.swap(false, Ordering::AcqRel);
        debug_assert!(old_value);
    }

    /// return whether `key` has already exist.
    pub fn insert(&self, key: SK, mut value: V) -> Option<V> {
        self.insert_inner::<false>(key, value)
    }

    /// return whether `key` has already exist.
    fn insert_inner<const INSURE_SINGLE_WRITER: bool>(&self, key: SK, mut value: V) -> Option<V> {
        let mut prev_nodes = [self.dummy_head as *mut _; MAX_LEVEL + 1];
        let node = self.find_first_ge(&key, Some(&mut prev_nodes));

        if let MrMw = RW_MODE {
            if !INSURE_SINGLE_WRITER {
                if node.is_null() {
                    self.lock_tail_insertion();
                } else {
                    unsafe {
                        (*node).lock_insertion();
                    }
                }
                self.update_first_ge(&key, &mut prev_nodes);
            }
        }

        let has_key = unsafe { Self::node_eq_key(node, &key) };
        let result = if has_key {
            unsafe {
                std::mem::swap(&mut (*node).entry.value, &mut value);
            }
            Some(value)
        } else {
            self.insert_after(prev_nodes, key, value);
            None
        };

        if let MrMw = RW_MODE {
            if !INSURE_SINGLE_WRITER {
                if node.is_null() {
                    self.unlock_tail_insertion();
                } else {
                    unsafe {
                        (*node).unlock_insertion();
                    }
                }
            }
        }
        result
    }

    /// Insert node with `key`, `value` after `prev_nodes`
    fn insert_after(
        &self,
        prev_nodes: [*mut Node<SK, V, RW_MODE>; MAX_LEVEL + 1],
        key: SK,
        value: V,
    ) {
        #[cfg(debug_assertions)]
        {
            for (level, prev) in prev_nodes.iter().enumerate() {
                unsafe {
                    if (*prev) != self.dummy_head as *mut _ {
                        let prev_key = &(**prev).entry.key;
                        if !prev_key.lt(&key) {
                            println!("??");
                        }
                        debug_assert!(prev_key.lt(&key));
                    }

                    debug_assert!(!Self::node_lt_key((**prev).get_next(level), &key));
                }
            }
        }

        let level = rand_level();
        if level > self.cur_max_level.load(Ordering::Acquire) {
            self.cur_max_level.store(level, Ordering::Release);
        }

        let new_node = Node::new_with_level(key, value, level);
        unsafe {
            if (*(*prev_nodes.get_unchecked(0))).get_next(0).is_null() {
                self.tail.store(new_node, Ordering::Release);
            }

            for i in 0..=level {
                // set next of new_node first to ensure concurrent read is correct.
                (*new_node).set_next(i, (*(prev_nodes[i])).get_next(i));
                (*(prev_nodes[i])).set_next(i, new_node);
            }
        }

        self.len.fetch_add(1, Ordering::Release);
    }

    /// Get first real node of SkipMap
    pub fn first_node(&self) -> *const Node<SK, V, RW_MODE> {
        unsafe { (*self.dummy_head).get_next(0) }
    }

    pub fn iter_ptr<'a>(&self) -> IterPtr<'a, SK, V, RW_MODE> {
        unsafe {
            IterPtr {
                node: self.first_node(),
                _marker: PhantomData,
            }
        }
    }

    pub fn iter<'a>(&self) -> Iter<'a, SK, V, RW_MODE> {
        unsafe {
            Iter {
                node: (*self.dummy_head).get_next(0),
                _marker: PhantomData,
            }
        }
    }

    /// Get first key-value pair.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kvlite::collections::skip_list::skipmap::{SrSwSkipMap, ReadWriteMode};
    /// let mut skip_map: SrSwSkipMap<&str, i32> = SrSwSkipMap::new();
    /// assert!(skip_map.first_key_value().is_none());
    ///
    /// skip_map.insert("hello", 2);
    /// skip_map.insert("apple", 1);
    /// let entry = skip_map.first_key_value().unwrap();
    /// assert_eq!(entry.key, "apple");
    /// assert_eq!(entry.value, 1);
    /// ```
    pub fn first_key_value(&self) -> Option<&Entry<SK, V>> {
        if self.is_empty() {
            None
        } else {
            unsafe { Some(&(*(*self.dummy_head).get_next(0)).entry) }
        }
    }

    /// Get last key-value pair.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kvlite::collections::skip_list::skipmap::{SrSwSkipMap, ReadWriteMode};
    /// let mut skip_map: SrSwSkipMap<&str, i32> = SrSwSkipMap::new();
    /// assert!(skip_map.last_key_value().is_none());
    ///
    /// skip_map.insert("hello", 2);
    /// skip_map.insert("apple", 1);
    /// let entry = skip_map.last_key_value().unwrap();
    /// assert_eq!(entry.key, "hello");
    /// assert_eq!(entry.value, 2);
    /// ```
    pub fn last_key_value(&self) -> Option<&Entry<SK, V>> {
        if self.is_empty() {
            None
        } else {
            Some(unsafe { &(*self.tail.load(Ordering::Acquire)).entry })
        }
    }

    pub fn into_ptr_iter(self) -> IntoPtrIter<SK, V, RW_MODE> {
        unsafe {
            let node = (*self.dummy_head).get_next(0);
            IntoPtrIter { _inner: self, node }
        }
    }
}

impl<K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> Default
    for SkipMap<K, V, RW_MODE>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> Drop for SkipMap<K, V, RW_MODE> {
    fn drop(&mut self) {
        let mut node = self.dummy_head;

        unsafe {
            while !node.is_null() {
                let next_node = (*node).get_next(0);
                drop_node(node as *mut Node<K, V, RW_MODE>);
                node = next_node;
            }
        }
    }
}

pub struct Iter<'a, K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> {
    node: *const Node<K, V, RW_MODE>,
    _marker: PhantomData<&'a Node<K, V, RW_MODE>>,
}

impl<'a, K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> Iterator
    for Iter<'a, K, V, RW_MODE>
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.node.is_null() {
            None
        } else {
            let n = self.node;
            unsafe {
                self.node = (*self.node).get_next(0);
                Some((&(*n).entry.key, &(*n).entry.value))
            }
        }
    }
}

/// Iteration over the contents of a SkipMap
pub struct IterPtr<'a, K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> {
    node: *const Node<K, V, RW_MODE>,
    _marker: PhantomData<&'a Node<K, V, RW_MODE>>,
}

impl<'a, K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> IterPtr<'a, K, V, RW_MODE> {
    pub fn current_no_consume(&self) -> *const Node<K, V, RW_MODE> {
        self.node
    }

    /// # Notice
    ///
    /// Make sure `self.node` is not null.
    pub fn next_node(&mut self) -> *const Node<K, V, RW_MODE> {
        debug_assert!(!self.node.is_null());
        unsafe {
            self.node = (*self.node).get_next(0);
        }
        self.node
    }
}

impl<'a, K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> Iterator
    for IterPtr<'a, K, V, RW_MODE>
{
    type Item = *const Node<K, V, RW_MODE>;

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

pub struct IntoPtrIter<K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> {
    _inner: SkipMap<K, V, RW_MODE>,
    node: *mut Node<K, V, RW_MODE>,
}

impl<K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> IntoPtrIter<K, V, RW_MODE> {
    pub fn current_mut_no_consume(&self) -> *mut Node<K, V, RW_MODE> {
        self.node as *mut _
    }

    /// # Notice
    ///
    /// Make sure `self.node` is not null.
    pub fn next_node(&mut self) -> *mut Node<K, V, RW_MODE> {
        debug_assert!(!self.node.is_null());
        unsafe {
            self.node = (*self.node).get_next(0);
        }
        self.node
    }
}

impl<K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> Iterator
    for IntoPtrIter<K, V, RW_MODE>
{
    type Item = *mut Node<K, V, RW_MODE>;

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

pub struct IntoIter<K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> {
    _inner: SkipMap<K, V, RW_MODE>,
    node: *mut Node<K, V, RW_MODE>,
}

impl<K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> Iterator
    for IntoIter<K, V, RW_MODE>
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.node.is_null() {
            None
        } else {
            let n = self.node;
            unsafe {
                self.node = (*self.node).get_next(0);
                let entry = std::mem::take(&mut (*n).entry);
                Some(entry.key_value())
            }
        }
    }
}

impl<K: Ord + Default, V: Default, const RW_MODE: ReadWriteMode> IntoIterator
    for SkipMap<K, V, RW_MODE>
{
    type Item = (K, V);
    type IntoIter = IntoIter<K, V, RW_MODE>;

    fn into_iter(self) -> Self::IntoIter {
        unsafe {
            let node = (*self.dummy_head).get_next(0);
            IntoIter { _inner: self, node }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::collections::skip_list::skipmap::ReadWriteMode::{MrSw, SrSw};
    use crate::collections::skip_list::skipmap::SrSwSkipMap;
    use crate::db::no_transaction_db::tests::create_random_map;
    use rand::Rng;

    #[test]
    fn test_key() {
        let mut skip_map: SrSwSkipMap<i32, i32> = SrSwSkipMap::new();
        skip_map.insert(1, 1);
        skip_map.insert(1, 2);
        assert_eq!(skip_map.len(), 1);

        assert!(skip_map.remove(1));
        assert!(!skip_map.remove(1));
        assert!(skip_map.is_empty());
    }

    #[test]
    fn test_insert() {
        let skip_map: SrSwSkipMap<i32, String> = SrSwSkipMap::new();
        for i in 0..100 {
            skip_map.insert(i, format!("value{}", i));
            assert_eq!(skip_map.last_key_value().unwrap().key, i);
        }
        debug_assert_eq!(100, skip_map.len());
        for i in 0..100 {
            let node = skip_map.find_first_ge(&i, None);
            unsafe {
                assert_eq!(format!("value{}", i), (*node).entry.value);
            }
        }

        let mut count = 0;
        for node in skip_map.iter_ptr() {
            unsafe {
                assert_eq!(format!("value{}", count), (*node).entry.value);
            }
            count += 1;
        }
        assert_eq!(count, skip_map.len());

        let map = create_random_map(20000);
        for (k, v) in &map {
            skip_map.insert(*k, v.to_string());
        }
        for (k, v) in map {
            unsafe {
                assert_eq!(
                    (*skip_map.find_first_ge(&k, None)).entry.value,
                    v.to_string()
                );
            }
        }
    }

    #[test]
    fn test_merge() {
        let map1: SrSwSkipMap<String, String> = SrSwSkipMap::new();
        let map2: SrSwSkipMap<String, String> = SrSwSkipMap::new();
        map1.insert("hello".to_string(), "world".to_string());
        map2.insert("hello".to_string(), "world3".to_string());
        map2.insert("a".to_string(), "b".to_string());
        map1.merge(map2);
        assert_eq!(2, map1.len());
        assert_eq!(
            "world3".to_string(),
            map1.get_clone(&"hello".to_string()).unwrap()
        );
        assert_eq!("b".to_string(), map1.get_clone(&"a".to_string()).unwrap());
    }

    #[test]
    fn test_remove() {
        let mut skip_map: SrSwSkipMap<i32, String> = SrSwSkipMap::new();
        for i in 0..100 {
            skip_map.insert(i, format!("value{}", i));
        }
        for i in 1..99 {
            assert!(skip_map.remove(i));
        }
        assert_eq!(2, skip_map.len());
        let value = [0, 99];
        for (node, v) in skip_map.iter_ptr().zip(value.iter()) {
            unsafe {
                assert_eq!((*node).entry.key, *v);
            }
        }
        skip_map.insert(0, "temp".into());
        assert!(skip_map.remove(0));
        assert_eq!(skip_map.len(), 1);

        assert!(skip_map.remove(99));
        assert!(skip_map.last_key_value().is_none());
        assert!(!skip_map.remove(0));
        assert_eq!(skip_map.len(), 0);
    }

    #[test]
    fn test_first_key_value() {
        let mut skip_map: SrSwSkipMap<i32, i32> = SrSwSkipMap::new();
        macro_rules! assert_first_key {
            ($k:literal) => {
                assert_eq!(skip_map.first_key_value().unwrap().key, $k);
            };
        }
        assert!(skip_map.first_key_value().is_none());
        skip_map.insert(10, 10);
        assert_first_key!(10);
        skip_map.insert(5, 5);
        assert_first_key!(5);
        skip_map.insert(3, 3);
        assert_first_key!(3);
        skip_map.insert(10, 10);
        assert_first_key!(3);
        skip_map.remove(3);
        assert_first_key!(5);
    }

    #[test]
    fn test_last_key_value() {
        let mut skip_map: SrSwSkipMap<i32, i32> = SrSwSkipMap::new();

        macro_rules! assert_last_key {
            ($k:literal) => {
                assert_eq!(skip_map.last_key_value().unwrap().key, $k);
            };
        }

        assert!(skip_map.last_key_value().is_none());
        skip_map.insert(10, 10);
        assert_last_key!(10);
        skip_map.insert(5, 5);
        assert_last_key!(10);
        skip_map.insert(13, 13);
        assert_last_key!(13);
        skip_map.insert(14, 14);
        assert_last_key!(14);
        skip_map.remove(14);
        assert_last_key!(13);
    }

    #[test]
    fn test_find_last_le() {
        let skip_map: SrSwSkipMap<i32, i32> = SrSwSkipMap::new();
        assert!(skip_map.find_last_le(&1).is_null());
        for i in 1..=100 {
            skip_map.insert(2 * i + 1, (2 * i + 1) * 2);
        }
        let mut r = rand::thread_rng();

        for _ in 0..50 {
            let key = r.gen_range(10..190);
            unsafe {
                if key % 2 == 1 {
                    assert_eq!((*skip_map.find_last_le(&key)).entry.value, key * 2);
                } else {
                    assert_eq!((*skip_map.find_last_le(&key)).entry.value, (key - 1) * 2);
                }
            }
        }
    }
}
