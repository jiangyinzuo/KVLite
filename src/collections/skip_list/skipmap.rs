use crate::collections::skip_list::{rand_level, MAX_LEVEL};
use crate::collections::Entry;
use std::alloc::Layout;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

#[repr(C)]
pub struct Node<K: Ord + Default, V: Default> {
    pub entry: Entry<K, V>,
    /// ranges [0, `MAX_LEVEL`]
    level: usize,
    /// the actual size is `level + 1`
    next: [*mut Self; 0],
}

impl<K: Ord + Default, V: Default> Node<K, V> {
    fn head() -> *mut Node<K, V> {
        Self::new_with_level(K::default(), V::default(), MAX_LEVEL)
    }

    fn new_with_level(key: K, value: V, level: usize) -> *mut Node<K, V> {
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
            std::ptr::write(&mut node.level, level);
            std::ptr::write_bytes(node.next.as_mut_ptr(), 0, level + 1);
            node_ptr
        }
    }

    fn get_layout(&self) -> Layout {
        let pointers_size = (self.level + 1) * std::mem::size_of::<*mut Self>();

        Layout::from_size_align(
            std::mem::size_of::<Self>() + pointers_size,
            std::mem::align_of::<Self>(),
        )
        .unwrap()
    }

    #[inline]
    pub fn get_next(&self, level: usize) -> *mut Self {
        unsafe { *self.next.get_unchecked(level) }
    }

    #[inline]
    fn set_next(&mut self, level: usize, node: *mut Self) {
        unsafe {
            *self.next.get_unchecked_mut(level) = node;
        }
    }
}

unsafe fn drop_node<K: Ord + Default, V: Default>(node: *mut Node<K, V>) {
    let layout = (*node).get_layout();
    std::ptr::drop_in_place(node as *mut Node<K, V>);
    std::alloc::dealloc(node as *mut u8, layout);
}

/// Map that allows duplicate keys, based on skip list
///
/// # NOTICE:
///
/// SkipMap is not thread-safe.
pub struct SkipMap<K: Ord + Default, V: Default> {
    dummy_head: *const Node<K, V>,
    tail: AtomicPtr<Node<K, V>>,
    cur_max_level: AtomicUsize,
    len: usize,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

unsafe impl<K: Ord + Default, V: Default> Send for SkipMap<K, V> {}
unsafe impl<K: Ord + Default, V: Default> Sync for SkipMap<K, V> {}

impl<K: Ord + Default, V: Default> SkipMap<K, V> {
    pub fn new() -> SkipMap<K, V> {
        SkipMap {
            dummy_head: Node::head(),
            tail: AtomicPtr::default(),
            cur_max_level: AtomicUsize::default(),
            len: 0,
            _key: PhantomData,
            _value: PhantomData,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
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

    /// # Safety
    /// node s
    /// hould be null or initialized
    pub unsafe fn node_cmp(node: *mut Node<K, V>, key: &K) -> std::cmp::Ordering {
        if node.is_null() {
            return std::cmp::Ordering::Greater;
        }
        (*node).entry.key.cmp(key)
    }

    /// # Example
    ///
    /// ```rust
    /// use kvlite::collections::skip_list::skipmap::SkipMap;
    /// let mut skip_map = SkipMap::new();
    /// for i in 1..10 {
    ///     skip_map.insert(i, i + 1);
    /// }
    /// let node = skip_map.find_first_ge(&3, None);
    /// unsafe {
    ///     let node = SkipMap::find_last_le_from_node(node, &9);
    ///     assert_eq!((*node).entry.value, 10);
    ///     let node2 = SkipMap::find_last_le_from_node(node, &-123);
    ///     assert_eq!(node, node2);
    /// }
    /// ```
    ///
    /// # Safety
    /// `node` should be a part of skip-map and should not be nullptr
    pub unsafe fn find_last_le_from_node(mut node: *mut Node<K, V>, key: &K) -> *mut Node<K, V> {
        debug_assert!(!node.is_null());
        if (*node).entry.key.eq(key) {
            return node;
        }
        let mut level = (*node).level;
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

    /// # Example
    ///
    /// ```rust
    /// use kvlite::collections::skip_list::skipmap::SkipMap;
    /// let mut skip_map = SkipMap::new();
    /// for i in 1..10 {
    ///     skip_map.insert(i, i + 1);
    /// }
    /// let node = skip_map.find_first_ge(&3, None);
    /// unsafe {
    ///     let node = SkipMap::find_first_ge_from_node(node, &7);
    ///     assert_eq!((*node).entry.value, 8);
    /// }
    /// ```
    /// # Safety
    /// `node` should be a part of skip-map and should not be nullptr
    pub unsafe fn find_first_ge_from_node(mut node: *mut Node<K, V>, key: &K) -> *mut Node<K, V> {
        debug_assert!(!node.is_null());
        if (*node).entry.key.eq(key) {
            return node;
        }
        let mut level = (*node).level;
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

    /// Return the first node `N` whose key is greater or equal than given `key`.
    /// if `prev_nodes` is `Some(...)`, it will be assigned to all the previous nodes of `N`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kvlite::collections::skip_list::skipmap::SkipMap;
    /// let mut skip_map = SkipMap::new();
    /// assert!(skip_map.find_first_ge(&1, None).is_null());
    /// skip_map.insert(3, 3);
    /// assert!(skip_map.find_first_ge(&5, None).is_null());
    /// ```
    pub fn find_first_ge(
        &self,
        key: &K,
        mut prev_nodes: Option<&mut [*mut Node<K, V>; MAX_LEVEL + 1]>,
    ) -> *mut Node<K, V> {
        let mut level = self.cur_max_level.load(Ordering::Acquire);
        let mut node = self.dummy_head as *mut Node<K, V>;
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

    /// Return the last node whose key is less than or equal to `key`,
    /// if does not exist, return nullptr.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kvlite::collections::skip_list::skipmap::SkipMap;
    /// let mut skip_map = SkipMap::new();
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
    pub fn find_last_le(&self, key: &K) -> *mut Node<K, V> {
        let mut level = self.cur_max_level.load(Ordering::Acquire);
        let mut node = self.dummy_head as *mut Node<K, V>;

        let result = loop {
            let next = unsafe { (*node).get_next(level) };
            match unsafe { Self::node_cmp(next, key) } {
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

    pub fn get_clone(&self, key: &K) -> Option<V>
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

    pub fn range_get(&self, key_start: &K, key_end: &K, kvs: &mut Self)
    where
        K: Clone,
        V: Clone,
    {
        let mut node = self.find_first_ge(key_start, None);
        unsafe {
            while !node.is_null() && (*node).entry.key.le(key_end) {
                kvs.insert((*node).entry.key.clone(), (*node).entry.value.clone());
                node = (*node).get_next(0);
            }
        }
    }

    pub fn merge(&mut self, other: Self) {
        // todo: optimize the time complexity
        for n in other.into_ptr_iter() {
            unsafe {
                let kv: Entry<K, V> = std::mem::take(&mut (*n).entry);
                self.insert(kv.key, kv.value);
            }
        }
    }

    /// return whether `key` has already exist.
    pub fn insert(&mut self, key: K, value: V) -> bool {
        let mut prev_nodes = [self.dummy_head as *mut _; MAX_LEVEL + 1];
        let node = self.find_first_ge(&key, Some(&mut prev_nodes));
        let has_key = unsafe { Self::node_eq_key(node, &key) };
        if has_key {
            unsafe {
                (*node).entry.value = value;
            }
        } else {
            self.insert_after(prev_nodes, key, value);
        }
        has_key
    }

    /// Insert node with `key`, `value` after `prev_nodes`
    fn insert_after(&mut self, prev_nodes: [*mut Node<K, V>; MAX_LEVEL + 1], key: K, value: V) {
        #[cfg(debug_assertions)]
        {
            for (level, prev) in prev_nodes.iter().enumerate() {
                unsafe {
                    debug_assert!(
                        (*prev) == self.dummy_head as *mut _ || (**prev).entry.key.le(&key)
                    );
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
            if (*(*prev_nodes.get_unchecked(0))).get_next(0).is_null() {
                self.tail.store(new_node, Ordering::Release);
            }

            for i in 0..=level {
                // set next of new_node first to ensure concurrent read is correct.
                (*new_node).set_next(i, (*(prev_nodes[i])).get_next(i));
                (*(prev_nodes[i])).set_next(i, new_node);
            }
        }

        self.len += 1;
    }

    /// Remove all the `key` in map, return whether `key` exists
    pub fn remove(&mut self, key: K) -> bool {
        let mut prev_nodes = [self.dummy_head as *mut _; MAX_LEVEL + 1];
        let mut node = self.find_first_ge(&key, Some(&mut prev_nodes));
        let has_key = unsafe { Self::node_eq_key(node, &key) };
        if has_key {
            unsafe {
                while !node.is_null() && Self::node_eq_key(node, &key) {
                    let next_node = (*node).get_next(0);
                    for i in 0..=(*node).level {
                        (*prev_nodes[i]).set_next(i, (*node).get_next(i))
                    }
                    self.len -= 1;
                    if next_node.is_null() {
                        self.tail
                            .store(*prev_nodes.get_unchecked(0) as *mut _, Ordering::SeqCst);
                    }
                    drop_node(node);
                    node = next_node;
                }
            }
            true
        } else {
            false
        }
    }

    pub fn iter_ptr<'a>(&self) -> IterPtr<'a, K, V> {
        unsafe {
            IterPtr {
                node: (*self.dummy_head).get_next(0),
                _marker: PhantomData,
            }
        }
    }

    pub fn iter<'a>(&self) -> Iter<'a, K, V> {
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
    /// use kvlite::collections::skip_list::skipmap::SkipMap;
    /// let mut skip_map = SkipMap::new();
    /// assert!(skip_map.first_key_value().is_none());
    ///
    /// skip_map.insert("hello", 2);
    /// skip_map.insert("apple", 1);
    /// let entry = skip_map.first_key_value().unwrap();
    /// assert_eq!(entry.key, "apple");
    /// assert_eq!(entry.value, 1);
    /// ```
    pub fn first_key_value(&self) -> Option<&Entry<K, V>> {
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
    /// use kvlite::collections::skip_list::skipmap::SkipMap;
    /// let mut skip_map = SkipMap::new();
    /// assert!(skip_map.last_key_value().is_none());
    ///
    /// skip_map.insert("hello", 2);
    /// skip_map.insert("apple", 1);
    /// let entry = skip_map.last_key_value().unwrap();
    /// assert_eq!(entry.key, "hello");
    /// assert_eq!(entry.value, 2);
    /// ```
    pub fn last_key_value(&self) -> Option<&Entry<K, V>> {
        if self.is_empty() {
            None
        } else {
            Some(unsafe { &(*self.tail.load(Ordering::Acquire)).entry })
        }
    }

    pub fn into_ptr_iter(self) -> IntoPtrIter<K, V> {
        unsafe {
            let node = (*self.dummy_head).get_next(0);
            IntoPtrIter { inner: self, node }
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
        let mut node = self.dummy_head;

        unsafe {
            while !node.is_null() {
                let next_node = (*node).get_next(0);
                drop_node(node as *mut Node<K, V>);
                node = next_node;
            }
        }
    }
}

pub struct Iter<'a, K: Ord + Default, V: Default> {
    node: *const Node<K, V>,
    _marker: PhantomData<&'a Node<K, V>>,
}

impl<'a, K: Ord + Default, V: Default> Iterator for Iter<'a, K, V> {
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
pub struct IterPtr<'a, K: Ord + Default, V: Default> {
    node: *const Node<K, V>,
    _marker: PhantomData<&'a Node<K, V>>,
}

impl<'a, K: Ord + Default, V: Default> IterPtr<'a, K, V> {
    pub fn current_no_consume(&self) -> *const Node<K, V> {
        self.node
    }

    /// # Notice
    ///
    /// Make sure `self.node` is not null.
    pub fn next_node(&mut self) -> *const Node<K, V> {
        debug_assert!(!self.node.is_null());
        unsafe {
            self.node = (*self.node).get_next(0);
        }
        self.node
    }
}

impl<'a, K: Ord + Default, V: Default> Iterator for IterPtr<'a, K, V> {
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

pub struct IntoPtrIter<K: Ord + Default, V: Default> {
    inner: SkipMap<K, V>,
    node: *mut Node<K, V>,
}

impl<K: Ord + Default, V: Default> IntoPtrIter<K, V> {
    pub fn current_mut_no_consume(&self) -> *mut Node<K, V> {
        self.node as *mut _
    }

    /// # Notice
    ///
    /// Make sure `self.node` is not null.
    pub fn next_node(&mut self) -> *mut Node<K, V> {
        debug_assert!(!self.node.is_null());
        unsafe {
            self.node = (*self.node).get_next(0);
        }
        self.node
    }
}

impl<K: Ord + Default, V: Default> Iterator for IntoPtrIter<K, V> {
    type Item = *mut Node<K, V>;

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

pub struct IntoIter<K: Ord + Default, V: Default> {
    inner: SkipMap<K, V>,
    node: *mut Node<K, V>,
}

impl<K: Ord + Default, V: Default> Iterator for IntoIter<K, V> {
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

impl<K: Ord + Default, V: Default> IntoIterator for SkipMap<K, V> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        unsafe {
            let node = (*self.dummy_head).get_next(0);
            IntoIter { inner: self, node }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::collections::skip_list::skipmap::SkipMap;
    use crate::db::no_transaction_db::tests::create_random_map;
    use rand::Rng;

    #[test]
    fn test_key() {
        let mut skip_map = SkipMap::new();
        skip_map.insert(1, 1);
        skip_map.insert(1, 2);
        assert_eq!(skip_map.len(), 1);

        assert!(skip_map.remove(1));
        assert!(!skip_map.remove(1));
        assert!(skip_map.is_empty());
    }

    #[test]
    fn test_insert() {
        let mut skip_map: SkipMap<i32, String> = SkipMap::new();
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
        let mut map1 = SkipMap::new();
        let mut map2 = SkipMap::new();
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
        let mut skip_map: SkipMap<i32, String> = SkipMap::new();
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
        let mut skip_map = SkipMap::new();
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
        let mut skip_map = SkipMap::new();

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
        let mut skip_map = SkipMap::new();
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
