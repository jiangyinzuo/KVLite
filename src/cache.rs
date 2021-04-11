use std::alloc::Layout;
use std::hash::Hash;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

const CACHE_CAP: usize = 256;

const NUM_SHARD_BITS: usize = 4;
const NUM_SHARD: usize = 1 << NUM_SHARD_BITS;

pub struct ShardLRUCache<K: Eq, V> {
    caches: [Mutex<LRUCache<K, V>>; NUM_SHARD],
}

impl<K: Eq, V> Default for ShardLRUCache<K, V> {
    fn default() -> Self {
        ShardLRUCache {
            caches: [
                Mutex::new(LRUCache::new()),
                Mutex::new(LRUCache::new()),
                Mutex::new(LRUCache::new()),
                Mutex::new(LRUCache::new()),
                Mutex::new(LRUCache::new()),
                Mutex::new(LRUCache::new()),
                Mutex::new(LRUCache::new()),
                Mutex::new(LRUCache::new()),
                Mutex::new(LRUCache::new()),
                Mutex::new(LRUCache::new()),
                Mutex::new(LRUCache::new()),
                Mutex::new(LRUCache::new()),
                Mutex::new(LRUCache::new()),
                Mutex::new(LRUCache::new()),
                Mutex::new(LRUCache::new()),
                Mutex::new(LRUCache::new()),
            ],
        }
    }
}

impl<K: Eq + Hash, V> ShardLRUCache<K, V> {
    pub fn insert(&self, key: K, value: V, hash: u32) {
        let mut guard: MutexGuard<LRUCache<K, V>> = self.caches[shard(hash)].lock().unwrap();
        guard.insert(key, value, hash);
    }

    pub fn look_up(&self, key: &K, hash: u32) -> EntryTracker<K, V> {
        let mut guard: MutexGuard<LRUCache<K, V>> = self.caches[shard(hash)].lock().unwrap();
        guard.look_up(key, hash)
    }

    pub fn erase(&self, key: &K, hash: u32) {
        let mut guard: MutexGuard<LRUCache<K, V>> = self.caches[shard(hash)].lock().unwrap();
        guard.erase(key, hash);
    }
}

unsafe impl<K: Eq, V> Send for ShardLRUCache<K, V> {}
unsafe impl<K: Eq, V> Sync for ShardLRUCache<K, V> {}

#[inline]
fn shard(hash: u32) -> usize {
    (hash >> (32 - NUM_SHARD_BITS)) as usize
}

struct LRUCache<K: Eq, V> {
    table: HashTable<K, V>,
    head: *mut LRUEntry<K, V>,
    // dummy tail, tail.prev is the oldest entry
    tail: *mut LRUEntry<K, V>,
}

unsafe impl<K: Eq, V> Send for LRUCache<K, V> {}
unsafe impl<K: Eq, V> Sync for LRUCache<K, V> {}

impl<K: Eq, V> LRUCache<K, V> {
    fn new() -> LRUCache<K, V> {
        let head = LRUEntry::new_empty();
        unsafe {
            (*head).next = head;
            (*head).prev = head;
        }

        LRUCache {
            table: HashTable::default(),
            head,
            tail: head,
        }
    }

    fn attach(&mut self, n: *mut LRUEntry<K, V>) {
        unsafe {
            (*n).next = (*self.head).next;
            (*n).prev = self.head;
            (*self.head).next = n;
            (*(*n).next).prev = n;
        }
    }

    fn look_up(&mut self, key: &K, hash: u32) -> EntryTracker<K, V> {
        let n = self.table.look_up(key, hash);
        if !n.is_null() {
            if self.head != n {
                detach(n);
                self.attach_to_head(n);
            }

            unsafe {
                (*n).ref_count.fetch_add(1, Ordering::Release);
            }
        }
        EntryTracker(n)
    }

    fn insert(&mut self, key: K, value: V, hash: u32) {
        if self.table.len >= CACHE_CAP {
            unsafe {
                let old = (*self.tail).prev;
                debug_assert_ne!(self.tail, old);
                detach(old);
                self.table.remove(old);
            }
        }
        let new_entry = LRUEntry::new(key, value, hash);
        self.attach_to_head(new_entry);
        self.table.insert(new_entry);
    }

    fn erase(&mut self, key: &K, hash: u32) {
        let n = self.table.look_up(key, hash);
        if !n.is_null() {
            // FIXME
            // detach(n);
            // unsafe {
            //     self.table.remove(n);
            // }
        }
    }

    fn attach_to_head(&mut self, n: *mut LRUEntry<K, V>) {
        debug_assert_ne!(n, self.head);
        unsafe {
            (*n).next = self.head;
            (*self.head).prev = n;
            (*n).prev = self.tail;
            (*self.tail).next = n;
            self.head = n;
        }
    }
}

impl<K: Eq, V> Drop for LRUCache<K, V> {
    fn drop(&mut self) {
        let mut node = self.head;
        for _ in 0..=self.table.len {
            debug_assert!(!node.is_null());
            unsafe {
                release((*node).prev);
                node = (*node).next;
            }
        }
    }
}

pub struct EntryTracker<K: Eq, V>(pub *const LRUEntry<K, V>);

impl<K: Eq, V> Drop for EntryTracker<K, V> {
    fn drop(&mut self) {
        if !self.0.is_null() {
            release(self.0 as *mut LRUEntry<K, V>);
        }
    }
}

pub struct LRUEntry<K: Eq, V> {
    key: MaybeUninit<K>,
    value: MaybeUninit<V>,
    hash: u32,
    next_hash: *mut LRUEntry<K, V>,
    prev: *mut LRUEntry<K, V>,
    next: *mut LRUEntry<K, V>,
    ref_count: AtomicUsize,
}

impl<K: Eq, V> LRUEntry<K, V> {
    fn new(key: K, value: V, hash: u32) -> *mut Self {
        let layout = Layout::new::<LRUEntry<K, V>>();
        unsafe {
            let node_ptr = std::alloc::alloc(layout) as *mut Self;
            let node = &mut *node_ptr;
            std::ptr::write(
                node,
                LRUEntry {
                    key: MaybeUninit::new(key),
                    value: MaybeUninit::new(value),
                    hash,
                    next_hash: ptr::null_mut(),
                    prev: ptr::null_mut(),
                    next: ptr::null_mut(),
                    ref_count: AtomicUsize::new(1),
                },
            );
            node
        }
    }

    fn new_empty() -> *mut Self {
        unsafe {
            let layout = Layout::new::<LRUEntry<K, V>>();

            let node_ptr = std::alloc::alloc(layout) as *mut Self;
            let node = &mut *node_ptr;
            std::ptr::write(
                node,
                LRUEntry {
                    key: MaybeUninit::uninit(),
                    value: MaybeUninit::uninit(),
                    hash: 0,
                    next_hash: ptr::null_mut(),
                    prev: ptr::null_mut(),
                    next: ptr::null_mut(),
                    ref_count: AtomicUsize::new(1),
                },
            );
            node
        }
    }

    #[inline]
    pub fn value(&self) -> &V {
        unsafe { self.value.assume_init_ref() }
    }
}

unsafe impl<K: Eq, V> Send for LRUEntry<K, V> {}

const TABLE_SIZE: usize = 256;

struct HashTable<K: Eq, V> {
    table: [*mut LRUEntry<K, V>; TABLE_SIZE],
    len: usize,
}

impl<K: Eq, V> Default for HashTable<K, V> {
    fn default() -> Self {
        unsafe {
            HashTable {
                table: std::mem::zeroed(),
                len: 0,
            }
        }
    }
}

impl<K: Eq, V> HashTable<K, V> {
    fn look_up(&self, key: &K, hash: u32) -> *mut LRUEntry<K, V> {
        let idx = hash as usize & (TABLE_SIZE - 1);
        unsafe {
            let p = self.table.get_unchecked(idx);
            let mut node = *p;
            let result = Self::find_ptr(&mut node, hash, key);
            if !(*result).is_null() {
                *result
            } else {
                std::ptr::null_mut()
            }
        }
    }

    fn insert(&mut self, entry: *mut LRUEntry<K, V>) {
        unsafe {
            let idx = (*entry).hash as usize & (TABLE_SIZE - 1);
            let p = self.table.get_unchecked_mut(idx);
            if (*p).is_null() {
                *p = entry;
            } else {
                (*entry).next_hash = *p;
                *p = entry;
            }
        }
        self.len += 1;
    }

    /// Remove `entry` from hashtable and decrease `entry.ref_count` by 1.
    /// # Safety:
    ///
    /// `entry` should not be null
    unsafe fn remove(&mut self, entry: *mut LRUEntry<K, V>) {
        debug_assert!(!entry.is_null());
        let hash = (*entry).hash;
        let idx = hash as usize & (TABLE_SIZE - 1);
        let p = self.table.get_unchecked(idx);
        debug_assert!(!(*p).is_null());
        let mut node = *p;
        let result = Self::find_ptr_by_ptr(&mut node, entry);
        let old = *result;

        debug_assert!(!old.is_null());
        self.len -= 1;
        (*result) = (*old).next_hash;
        release(entry);
    }

    fn find_ptr(
        mut node: *mut *mut LRUEntry<K, V>,
        hash: u32,
        key: &K,
    ) -> *mut *mut LRUEntry<K, V> {
        unsafe {
            while !((*node).is_null()
                || (**node).hash == hash && ((**node).key.assume_init_ref()).eq(key))
            {
                node = &mut (**node).next_hash;
            }
        }
        node
    }

    fn find_ptr_by_ptr(
        mut node: *mut *mut LRUEntry<K, V>,
        entry: *mut LRUEntry<K, V>,
    ) -> *mut *mut LRUEntry<K, V> {
        unsafe {
            while !((*node).is_null() || (*node) == entry) {
                node = &mut (**node).next_hash;
            }
        }
        node
    }
}

fn detach<K: Eq, V>(n: *mut LRUEntry<K, V>) {
    debug_assert!(!n.is_null());
    unsafe {
        if !(*n).next.is_null() {
            (*(*n).next).prev = (*n).prev;
        }
        if !(*n).prev.is_null() {
            (*(*n).prev).next = (*n).next;
        }
    }
}

fn release<K: Eq, V>(n: *mut LRUEntry<K, V>) {
    unsafe {
        let count = (*n).ref_count.fetch_sub(1, Ordering::Release);
        if count == 1 {
            let layout = Layout::new::<LRUEntry<K, V>>();
            std::ptr::drop_in_place((*n).key.as_mut_ptr());
            std::ptr::drop_in_place((*n).value.as_mut_ptr());
            std::alloc::dealloc(n as *mut u8, layout);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::{HashTable, LRUCache, LRUEntry, ShardLRUCache, CACHE_CAP, TABLE_SIZE};
    use crate::hash::murmur_hash;
    use std::sync::{Arc, Barrier};

    fn make_entry(i: usize) -> *mut LRUEntry<String, String> {
        let h = murmur_hash(&i.to_le_bytes(), 0x12345678);
        LRUEntry::new(i.to_string(), i.to_string(), h)
    }

    #[test]
    fn test_hashtable() {
        let mut table = HashTable::<String, String>::default();
        let s = String::from("hello");
        let p = table.look_up(&s, 321);
        assert!(p.is_null());

        let entry = LRUEntry::new(String::from("key1"), String::from("value1"), 1234);
        table.insert(entry);
        let p = table.look_up(&s, 1234);
        assert!(p.is_null());
        let p = table.look_up(unsafe { (*entry).key.assume_init_ref() }, 1234);
        unsafe {
            assert_eq!((*p).hash, 1234);
        }
        unsafe { table.remove(entry) };
        assert_eq!(p, entry);

        assert_eq!(table.len, 0);

        for i in 0..TABLE_SIZE * 5 {
            let entry = make_entry(i);
            table.insert(entry);
        }

        assert_eq!(table.len, TABLE_SIZE * 5);

        for i in 0..TABLE_SIZE * 5 {
            let h = murmur_hash(&i.to_le_bytes(), 0x12345678);
            let entry = table.look_up(&i.to_string(), h);
            unsafe {
                assert_eq!((*entry).hash, h);
                assert_eq!(*(*entry).key.as_mut_ptr(), i.to_string());
                table.remove(entry);
            }
        }
        assert_eq!(table.len, 0);
    }

    #[test]
    fn test_lru_cache() {
        let mut lru_cache = LRUCache::new();
        lru_cache.erase(&"h".to_string(), 123);
        for i in 0..CACHE_CAP {
            let key = i.to_string();
            let value = i.to_string();
            let h = murmur_hash(key.as_bytes(), 0x87654321);
            lru_cache.insert(key, value, h);
        }
        assert_eq!(lru_cache.table.len, CACHE_CAP);

        for i in 0..CACHE_CAP {
            let key = i.to_string();
            let h = murmur_hash(key.as_bytes(), 0x87654321);
            let tracker = lru_cache.look_up(&key, h);
            let tracker2 = lru_cache.look_up(&key, h);
            unsafe {
                assert_eq!((*tracker.0).value.assume_init_ref(), &key);
                assert_eq!((*tracker2.0).value.assume_init_ref(), &key);
            }
        }

        for i in CACHE_CAP..CACHE_CAP + 20 {
            let key = i.to_string();
            let value = i.to_string();
            let h = murmur_hash(key.as_bytes(), 0x87654321);
            lru_cache.insert(key, value, h);
        }
        assert_eq!(lru_cache.table.len, CACHE_CAP);

        for i in 0..500 {
            let h = murmur_hash(i.to_string().as_bytes(), 0x87654321);
            let tracker = lru_cache.look_up(&"hello".to_string(), h);
            assert!(tracker.0.is_null());
        }
    }

    #[test]
    fn test_shard_lru_cache() {
        let lru_cache = Arc::new(ShardLRUCache::default());
        for i in 0..CACHE_CAP {
            let key = i.to_string();
            let value = i.to_string();
            let h = murmur_hash(key.as_bytes(), 0x87654321);
            lru_cache.insert(key, value, h);
        }

        let key = 3.to_string();
        lru_cache.look_up(&key, murmur_hash(key.as_bytes(), 0x87654321));

        fn look_up(lru_cache: Arc<ShardLRUCache<String, String>>) {
            for i in 0..100 {
                let key = i.to_string();
                let h = murmur_hash(key.as_bytes(), 0x87654321);
                let tracker = lru_cache.look_up(&key, h);
                unsafe {
                    assert_eq!((*(tracker.0)).value.assume_init_ref(), &i.to_string());
                }
            }
        }

        let barrier = Arc::new(Barrier::new(1));
        let barrier2 = barrier.clone();

        let lru_cache2 = lru_cache.clone();
        let lru_cache3 = lru_cache.clone();

        let handle1 = std::thread::spawn(move || {
            barrier.wait();
            look_up(lru_cache2);
        });

        let handle2 = std::thread::spawn(move || {
            barrier2.wait();
            look_up(lru_cache3);
        });

        handle1.join().unwrap();
        handle2.join().unwrap();

        for i in 0..500 {
            let h = murmur_hash(i.to_string().as_bytes(), 0x87654321);
            let tracker = lru_cache.look_up(&"hello".to_string(), h);
            assert!(tracker.0.is_null());
        }
    }
}
