use crate::cache::ShardLRUCache;
use crate::collections::skip_list::skipmap::SkipMap;
use crate::compact::level_n::start_compact;
use crate::db::{Key, Value, MAX_LEVEL};
use crate::sstable::table_cache::IndexCache;
use crate::sstable::table_handle::{TableReadHandle, TableWriteHandle};
use crate::Result;
use crossbeam_channel::{Receiver, Sender};
use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;

/// Struct for adding and removing sstable files.
pub struct LevelNManager {
    db_path: String,
    level_tables: [std::sync::RwLock<BTreeMap<(Key, u64), Arc<TableReadHandle>>>; MAX_LEVEL],
    level_sizes: [AtomicU64; MAX_LEVEL],
    next_table_id: [AtomicU64; MAX_LEVEL],

    pub(crate) index_cache: Arc<ShardLRUCache<u64, IndexCache>>,
    senders: Vec<Sender<bool>>,
    handles: RwLock<Vec<JoinHandle<()>>>,
    next_to_compact: AtomicUsize,
}

unsafe impl Sync for LevelNManager {}
unsafe impl Send for LevelNManager {}

impl LevelNManager {
    /// Open all the sstables at `db_path` when initializing DB.
    pub fn open_tables(
        db_path: String,
        index_cache: Arc<ShardLRUCache<u64, IndexCache>>,
    ) -> Arc<LevelNManager> {
        for i in 1..=MAX_LEVEL {
            std::fs::create_dir_all(format!("{}/{}", db_path, i)).unwrap();
        }

        let mut manager = LevelNManager {
            db_path,
            level_tables: [
                std::sync::RwLock::default(),
                std::sync::RwLock::default(),
                std::sync::RwLock::default(),
                std::sync::RwLock::default(),
                std::sync::RwLock::default(),
                std::sync::RwLock::default(),
                std::sync::RwLock::default(),
            ],
            level_sizes: [
                AtomicU64::default(),
                AtomicU64::default(),
                AtomicU64::default(),
                AtomicU64::default(),
                AtomicU64::default(),
                AtomicU64::default(),
                AtomicU64::default(),
            ],
            next_table_id: [
                AtomicU64::default(),
                AtomicU64::default(),
                AtomicU64::default(),
                AtomicU64::default(),
                AtomicU64::default(),
                AtomicU64::default(),
                AtomicU64::default(),
            ],
            senders: Vec::with_capacity(MAX_LEVEL - 1),
            handles: RwLock::new(Vec::with_capacity(MAX_LEVEL - 1)),
            next_to_compact: AtomicUsize::default(),
            index_cache,
        };

        let mut receivers = VecDeque::with_capacity(MAX_LEVEL - 1);

        for i in 1..=MAX_LEVEL {
            let dir = std::fs::read_dir(format!("{}/{}", &manager.db_path, i)).unwrap();
            let mut file_size = 0;
            let mut next_table_id = 0;
            for d in dir {
                let d = d.unwrap();
                let path = d.path();
                // The file whose file_name is a number is considered as sstable.
                if let Ok(table_id) = path.file_name().unwrap().to_str().unwrap().parse::<u64>() {
                    next_table_id = next_table_id.max(table_id);
                    let handle = TableReadHandle::open_table(&manager.db_path, i as _, table_id);

                    // Safety: i is in range [1, MAX_LEVEL]
                    unsafe {
                        let mut guard = manager
                            .level_tables
                            .get_unchecked_mut(i - 1)
                            .write()
                            .unwrap();
                        guard.insert(
                            (handle.max_key().clone(), handle.table_id()),
                            Arc::new(handle),
                        );
                    }

                    file_size += d.metadata().unwrap().len();
                } else {
                    info!("clear temporary file {:?}", path);
                    std::fs::remove_file(path).unwrap();
                }
            }
            // Safety: i is in range [1, MAX_LEVEL]
            unsafe {
                manager
                    .level_sizes
                    .get_unchecked(i - 1)
                    .store(file_size, Ordering::Release);
                manager
                    .next_table_id
                    .get_unchecked(i - 1)
                    .store(next_table_id as u64 + 1, Ordering::Release);
            }

            if i < MAX_LEVEL {
                let (sender, receiver) = crossbeam_channel::unbounded();
                manager.senders.push(sender);
                receivers.push_back(receiver);
            }
        }

        let manager = Arc::new(manager);
        for i in 1..=MAX_LEVEL - 1 {
            let mut guard = manager.handles.write().unwrap();
            guard.push(Self::start_compacting_task(
                manager.clone(),
                unsafe { NonZeroUsize::new_unchecked(i) },
                receivers.pop_front().unwrap(),
            ));
        }
        manager
    }

    fn start_compacting_task(
        leveln_manager: Arc<LevelNManager>,
        compact_level: NonZeroUsize,
        receiver: Receiver<bool>,
    ) -> JoinHandle<()> {
        std::thread::spawn(move || {
            info!("start compacting task for level {}.", compact_level);
            while let Ok(true) = receiver.recv() {
                if leveln_manager.size_over(compact_level) {
                    if let Some(handle_to_compact) =
                        leveln_manager.get_handle_to_compact(compact_level)
                    {
                        debug!("compact level: {}", compact_level);
                        start_compact(compact_level, handle_to_compact, leveln_manager.clone());
                    }
                }
            }
            info!("compacting task for level {} exit.", compact_level);
        })
    }

    pub fn get_level_tables_lock(
        &self,
        level: NonZeroUsize,
    ) -> &std::sync::RwLock<BTreeMap<(Key, u64), Arc<TableReadHandle>>> {
        let lock = self.level_tables.get(level.get() - 1).unwrap();
        lock
    }

    pub fn range_query(&self, key_start: &Key, key_end: &Key, kvs: &mut SkipMap<Key, Value>) {
        for level in (1..=MAX_LEVEL).rev() {
            let tables_lock =
                self.get_level_tables_lock(unsafe { NonZeroUsize::new_unchecked(level) });
            let tables_guard = tables_lock.read().unwrap();
            for (_k, table_read_handle) in tables_guard.range((key_start.clone(), 0)..) {
                if !table_read_handle.range_query(key_start, key_end, kvs) {
                    break
                }
            }
        }
    }

    pub fn query(&self, key: &Key) -> Result<Option<Value>> {
        for level in 1..=MAX_LEVEL {
            let tables_lock =
                self.get_level_tables_lock(unsafe { NonZeroUsize::new_unchecked(level) });
            let tables_guard = tables_lock.read().unwrap();

            if let Some((k, table_read_handle)) = tables_guard.range((key.clone(), 0)..).next() {
                debug_assert!(key.le(&k.0));
                debug_assert!(table_read_handle.readable());
                let entry_tracker = self
                    .index_cache
                    .look_up(&table_read_handle.table_key(), table_read_handle.hash());

                let option = if entry_tracker.0.is_null() {
                    table_read_handle.query_sstable(key, &self.index_cache)
                } else {
                    let index_cache = unsafe { (*entry_tracker.0).value() };
                    table_read_handle.query_sstable_with_cache(key, &index_cache)
                };
                if option.is_some() {
                    return Ok(option);
                }
            }
        }
        Ok(None)
    }

    fn get_next_table_id(&self, level: NonZeroUsize) -> u64 {
        unsafe {
            self.next_table_id
                .get_unchecked(level.get() - 1)
                .fetch_add(1, Ordering::SeqCst)
        }
    }

    pub fn upsert_table_handle(&self, handle: TableWriteHandle) {
        let file_size = handle.writer.writer.pos;
        debug_assert!(file_size > 0);

        let level = NonZeroUsize::new(handle.level()).unwrap();

        let handle = TableReadHandle::from_table_write_handle(handle);

        let lock = self.get_level_tables_lock(level);
        let mut table_guard = lock.write().unwrap();

        let option = table_guard.insert(
            (handle.max_key().clone(), handle.table_id()),
            Arc::new(handle),
        );

        debug_assert!(option.is_none());

        unsafe {
            self.level_sizes
                .get_unchecked(level.get() - 1)
                .fetch_add(file_size, Ordering::SeqCst);
        }
    }

    pub fn ready_to_delete(&self, table_handle: Arc<TableReadHandle>) {
        let level = table_handle.level();
        debug_assert!(level > 0);
        unsafe {
            self.level_sizes
                .get_unchecked(level - 1)
                .fetch_sub(table_handle.file_size(), Ordering::Release);
        }

        let mut guard = self
            .get_level_tables_lock(unsafe { NonZeroUsize::new_unchecked(level) })
            .write()
            .unwrap();
        let t = guard
            .remove(&(table_handle.max_key().clone(), table_handle.table_id()))
            .unwrap();

        t.ready_to_delete();
        self.index_cache
            .erase(&table_handle.table_key(), table_handle.hash());
    }

    /// Create a new sstable without `min_key` or `max_key`
    pub fn create_table_write_handle(
        &self,
        level: NonZeroUsize,
        kv_total: u32,
    ) -> TableWriteHandle {
        let next_table_id = self.get_next_table_id(level);
        TableWriteHandle::new(&self.db_path, level.get(), next_table_id, kv_total)
    }

    /// Get sstable file count of `level`, used for judging whether need compacting.
    pub fn file_count(&self, level: usize) -> usize {
        debug_assert!((1..=MAX_LEVEL).contains(&level));
        let tables = self.level_tables.get(level).unwrap();
        let guard = tables.read().unwrap();
        guard.len()
    }

    /// Get tables in `level` that intersect with [`min_key`, `max_key`].
    pub fn get_overlap_tables(
        &self,
        level: NonZeroUsize,
        min_key: &Key,
        max_key: &Key,
    ) -> VecDeque<Arc<TableReadHandle>> {
        let tables_lock = self.get_level_tables_lock(level);
        let tables_guard = tables_lock.read().unwrap();

        let mut tables = VecDeque::new();

        // min_key:       "3"
        //                 |-------------->
        // max_key:  "1", "3", "5", "7" ...
        for (_key, handle) in tables_guard.range((min_key.clone(), 0)..) {
            if handle.is_overlapping(min_key, max_key) {
                if handle.test_and_set_compacting() {
                    let handle = handle.clone();
                    tables.push_back(handle);
                }
            } else {
                break;
            }
        }
        tables
    }

    /// Get total size of sstables in `level`
    pub(crate) fn level_size(&self, level: usize) -> u64 {
        debug_assert!((1..=MAX_LEVEL).contains(&level));
        unsafe {
            self.level_sizes
                .get_unchecked(level - 1)
                .load(Ordering::Acquire)
        }
    }

    /// If total size of `level` is larger than 10^i MB, it should be compacted.
    pub fn size_over(&self, level: NonZeroUsize) -> bool {
        let size = self.level_size(level.get());
        #[cfg(debug_assertions)]
        {
            size > 10u64.pow(level.get() as u32) * 1024
        }
        #[cfg(not(debug_assertions))]
        {
            size > 10u64.pow(level.get() as u32) * 1024 * 1024
        }
    }

    pub(crate) fn get_handle_to_compact(
        &self,
        level: NonZeroUsize,
    ) -> Option<Arc<TableReadHandle>> {
        let lock = self.get_level_tables_lock(level);
        let guard = lock.read().unwrap();

        // find a handle to compact
        for _ in 0..10 {
            let id = self.next_to_compact.load(Ordering::Acquire) % guard.len();
            let v = guard.values().nth(id).unwrap();
            self.next_to_compact
                .store((id + 1) % guard.len(), Ordering::SeqCst);
            if v.test_and_set_compacting() {
                return Some(v.clone());
            }
        }
        None
    }

    /// May compact `level`th sstables.
    pub fn may_compact(&self, level: NonZeroUsize) {
        if level.get() < MAX_LEVEL && self.size_over(level) {
            if let Err(e) = self.senders.get(level.get() - 1).unwrap().send(true) {
                warn!("{:#?}", e);
            }
        }
    }

    pub(crate) fn close(&self) {
        for sender in self.senders.iter() {
            sender.send(false).unwrap();
        }
        let mut guard = self.handles.write().unwrap();
        while !guard.is_empty() {
            let handle = guard.pop().unwrap();
            handle.join().unwrap();
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::cache::ShardLRUCache;
    use crate::db::MAX_LEVEL;
    use crate::sstable::manager::level_n::LevelNManager;
    use crate::sstable::table_handle::tests::create_read_handle;
    use std::sync::Arc;

    pub(crate) fn create_manager(db_path: &str) -> Arc<LevelNManager> {
        let index_cache = Arc::new(ShardLRUCache::default());
        LevelNManager::open_tables(db_path.to_string(), index_cache)
    }

    #[test]
    fn test_manager() {
        let _ = env_logger::try_init();
        let path = tempfile::TempDir::new().unwrap();
        for i in 0..=MAX_LEVEL {
            std::fs::create_dir_all(path.path().join(i.to_string())).unwrap();
        }

        let db_path = path.path().to_str().unwrap();
        let read_handle = create_read_handle(db_path, 1, 1, 0..100);

        assert_eq!(read_handle.kv_total(), 100);
        let manager = create_manager(db_path);
        debug_assert!(
            manager.level_size(1) > 2000,
            "actual: {}",
            manager.level_size(1)
        );
        manager.close();
    }
}
