use crate::collections::treap::TreapMap;
use crate::db::MAX_LEVEL;
use crate::sstable::table_handle::{TableReadHandle, TableWriteHandle};
use crate::Result;
use rand::Rng;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Struct for adding and removing sstable files.
pub struct TableManager {
    db_path: String,
    level_tables: [std::sync::RwLock<BTreeMap<(String, u64), Arc<TableReadHandle>>>; MAX_LEVEL],
    level_sizes: [AtomicU64; MAX_LEVEL],
    next_table_id: [AtomicU64; MAX_LEVEL],
}

unsafe impl Sync for TableManager {}
unsafe impl Send for TableManager {}

impl TableManager {
    /// Open all the sstables at `db_path` when initializing DB.
    pub fn open_tables(db_path: String) -> TableManager {
        for i in 1..=MAX_LEVEL {
            std::fs::create_dir_all(format!("{}/{}", db_path, i)).unwrap();
        }

        let mut manager = TableManager {
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
        };

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
                            (handle.max_key().to_string(), handle.table_id()),
                            Arc::new(handle),
                        );
                    }

                    file_size += d.metadata().unwrap().len();
                } else {
                    // remove temporary file.
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
        }
        manager
    }

    pub fn get_level_tables_lock(
        &self,
        level: usize,
    ) -> &std::sync::RwLock<BTreeMap<(String, u64), Arc<TableReadHandle>>> {
        debug_assert!((1..=MAX_LEVEL).contains(&level));
        let lock = self.level_tables.get(level - 1).unwrap();
        lock
    }

    pub fn query_tables(&self, key: &String) -> Result<Option<String>> {
        for level in 1..=MAX_LEVEL {
            let tables_lock = self.get_level_tables_lock(level);
            let tables_guard = tables_lock.read().unwrap();

            // TODO: change this to binary search
            for table in tables_guard.values() {
                let option = table.query_sstable(key);
                if option.is_some() {
                    return Ok(option);
                }
            }
        }
        Ok(None)
    }

    fn get_next_table_id(&self, level: usize) -> u64 {
        debug_assert!((1..=MAX_LEVEL).contains(&level));
        unsafe {
            self.next_table_id
                .get_unchecked(level - 1)
                .fetch_add(1, Ordering::Release)
        }
    }

    pub fn upsert_table_handle(&self, handle: TableWriteHandle) {
        let file_size = handle.writer.writer.pos;
        debug_assert!(file_size > 0);

        let level = handle.level();
        let mut table_guard = self.get_level_tables_lock(level).write().unwrap();
        handle.rename();
        let handle = TableReadHandle::from_table_write_handle(handle);
        table_guard.insert(
            (handle.max_key().clone(), handle.table_id()),
            Arc::new(handle),
        );

        debug_assert!((1..=MAX_LEVEL).contains(&level));
        unsafe {
            self.level_sizes
                .get_unchecked(level - 1)
                .fetch_add(file_size, Ordering::Release);
        }
    }

    pub fn ready_to_delete(&self, table_handle: Arc<TableReadHandle>) {
        let level = table_handle.level();
        unsafe {
            self.level_sizes
                .get_unchecked(level - 1)
                .fetch_sub(table_handle.file_size(), Ordering::Release);
        }
        let mut guard = self.get_level_tables_lock(level).write().unwrap();
        guard
            .remove(&(table_handle.max_key().into(), table_handle.table_id()))
            .unwrap();

        table_handle.ready_to_delete();
    }

    /// Create a new sstable without `min_key` or `max_key`
    pub fn create_table_write_handle(&self, level: usize) -> TableWriteHandle {
        debug_assert!((1..=MAX_LEVEL).contains(&level));
        let next_table_id = self.get_next_table_id(level);
        TableWriteHandle::new(&self.db_path, level, next_table_id)
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
        level: usize,
        min_key: &String,
        max_key: &String,
    ) -> VecDeque<Arc<TableReadHandle>> {
        debug_assert!((1..=MAX_LEVEL).contains(&level));
        let tables_lock = self.get_level_tables_lock(level);
        let tables_guard = tables_lock.read().unwrap();

        let mut tables = VecDeque::new();

        // TODO: change this to O(logn)
        for (_key, handle) in tables_guard.iter() {
            if handle.is_overlapping(min_key, max_key) && handle.test_and_set_compacting() {
                let handle = handle.clone();
                tables.push_back(handle);
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
    pub fn size_over(&self, level: usize) -> bool {
        debug_assert!((1..=MAX_LEVEL).contains(&level));
        let size = self.level_size(level);
        size > 10u64.pow(level as u32) * 1024 * 1024
    }

    pub fn random_handle(&self, level: usize) -> Arc<TableReadHandle> {
        debug_assert!((1..=MAX_LEVEL).contains(&level));
        let lock = self.get_level_tables_lock(level);
        let guard = lock.read().unwrap();
        let mut rng = rand::thread_rng();
        let id = rng.gen_range(0..guard.len());
        let v = guard.values().nth(id).unwrap();
        v.clone()
    }
}
