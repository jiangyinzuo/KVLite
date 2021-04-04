use crate::db::MAX_LEVEL;
use crate::sstable::table_handle::{TableReadHandle, TableWriteHandle};
use crate::sstable::NUM_LEVEL0_TABLE_TO_COMPACT;
use crate::Result;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Struct for adding and removing sstable files.
pub struct TableManager {
    db_path: String,
    level_tables: [std::sync::RwLock<BTreeMap<u128, Arc<TableReadHandle>>>; MAX_LEVEL + 1],
    level_sizes: [AtomicU64; MAX_LEVEL + 1],
}

unsafe impl Sync for TableManager {}
unsafe impl Send for TableManager {}

impl TableManager {
    /// Open all the sstables at `db_path` when initializing DB.
    pub fn open_tables(db_path: String) -> TableManager {
        for i in 0..=MAX_LEVEL {
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
                AtomicU64::default(),
            ],
        };

        for i in 0..=MAX_LEVEL {
            let dir = std::fs::read_dir(format!("{}/{}", &manager.db_path, i)).unwrap();
            let mut file_size = 0;
            for d in dir {
                let d = d.unwrap();
                let path = d.path();
                // The file whose file_name is a number is considered as sstable.
                if let Ok(table_id) = path.file_name().unwrap().to_str().unwrap().parse::<u128>() {
                    let handle = TableReadHandle::open_table(&manager.db_path, i as _, table_id);

                    // Safety: i is in range [0, MAX_LEVEL]
                    unsafe {
                        let mut guard = manager.level_tables.get_unchecked_mut(i).write().unwrap();
                        guard.insert(table_id, Arc::new(handle));
                    }
                    file_size += d.metadata().unwrap().len();
                } else {
                    // remove temporary file.
                    std::fs::remove_file(path).unwrap();
                }
            }
            unsafe {
                manager
                    .level_sizes
                    .get_unchecked(i)
                    .store(file_size, Ordering::Release);
            }
        }
        manager
    }

    pub fn get_level_tables_lock(
        &self,
        level: usize,
    ) -> &std::sync::RwLock<BTreeMap<u128, Arc<TableReadHandle>>> {
        let lock = self.level_tables.get(level).unwrap();
        lock
    }

    fn query_level0_tables(&self, key: &String) -> Result<Option<String>> {
        let tables_lock = self.get_level_tables_lock(0);
        let tables_guard = tables_lock.read().unwrap();

        // query the latest table first
        for table in tables_guard.values().rev() {
            let option = table.query_sstable(key);
            if option.is_some() {
                return Ok(option);
            }
        }
        Ok(None)
    }

    pub fn query_tables(&self, key: &String) -> Result<Option<String>> {
        let option = self.query_level0_tables(key)?;
        if option.is_some() {
            return Ok(option);
        }

        for level in 1..=MAX_LEVEL {
            let tables_lock = self.get_level_tables_lock(level);
            let tables_guard = tables_lock.read().unwrap();

            // query the latest table first
            for table in tables_guard.values().rev() {
                let option = table.query_sstable(key);
                if option.is_some() {
                    return Ok(option);
                }
            }
        }
        Ok(None)
    }

    fn get_next_table_id(&self, level: usize) -> u128 {
        let table_guard = self.get_level_tables_lock(level).read().unwrap();
        match table_guard.last_key_value() {
            Some((k, _v)) => k + 1,
            None => 1,
        }
    }

    pub fn insert_table_handle(&self, handle: TableWriteHandle, min_key: String, max_key: String) {
        let file_size = handle.writer.writer.pos;
        debug_assert!(file_size > 0);

        let level = handle.level();
        let mut table_guard = self.get_level_tables_lock(level).write().unwrap();
        handle.rename();
        let handle = TableReadHandle::from_table_write_handle(handle, min_key, max_key);
        table_guard.insert(handle.table_id(), Arc::new(handle));
        unsafe {
            self.level_sizes
                .get_unchecked(level)
                .fetch_add(file_size, Ordering::Release);
        }
    }

    /// Create a new sstable without `min_key` or `max_key`
    pub fn create_table_write_handle(&self, level: usize) -> TableWriteHandle {
        let next_table_id = self.get_next_table_id(level);
        TableWriteHandle::new(&self.db_path, level, next_table_id)
    }

    /// Get sstable file count of `level`, used for judging whether need compacting.
    pub fn file_count(&self, level: usize) -> usize {
        let tables = self.level_tables.get(level).unwrap();
        let guard = tables.read().unwrap();
        guard.len()
    }

    /// Return level0 tables to compact
    pub fn assign_level0_tables_to_compact(&self) -> (Vec<Arc<TableReadHandle>>, String, String) {
        let tables = unsafe { self.level_tables.get_unchecked(0) };
        let guard = tables.read().unwrap();

        let mut tables = Vec::new();
        tables.reserve(NUM_LEVEL0_TABLE_TO_COMPACT);

        let mut count = 0;
        let mut min_key = "";
        let mut max_key = "";
        for (_id, table) in guard.iter() {
            if table.test_and_set_compacting() {
                tables.push(table.clone());
                count += 1;
                let keys = table.min_max_key();
                min_key = min_key.min(keys.0);
                max_key = if max_key.is_empty() {
                    keys.1
                } else {
                    max_key.max(keys.1)
                };
                if count >= NUM_LEVEL0_TABLE_TO_COMPACT {
                    break;
                }
            }
        }
        (tables, min_key.to_string(), max_key.to_string())
    }

    /// Get tables in `level` that intersect with [`min_key`, `max_key`].
    pub fn get_overlap_tables(
        &self,
        level: usize,
        min_key: &String,
        max_key: &String,
    ) -> VecDeque<Arc<TableReadHandle>> {
        let tables_lock = self.get_level_tables_lock(level);
        let tables_guard = tables_lock.read().unwrap();

        let mut tables = VecDeque::new();

        // TODO: change this to O(logn)
        for (_table_id, handle) in tables_guard.iter() {
            if handle.is_overlapping(min_key, max_key) {
                let handle = handle.clone();
                tables.push_back(handle);
            }
        }
        tables
    }

    pub fn ready_to_delete(&self, level: usize, table_id: u128) {
        let lock = self.get_level_tables_lock(level);
        let mut guard = lock.write().unwrap();
        let table_handle = guard.remove(&table_id).unwrap();

        unsafe {
            self.level_sizes
                .get_unchecked(level)
                .fetch_sub(table_handle.file_size(), Ordering::Release);
        }

        table_handle.ready_to_delete();
        debug!("count of TableHandle: {}", Arc::strong_count(&table_handle));
    }

    /// Get total size of sstables in `level`
    pub(crate) fn level_size(&self, level: usize) -> u64 {
        unsafe {
            self.level_sizes
                .get_unchecked(level)
                .load(Ordering::Acquire)
        }
    }
}
