use crate::db::MAX_LEVEL;
use crate::sstable::table_handle::TableHandle;
use crate::sstable::NUM_LEVEL0_TABLE_TO_COMPACT;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Struct for adding and removing sstable files.
pub struct TableManager {
    db_path: String,
    level_tables: [tokio::sync::RwLock<BTreeMap<u128, Arc<TableHandle>>>; MAX_LEVEL + 1],
}

unsafe impl Sync for TableManager {}
unsafe impl Send for TableManager {}

impl TableManager {
    /// Open all the sstables at `db_path` when initializing DB.
    pub async fn open_tables(db_path: String) -> TableManager {
        for i in 0..=MAX_LEVEL {
            tokio::fs::create_dir_all(format!("{}/{}", db_path, i))
                .await
                .unwrap();
        }

        let mut manager = TableManager {
            db_path,
            level_tables: [
                tokio::sync::RwLock::default(),
                tokio::sync::RwLock::default(),
                tokio::sync::RwLock::default(),
                tokio::sync::RwLock::default(),
                tokio::sync::RwLock::default(),
                tokio::sync::RwLock::default(),
                tokio::sync::RwLock::default(),
                tokio::sync::RwLock::default(),
            ],
        };

        for i in 0..=MAX_LEVEL {
            let dir = std::fs::read_dir(format!("{}/{}", &manager.db_path, i)).unwrap();
            for d in dir {
                let path = d.unwrap().path();

                // The file whose file_name is a number is considered as sstable.
                if let Ok(table_id) = path.file_name().unwrap().to_str().unwrap().parse::<u128>() {
                    let handle = TableHandle::open_table(&manager.db_path, i as _, table_id);

                    // Safety: i is in range [0, MAX_LEVEL]
                    unsafe {
                        let mut guard = manager.level_tables.get_unchecked_mut(i).write().await;
                        guard.insert(table_id, Arc::new(handle));
                    }
                } else {
                    // remove temporary file.
                    tokio::fs::remove_file(path).await.unwrap();
                }
            }
        }
        manager
    }

    pub fn get_level_tables_lock(
        &self,
        level: usize,
    ) -> &tokio::sync::RwLock<BTreeMap<u128, Arc<TableHandle>>> {
        let lock = self.level_tables.get(level).unwrap();
        lock
    }

    /// Create a new sstable with `level`, `min_key` and `max_key`.
    pub async fn create_table(
        &self,
        level: usize,
        min_key: &str,
        max_key: &str,
    ) -> Arc<TableHandle> {
        let mut table_guard = self.get_level_tables_lock(level).write().await;
        let next_table_id = match table_guard.last_key_value() {
            Some((k, _v)) => k + 1,
            None => 1,
        };
        let handle = TableHandle::new(&self.db_path, level as u8, next_table_id, min_key, max_key);
        let handle = Arc::new(handle);
        table_guard.insert(next_table_id, handle.clone());
        handle
    }

    /// Get sstable file count of `level`, used for judging whether need compacting.
    pub async fn file_count(&self, level: usize) -> usize {
        let tables = self.level_tables.get(level).unwrap();
        let guard = tables.read().await;
        guard.len()
    }

    /// Return level0 tables to compact
    pub async fn assign_level0_tables_to_compact(&self) -> (Vec<Arc<TableHandle>>, String, String) {
        let tables = unsafe { self.level_tables.get_unchecked(0) };
        let guard = tables.read().await;

        let mut tables = Vec::new();
        tables.reserve(NUM_LEVEL0_TABLE_TO_COMPACT);

        let mut count = 0;
        let mut min_key = "";
        let mut max_key = "";
        for (_id, table) in guard.iter() {
            if table.test_and_set_compacting().await {
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
    pub async fn get_overlap_tables(
        &self,
        level: usize,
        min_key: &String,
        max_key: &String,
    ) -> Vec<Arc<TableHandle>> {
        let tables_lock = self.get_level_tables_lock(level);
        let tables_guard = tables_lock.read().await;

        let mut tables = vec![];

        // TODO: change this to O(logn)
        for (_table_id, handle) in tables_guard.iter() {
            if handle.is_overlapping(min_key, max_key) {
                tables.push(handle.clone());
            }
        }
        tables
    }
}
