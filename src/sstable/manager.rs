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
                    let handle = TableHandle::new(&manager.db_path, i as _, table_id);

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

    pub async fn create_table(&self, level: usize) -> Arc<TableHandle> {
        let mut table_guard = self.get_level_tables_lock(level).write().await;
        let next_table_id = match table_guard.last_key_value() {
            Some((k, _v)) => k + 1,
            None => 1,
        };
        let handle = TableHandle::new(&self.db_path, level as u8, next_table_id);
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
    pub async fn assign_level0_tables_to_compact(&self) -> Vec<Arc<TableHandle>> {
        let tables = unsafe { self.level_tables.get_unchecked(0) };
        let guard = tables.read().await;

        let mut result = Vec::new();
        result.reserve(NUM_LEVEL0_TABLE_TO_COMPACT);

        for (_id, table) in guard.iter() {
            if table.test_and_set_compacting().await {
                result.push(table.clone());
            }
        }
        result
    }
}
