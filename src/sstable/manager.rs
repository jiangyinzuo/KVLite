use crate::db::MAX_LEVEL;
use crate::sstable::table_handle::TableHandle;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

pub struct TableManager {
    db_path: String,
    level_tables: [RwLock<BTreeMap<u128, Arc<RwLock<TableHandle>>>>; MAX_LEVEL + 1],
}

impl TableManager {
    pub fn open_tables(db_path: String) -> TableManager {
        for i in 0..=MAX_LEVEL {
            std::fs::create_dir_all(format!("{}/{}", db_path, i)).unwrap();
        }
        let mut manager = TableManager {
            db_path,
            level_tables: [
                RwLock::default(),
                RwLock::default(),
                RwLock::default(),
                RwLock::default(),
                RwLock::default(),
                RwLock::default(),
                RwLock::default(),
                RwLock::default(),
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
                        let mut guard = manager.level_tables.get_unchecked_mut(i).write().unwrap();
                        guard.insert(table_id, Arc::new(RwLock::new(handle)));
                    }
                } else {
                    // remove temporary file.
                    std::fs::remove_file(path).unwrap();
                }
            }
        }
        manager
    }

    pub fn get_level_tables_lock(
        &self,
        level: usize,
    ) -> &RwLock<BTreeMap<u128, Arc<RwLock<TableHandle>>>> {
        let lock = self.level_tables.get(level).unwrap();
        lock
    }

    pub fn create_table(&self, level: usize) -> Arc<RwLock<TableHandle>> {
        let mut table_guard = self.get_level_tables_lock(level).write().unwrap();
        let next_table_id = match table_guard.last_key_value() {
            Some((k, v)) => k + 1,
            None => 1,
        };
        let handle = TableHandle::new(&self.db_path, level as u8, next_table_id);
        let handle = Arc::new(RwLock::new(handle));
        table_guard.insert(next_table_id, handle.clone());
        handle
    }
}
