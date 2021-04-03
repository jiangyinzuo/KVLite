use crate::collections::skip_list::skipmap::SkipMap;
use crate::sstable::manager::TableManager;
use crate::sstable::table_handle::TableHandle;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub const LEVEL0_FILES_THRESHOLD: usize = 7;

pub struct Level0Compacter {
    table_manager: std::sync::Arc<TableManager>,
    rt: Runtime,
}

impl Level0Compacter {
    pub fn new(table_manager: Arc<TableManager>) -> Level0Compacter {
        Level0Compacter {
            table_manager,
            rt: tokio::runtime::Builder::new_multi_thread().build().unwrap(),
        }
    }

    pub fn may_compact(&self) {
        let table_count = self.table_manager.file_count(0);
        if table_count > LEVEL0_FILES_THRESHOLD {
            self.start_compacting_task();
        }
    }

    fn start_compacting_task(&self) {
        let table_manager = self.table_manager.clone();

        self.rt.spawn(async move {
            let (level0_tables, min_key, max_key) = table_manager.assign_level0_tables_to_compact();
            let level1_tables = table_manager.get_overlap_tables(1, &min_key, &max_key);

            compact_and_insert(
                &table_manager,
                level0_tables,
                level1_tables,
                &min_key,
                &max_key,
            );
        });
    }
}

/// Merge all the `level0_tables` and `level1_tables` to `new_table`,
/// then insert `new_table` to `TableManager`.
fn compact_and_insert(
    table_manager: &Arc<TableManager>,
    level0_tables: Vec<Arc<TableHandle>>,
    level1_tables: VecDeque<Arc<TableHandle>>,
    min_key: &String,
    max_key: &String,
) {
    let level0_skip_map = merge_level0_tables(&level0_tables);

    if level1_tables.is_empty() {
        let level1_table_size = level0_skip_map.len() / LEVEL0_FILES_THRESHOLD;
        if level1_table_size == 0 {
            // create only one level1 table
            let new_table = table_manager.create_table_handle(1, &min_key, &max_key);
            new_table.write_sstable(&level0_skip_map).unwrap();
            table_manager.insert_table_handle(new_table);
        } else {
            let mut level0_kvs = level0_skip_map.iter();
            for (i, kv) in level0_kvs.enumerate() {}
        }
    } else {
        // let level0_entry = level0_kvs.next();

        // while level0_entry.is_some() && !level1_tables.is_empty() {}
    }

    // for table in level0_tables {
    //     table_manager.ready_to_delete(0, table.table_id());
    // }
}

fn merge_level0_tables(level0_tables: &Vec<Arc<TableHandle>>) -> SkipMap<String, String> {
    let mut skip_map = SkipMap::new();
    for table in level0_tables {
        for (key, value) in table.iter() {
            skip_map.insert(key, value);
        }
    }
    skip_map
}
