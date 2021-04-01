use crate::collections::skip_list::skipmap::SkipMap;
use crate::sstable::manager::TableManager;
use crate::sstable::table_handle::TableHandle;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub const LEVEL0_FILES_THRESHOLD: usize = 4;

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
            let new_table = table_manager.create_table(1, &min_key, &max_key);
            compact_to(&table_manager, new_table, level0_tables, level1_tables);
        });
    }
}

/// Merge all the `level0_tables` and `level1_tables` to `new_table`
fn compact_to(
    table_manager: &Arc<TableManager>,
    new_table: Arc<TableHandle>,
    level0_tables: Vec<Arc<TableHandle>>,
    level1_tables: VecDeque<Arc<TableHandle>>,
) {
    let skip_map = merge_level0_tables(&level0_tables);
    let mut level0_kvs = skip_map.iter();
    let level0_entry = level0_kvs.next();

    if level1_tables.is_empty() {
        new_table.write_sstable(&skip_map).unwrap();
    }

    // TODO
    // while level0_entry.is_some() && !level1_tables.is_empty() {}
    for table in level0_tables {
        table_manager.ready_to_delete(0, table.table_id());
    }
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
