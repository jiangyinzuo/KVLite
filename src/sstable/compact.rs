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
            let tables_fut = table_manager.assign_level0_tables_to_compact();
            let (level0_tables, min_key, max_key) = tables_fut.await;
            let level1_tables = table_manager
                .get_overlap_tables(1, &min_key, &max_key)
                .await;
            let new_table = table_manager.create_table(1, &min_key, &max_key);
            compact_to(new_table, level0_tables, level1_tables);
        });
    }
}

fn compact_to(
    new_table: Arc<TableHandle>,
    level0_tables: Vec<Arc<TableHandle>>,
    level1_tables: VecDeque<Arc<TableHandle>>,
) {
}
