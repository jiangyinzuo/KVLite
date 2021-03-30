use crate::sstable::manager::TableManager;
use std::sync::Arc;

pub const LEVEL0_FILES_THRESHOLD: usize = 4;

pub struct Level0Compacter {
    table_manager: std::sync::Arc<TableManager>,
}

impl Level0Compacter {
    pub fn new(table_manager: Arc<TableManager>) -> Level0Compacter {
        Level0Compacter { table_manager }
    }

    pub async fn may_compact(&self) {
        let table_count = self.table_manager.file_count(0).await;
        if table_count > LEVEL0_FILES_THRESHOLD {
            self.do_compact();
        }
    }

    fn start_compacting_task(&self) {
        let table_manager = self.table_manager.clone();
        tokio::spawn(async move {
            let tables_fut = table_manager.assign_level0_tables_to_compact();
            let table = tables_fut.await;
        });
    }

    fn do_compact(&self) {}
}
