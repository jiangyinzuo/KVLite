use crate::sstable::manager::TableManager;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
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
            self.start_compacting_task();
        }
    }

    fn start_compacting_task(&self) {
        let table_manager = self.table_manager.clone();
        tokio::spawn(async move {
            let tables_fut = table_manager.assign_level0_tables_to_compact();
            let tables = tables_fut.await;

            let futs = tables.iter().map(|table| table.get_min_max_key());
            let mut futs_unordered = futs.collect::<FuturesUnordered<_>>();

            if let Some((mut min_key, mut max_key)) = futs_unordered.next().await {
                while let Some(res) = futs_unordered.next().await {
                    min_key = min_key.min(res.0);
                    max_key = max_key.min(res.1);
                }
                println!("{} {}", min_key, max_key);
            }
        });
    }
}
