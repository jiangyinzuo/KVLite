use crate::collections::skip_list::skipmap::{Iter, SkipMap};
use crate::sstable::manager::TableManager;
use crate::sstable::table_handle::TableReadHandle;
use crate::sstable::MAX_BLOCK_KV_PAIRS;
use crossbeam_channel::Receiver;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub const LEVEL0_FILES_THRESHOLD: usize = 7;

pub struct Level0Compactor {
    table_manager: std::sync::Arc<TableManager>,
    sender: crossbeam_channel::Sender<()>,
    rt: Arc<Runtime>,
}

impl Level0Compactor {
    pub fn new(table_manager: Arc<TableManager>, rt: Arc<Runtime>) -> Level0Compactor {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let compactor = Level0Compactor {
            table_manager,
            sender,
            rt,
        };
        compactor.start_compacting_task(receiver);
        compactor
    }

    pub fn may_compact(&self) {
        let table_count = self.table_manager.file_count(0);
        if table_count > LEVEL0_FILES_THRESHOLD {
            self.sender.send(()).unwrap();
        }
    }

    fn start_compacting_task(&self, receiver: Receiver<()>) {
        let table_manager = self.table_manager.clone();

        self.rt.spawn(async move {
            while let Ok(()) = receiver.recv() {
                let table_count = table_manager.file_count(0);
                if table_count > LEVEL0_FILES_THRESHOLD {
                    let (level0_tables, min_key, max_key) =
                        table_manager.assign_level0_tables_to_compact();
                    let level1_tables = table_manager.get_overlap_tables(1, &min_key, &max_key);
                    compact_and_insert(
                        &table_manager,
                        level0_tables,
                        level1_tables,
                        min_key,
                        max_key,
                    );
                }
            }
        });
    }
}

/// Merge all the `level0_tables` and `level1_tables` to `new_table`,
/// then insert `new_table` to `TableManager`.
fn compact_and_insert(
    table_manager: &Arc<TableManager>,
    level0_tables: Vec<Arc<TableReadHandle>>,
    mut level1_tables: VecDeque<Arc<TableReadHandle>>,
    level0_min_key: String,
    level0_max_key: String,
) {
    let level0_skip_map = merge_level0_tables(&level0_tables);

    if level1_tables.is_empty() {
        let level1_table_size = level0_skip_map.len() / LEVEL0_FILES_THRESHOLD;
        if level1_table_size == 0 {
            // create only one level1 table
            let mut new_table = table_manager.create_table_write_handle(1);
            new_table.write_sstable(&level0_skip_map).unwrap();
            table_manager.insert_table_handle(new_table, level0_min_key, level0_max_key);
        } else {
            let level0_kvs = level0_skip_map.iter();
            let mut temp_kvs = vec![];
            for (i, kv) in level0_kvs.enumerate() {
                unsafe {
                    temp_kvs.push((&(*kv).entry.key, &(*kv).entry.value));
                }
                if (i + 1) % level1_table_size == 0 {
                    let min_key = temp_kvs.first().unwrap().0;
                    let max_key = temp_kvs.last().unwrap().0;
                    let mut new_table = table_manager.create_table_write_handle(1);
                    new_table.write_sstable_from_vec(temp_kvs).unwrap();
                    table_manager.insert_table_handle(new_table, min_key.clone(), max_key.clone());
                    temp_kvs = vec![];
                }
            }
            if !temp_kvs.is_empty() {
                let min_key = temp_kvs.first().unwrap().0;
                let max_key = temp_kvs.last().unwrap().0;
                let mut new_table = table_manager.create_table_write_handle(1);
                new_table.write_sstable_from_vec(temp_kvs).unwrap();
                table_manager.insert_table_handle(new_table, min_key.clone(), max_key.clone());
            }
        }
    } else {
        let mut level0_iter = level0_skip_map.iter();
        while !level1_tables.is_empty() {
            let level1_table = match level1_tables.pop_front() {
                Some(elem) => elem,
                None => unsafe { std::hint::unreachable_unchecked() },
            };
            merge_to_level1_table(&mut level0_iter, level1_table, table_manager);
        }
        if !level0_iter.next_no_consume().is_null() {
            let mut new_table = table_manager.create_table_write_handle(1);
            new_table.write_sstable_from_iter(level0_iter).unwrap();
            table_manager.insert_table_handle(new_table, level0_min_key, level0_max_key);
        }
    }

    for table in level0_tables {
        table_manager.ready_to_delete(0, table.table_id());
    }
}

fn merge_level0_tables(level0_tables: &[Arc<TableReadHandle>]) -> SkipMap<String, String> {
    let mut skip_map = SkipMap::new();
    for table in level0_tables {
        for (key, value) in table.iter() {
            skip_map.insert(key, value);
        }
    }
    skip_map
}

/// Merge `level0_iter` and `old_level1_table` to a new level1 sstable,
/// add the new sstable to `table_manager`,
/// return file size of the new sstable
fn merge_to_level1_table(
    level0_iter: &mut Iter<String, String>,
    old_level1_table: Arc<TableReadHandle>,
    table_manager: &Arc<TableManager>,
) {
    let (mut min_key, mut max_key) = old_level1_table.min_max_key();

    let level1_iter = old_level1_table.iter();
    let mut cur_level0_node = level0_iter.next_no_consume();
    let mut new_table = table_manager.create_table_write_handle(1);

    for (level1_key, level1_value) in level1_iter {
        loop {
            if cur_level0_node.is_null() {
                new_table.writer.write_key_value(&level1_key, &level1_value);
                if new_table.writer.count == MAX_BLOCK_KV_PAIRS {
                    new_table.writer.add_index(level1_key.to_string());
                }
                break;
            } else {
                unsafe {
                    let level0_key = &(*cur_level0_node).entry.key;
                    let level0_value = &(*cur_level0_node).entry.value;

                    min_key = min_key.min(level0_key);
                    max_key = max_key.max(level0_key);

                    match level0_key.cmp(&level1_key) {
                        // update level1_value to level0_value
                        Ordering::Equal => {
                            new_table
                                .writer
                                .write_key_value_and_try_add_index(level0_key, level0_value);
                            break;
                        }
                        // insert level1_value
                        Ordering::Greater => {
                            new_table
                                .writer
                                .write_key_value_and_try_add_index(&level1_key, &level1_value);
                            break;
                        }
                        // insert level0_value
                        Ordering::Less => {
                            new_table
                                .writer
                                .write_key_value_and_try_add_index(level0_key, level0_value);
                            cur_level0_node = level0_iter.next_node();
                        }
                    }
                }
            }
        }
        if level1_key.eq(old_level1_table.max_key()) && new_table.writer.count > 0 {
            new_table.writer.add_index(level1_key.clone());
        }
    }
    new_table.writer.write_index_and_footer();
    table_manager.insert_table_handle(new_table, min_key.to_string(), max_key.to_string());
}
