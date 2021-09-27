use crate::collections::skip_list::skipmap::{IntoIter, IntoPtrIter, ReadWriteMode, SrSwSkipMap};
use crate::db::key_types::{DBKey, RawUserKey};
use crate::db::Value;
use crate::memory::MemTable;
use crate::sstable::manager::level_0::Level0Manager;
use crate::sstable::manager::level_n::LevelNManager;
use crate::sstable::table_handle::TableReadHandle;
use crate::wal::WAL;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;

pub const LEVEL0_FILES_THRESHOLD: usize = 4;

/// Merge all the `level0_table_handles` and `level1_tables` to `new_table`,
/// then insert `new_table` to `TableManager`.
/// In `level0_manager`, oldest table is at first
pub(crate) fn compact_and_insert<
    SK: 'static + DBKey,
    UK: 'static + DBKey,
    M: 'static + MemTable<SK, UK>,
    L: 'static + WAL<SK, UK>,
>(
    level0_manager: &Arc<Level0Manager<SK, UK, M, L>>,
    leveln_manager: &Arc<LevelNManager>,
    level0_table_handles: Vec<Arc<TableReadHandle>>,
    level1_table_handles: VecDeque<Arc<TableReadHandle>>,
) {
    let mut compactor = Compactor::new(
        level0_manager.clone(),
        leveln_manager.clone(),
        level0_table_handles,
        level1_table_handles,
    );
    compactor.run();
}

struct Compactor<SK: DBKey, UK: DBKey, M: MemTable<SK, UK>, L: WAL<SK, UK>> {
    level0_manager: Arc<Level0Manager<SK, UK, M, L>>,
    leveln_manager: Arc<LevelNManager>,
    level0_table_handles: Vec<Arc<TableReadHandle>>,
    level1_table_handles: VecDeque<Arc<TableReadHandle>>,
    #[cfg(debug_assertions)]
    kv_count: usize,
    _phantom_key: PhantomData<SK>,
    _phantom_uk: PhantomData<UK>,
    _phantom_table: PhantomData<M>,
}

impl<SK: 'static + DBKey, UK: 'static + DBKey, M: 'static + MemTable<SK, UK>, L: 'static>
    Compactor<SK, UK, M, L>
where
    L: WAL<SK, UK>,
{
    fn new(
        level0_manager: Arc<Level0Manager<SK, UK, M, L>>,
        leveln_manager: Arc<LevelNManager>,
        level0_table_handles: Vec<Arc<TableReadHandle>>,
        level1_table_handles: VecDeque<Arc<TableReadHandle>>,
    ) -> Compactor<SK, UK, M, L> {
        Compactor {
            level0_manager,
            leveln_manager,
            level0_table_handles,
            level1_table_handles,
            #[cfg(debug_assertions)]
            kv_count: 0,
            _phantom_key: PhantomData,
            _phantom_uk: PhantomData,
            _phantom_table: PhantomData,
        }
    }

    fn run(&mut self) {
        debug_assert!(!self.level0_table_handles.is_empty());

        let level0_skip_map: SrSwSkipMap<RawUserKey, Value> = self.merge_level0_tables();
        let mut kv_total = level0_skip_map.len();

        if self.level1_table_handles.is_empty() {
            let level1_table_size = (kv_total + 1) / self.level0_table_handles.len();
            debug_assert!(level1_table_size >= LEVEL0_FILES_THRESHOLD);

            let mut temp_kvs: Vec<(RawUserKey, Value)> = vec![];
            let iter: IntoIter<RawUserKey, Value, { ReadWriteMode::SrSw }> =
                level0_skip_map.into_iter();
            for (k, v) in iter {
                temp_kvs.push((k, v));
                #[cfg(debug_assertions)]
                {
                    self.kv_count += 1;
                }

                if temp_kvs.len() >= level1_table_size {
                    self.add_table_handle_from_vec(temp_kvs);
                    temp_kvs = vec![];
                }
            }
            if !temp_kvs.is_empty() {
                self.add_table_handle_from_vec(temp_kvs);
            }
        } else {
            for table in &self.level1_table_handles {
                kv_total += table.kv_total() as usize;
            }

            let level1_table_size = kv_total / self.level1_table_handles.len();
            debug_assert!(level1_table_size > 0);

            let mut temp_kvs = vec![];

            macro_rules! add_kv {
                ($key:expr, $value:expr) => {
                    temp_kvs.push(($key, $value));

                    #[cfg(debug_assertions)]
                    {
                        self.kv_count += 1;
                    }

                    if temp_kvs.len() >= level1_table_size {
                        self.add_table_handle_from_vec(temp_kvs);
                        temp_kvs = vec![];
                    }
                };
            }

            let mut level0_iter: IntoPtrIter<RawUserKey, Value, { ReadWriteMode::SrSw }> =
                level0_skip_map.into_ptr_iter();
            let mut kv = level0_iter.current_mut_no_consume();

            for level1_table_handle in self.level1_table_handles.iter() {
                for (level1_key, level1_value) in TableReadHandle::iter(level1_table_handle.clone())
                {
                    if kv.is_null() {
                        // write all the remain key-values in level1 tables.
                        add_kv!(level1_key, level1_value);
                        continue;
                    }

                    loop {
                        let level0_key = unsafe { &(*kv).entry.key };
                        debug_assert!(!level0_key.is_empty());
                        match level0_key.cmp(&level1_key) {
                            // set to level0_value
                            // drop level1_value
                            Ordering::Equal => {
                                let level0_entry = unsafe { std::mem::take(&mut (*kv).entry) };
                                let (level0_key, level0_value) = level0_entry.key_value();
                                add_kv!(level0_key, level0_value);
                                #[cfg(debug_assertions)]
                                {
                                    self.kv_count += 1;
                                }
                                kv = level0_iter.next_node();
                                break;
                            }
                            // insert level1_value
                            Ordering::Greater => {
                                add_kv!(level1_key, level1_value);
                                break;
                            }
                            // insert level0_value
                            Ordering::Less => {
                                let level0_entry = unsafe { std::mem::take(&mut (*kv).entry) };
                                let (level0_key, level0_value) = level0_entry.key_value();
                                add_kv!(level0_key, level0_value);
                                kv = level0_iter.next_node();
                                if kv.is_null() {
                                    add_kv!(level1_key, level1_value);
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            // write all the remain kv in level0 tables.
            while !kv.is_null() {
                unsafe {
                    let entry = std::mem::take(&mut (*kv).entry);
                    add_kv!(entry.key, entry.value);
                }
                kv = level0_iter.next_node();
            }

            if !temp_kvs.is_empty() {
                self.add_table_handle_from_vec(temp_kvs);
            }
        }

        #[cfg(debug_assertions)]
        {
            if self.kv_count != kv_total {
                error!("self.kv_count: {}, kv_total: {}", self.kv_count, kv_total);
            }
        }

        for table in &self.level1_table_handles {
            self.leveln_manager.ready_to_delete(table.clone());
        }
        for table in &self.level0_table_handles {
            self.level0_manager.ready_to_delete(table.table_id());
        }
        self.leveln_manager
            .may_compact(unsafe { NonZeroUsize::new_unchecked(1) });
    }

    fn merge_level0_tables(&self) -> SrSwSkipMap<RawUserKey, Value> {
        let skip_map = SrSwSkipMap::new();
        for table in &self.level0_table_handles {
            for (key, value) in TableReadHandle::iter(table.clone()) {
                skip_map.insert(key, value);
            }
        }
        skip_map
    }

    fn add_table_handle_from_vec(&self, temp_kvs: Vec<(RawUserKey, Value)>) {
        if !temp_kvs.is_empty() {
            let mut new_table = self.leveln_manager.create_table_write_handle(
                unsafe { NonZeroUsize::new_unchecked(1) },
                temp_kvs.len() as u32,
            );
            new_table.write_sstable_from_vec(temp_kvs).unwrap();
            self.leveln_manager.upsert_table_handle(new_table);
        }
    }
}
