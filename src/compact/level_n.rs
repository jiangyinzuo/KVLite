use crate::sstable::manager::level_n::LevelNManager;
use crate::sstable::table_handle::TableReadHandle;
use std::cmp::Ordering;
use std::num::NonZeroUsize;
use std::sync::Arc;

pub(crate) fn start_compact(
    compact_level: NonZeroUsize,
    handle_to_compact: Arc<TableReadHandle>,
    leveln_manager: Arc<LevelNManager>,
) {
    let mut compactor = Compactor::new(compact_level, handle_to_compact, leveln_manager);
    compactor.run();
}

struct Compactor {
    compact_level: NonZeroUsize,
    handle_to_compact: Arc<TableReadHandle>,
    leveln_manager: Arc<LevelNManager>,
    #[cfg(debug_assertions)]
    kv_count: usize,
}

impl Compactor {
    fn new(
        compact_level: NonZeroUsize,
        handle_to_compact: Arc<TableReadHandle>,
        leveln_manager: Arc<LevelNManager>,
    ) -> Compactor {
        debug_assert_eq!(handle_to_compact.level(), compact_level.get());
        Compactor {
            compact_level,
            handle_to_compact,
            leveln_manager,
            #[cfg(debug_assertions)]
            kv_count: 0,
        }
    }

    fn run(&mut self) {
        let next_level_table_handles = self.leveln_manager.get_overlap_tables(
            unsafe { NonZeroUsize::new_unchecked(self.compact_level.get() + 1) },
            self.handle_to_compact.min_key(),
            self.handle_to_compact.max_key(),
        );
        let mut total = self.handle_to_compact.kv_total() as usize;
        for handle in next_level_table_handles.iter() {
            total += handle.kv_total() as usize;
        }

        let new_table_size = total / next_level_table_handles.len().max(2) + 1;

        let mut temp_kvs = vec![];
        let mut table_to_compact_iter = self.handle_to_compact.iter();

        macro_rules! add_kv {
            ($key:expr, $value:expr) => {
                temp_kvs.push(($key, $value));
                #[cfg(debug_assertions)]
                {
                    self.kv_count += 1;
                }
                if temp_kvs.len() >= new_table_size {
                    self.add_table_handle(temp_kvs);
                    temp_kvs = vec![];
                }
            };
        }

        if next_level_table_handles.is_empty() {
            for (key, value) in table_to_compact_iter {
                add_kv!(key, value);
            }
        } else {
            enum CurLevelState {
                Start,
                HasValue((String, String)),
                End,
            }

            let mut cur_level_state = CurLevelState::Start;

            for next_level_table_handle in next_level_table_handles.iter() {
                for (next_level_key, next_level_value) in next_level_table_handle.iter() {
                    match cur_level_state {
                        CurLevelState::Start => loop {
                            let cur_level_kv = match table_to_compact_iter.next() {
                                Some(kv) => kv,
                                None => {
                                    add_kv!(next_level_key, next_level_value);
                                    cur_level_state = CurLevelState::End;
                                    break;
                                }
                            };
                            match cur_level_kv.0.cmp(&next_level_key) {
                                // reverse next level key-value
                                Ordering::Less => {
                                    add_kv!(cur_level_kv.0, cur_level_kv.1);
                                }
                                // drop next level key-value
                                Ordering::Equal => {
                                    add_kv!(cur_level_kv.0, cur_level_kv.1);
                                    #[cfg(debug_assertions)]
                                    {
                                        self.kv_count += 1;
                                    }
                                    break;
                                }
                                // remain current level key-value
                                Ordering::Greater => {
                                    add_kv!(next_level_key, next_level_value);
                                    cur_level_state = CurLevelState::HasValue(cur_level_kv);
                                    break;
                                }
                            }
                        },

                        CurLevelState::HasValue(mut cur_level_kv) => loop {
                            match cur_level_kv.0.cmp(&next_level_key) {
                                Ordering::Less => {
                                    add_kv!(cur_level_kv.0, cur_level_kv.1);
                                    match table_to_compact_iter.next() {
                                        Some(kv) => cur_level_kv = kv,
                                        None => {
                                            add_kv!(next_level_key, next_level_value);
                                            cur_level_state = CurLevelState::End;
                                            break;
                                        }
                                    }
                                }
                                Ordering::Equal => {
                                    add_kv!(cur_level_kv.0, cur_level_kv.1);
                                    #[cfg(debug_assertions)]
                                    {
                                        self.kv_count += 1;
                                    }
                                    cur_level_state = match table_to_compact_iter.next() {
                                        Some(kv) => CurLevelState::HasValue(kv),
                                        None => CurLevelState::End,
                                    };
                                    break;
                                }
                                Ordering::Greater => {
                                    add_kv!(next_level_key, next_level_value);
                                    cur_level_state = CurLevelState::HasValue(cur_level_kv);
                                    break;
                                }
                            }
                        },
                        CurLevelState::End => {
                            add_kv!(next_level_key, next_level_value);
                        }
                    }
                }
            }

            // write all the remain key-values in compact level table
            if let CurLevelState::HasValue(kv) = cur_level_state {
                add_kv!(kv.0, kv.1);
            }
            for kv in table_to_compact_iter {
                add_kv!(kv.0, kv.1);
            }
        }

        #[cfg(debug_assertions)]
        {
            if self.kv_count != total {
                error!("self.kv_count: {}, total: {}", self.kv_count, total);
            }
        }

        if !temp_kvs.is_empty() {
            self.add_table_handle(temp_kvs);
        }

        self.leveln_manager
            .ready_to_delete(self.handle_to_compact.clone());
        for table in next_level_table_handles {
            self.leveln_manager.ready_to_delete(table);
        }
        self.leveln_manager
            .may_compact(unsafe { NonZeroUsize::new_unchecked(self.compact_level.get() + 1) });
    }

    fn add_table_handle(&self, temp_kvs: Vec<(String, String)>) {
        debug_assert!(!temp_kvs.is_empty());
        let mut new_table = self.leveln_manager.create_table_write_handle(
            unsafe { NonZeroUsize::new_unchecked(self.compact_level.get() + 1) },
            temp_kvs.len() as u32,
        );
        new_table.write_sstable_from_vec(temp_kvs).unwrap();
        self.leveln_manager.upsert_table_handle(new_table);
    }
}

#[cfg(test)]
mod tests {
    use crate::compact::level_n::start_compact;
    use crate::sstable::manager::level_n::tests::create_manager;
    use crate::sstable::table_handle::temp_file_name;
    use std::num::NonZeroUsize;

    #[test]
    fn test_compact() {
        let path = tempfile::TempDir::new().unwrap();
        let db_path = path.path().to_str().unwrap();
        let manager = create_manager(db_path);

        let handle_args = vec![(1, 1, 100..120), (2, 1, 100..110), (2, 2, 112..130)];
        for (level, table_id, range) in &handle_args {
            let mut handle = manager.create_table_write_handle(
                NonZeroUsize::new(*level).unwrap(),
                (range.end - range.start) as u32,
            );
            let mut kvs = vec![];
            for i in range.clone() {
                kvs.push((format!("key{}", i), format!("value{}_{}", i, level)));
            }
            handle.write_sstable_from_vec(kvs).unwrap();
            debug_assert!(std::path::Path::new(&temp_file_name(&handle.file_path)).exists());

            assert_eq!(handle.max_key(), &format!("key{}", range.end - 1));
            manager.upsert_table_handle(handle);
        }

        assert!(manager.level_size(1) > 200);
        assert!(manager.level_size(2) > 200);

        let one = NonZeroUsize::new(1).unwrap();
        let handle_to_compact = manager.get_handle_to_compact(one).unwrap();
        assert_eq!(handle_to_compact.table_id(), 1);
        assert_eq!(handle_to_compact.max_key(), "key119");
        start_compact(one, handle_to_compact, manager.clone());
        assert_eq!(manager.level_size(1), 0);
    }
}
