use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::thread::JoinHandle;

use crossbeam_channel::Receiver;
use rand::Rng;

use crate::cache::ShardLRUCache;
use crate::collections::skip_list::skipmap::SkipMap;
use crate::compact::level_0::{compact_and_insert, LEVEL0_FILES_THRESHOLD};
use crate::db::key_types::{MemKey, UserKey};
use crate::db::{Value, ACTIVE_SIZE_THRESHOLD};
use crate::memory::MemTable;
use crate::sstable::manager::level_n::LevelNManager;
use crate::sstable::table_cache::IndexCache;
use crate::sstable::table_handle::{TableReadHandle, TableWriteHandle};
use crate::sstable::NUM_LEVEL0_TABLE_TO_COMPACT;
use crate::wal::simple_wal::SimpleWriteAheadLog;
use crate::Result;

/// Struct for read and write level0 sstable.
pub struct Level0Manager<K: MemKey, M: MemTable<K>> {
    db_path: String,

    level0_tables: std::sync::RwLock<BTreeMap<u64, Arc<TableReadHandle>>>,
    file_size: AtomicU64,

    table_manager: std::sync::Arc<LevelNManager>,
    sender: crossbeam_channel::Sender<bool>,

    /// Table ID is increasing order.
    wal: Arc<Mutex<SimpleWriteAheadLog>>,

    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    index_cache: Arc<ShardLRUCache<u64, IndexCache>>,

    background_task_write_to_level0_is_running: Arc<AtomicBool>,
    _phantom_key: PhantomData<K>,
    _phantom_table: PhantomData<M>,
}

impl<K: MemKey + 'static, M: MemTable<K> + 'static> Level0Manager<K, M> {
    fn open_tables(
        db_path: String,
        table_manager: Arc<LevelNManager>,
        wal: Arc<Mutex<SimpleWriteAheadLog>>,
        index_cache: Arc<ShardLRUCache<u64, IndexCache>>,
        background_task_write_to_level0_is_running: Arc<AtomicBool>,
    ) -> Result<Arc<Level0Manager<K, M>>> {
        std::fs::create_dir_all(format!("{}/0", db_path)).unwrap();
        let dir = std::fs::read_dir(format!("{}/0", db_path))?;

        let mut file_size = 0;
        let mut level0_tables = BTreeMap::new();
        for d in dir {
            let d = d.unwrap().path();
            let table_id = d
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
                .parse::<u64>();
            if let Ok(table_id) = table_id {
                file_size += d.metadata().unwrap().len();

                let handle = TableReadHandle::open_table(&db_path, 0, table_id);
                level0_tables.insert(handle.table_id(), Arc::new(handle));
            } else {
                // remove temporary file.
                std::fs::remove_file(d).unwrap();
            }
        }

        let (sender, receiver) = crossbeam_channel::unbounded();
        let level0_manager = Arc::new(Level0Manager {
            db_path,
            level0_tables: std::sync::RwLock::new(level0_tables),
            file_size: AtomicU64::new(file_size),
            table_manager,
            sender,
            wal,
            handle: Arc::new(Mutex::new(None)),
            index_cache,
            background_task_write_to_level0_is_running,
            _phantom_table: PhantomData,
            _phantom_key: PhantomData,
        });
        let handle = Self::start_compacting_task(level0_manager.clone(), receiver);
        {
            let mut guard = level0_manager.handle.lock().unwrap();
            *guard = Some(handle);
        }

        Ok(level0_manager)
    }

    /// Start a thread for writing immutable memory table to level0 sstable
    pub(crate) fn start_task_write_level0(
        db_path: String,
        leveln_manager: Arc<LevelNManager>,
        wal: Arc<Mutex<SimpleWriteAheadLog>>,
        imm_mem_table: Arc<RwLock<M>>,
        index_cache: Arc<ShardLRUCache<u64, IndexCache>>,
        recv: Receiver<()>,
        background_task_write_to_level0_is_running: Arc<AtomicBool>,
    ) -> (Arc<Level0Manager<K, M>>, JoinHandle<()>) {
        let manager = Self::open_tables(
            db_path,
            leveln_manager,
            wal,
            index_cache,
            background_task_write_to_level0_is_running,
        )
        .unwrap();
        let manager2 = manager.clone();

        let handle = thread::Builder::new()
            .name("level0 writer".to_owned())
            .spawn(move || {
                info!("thread `{}` start!", thread::current().name().unwrap());
                while let Ok(()) = recv.recv() {
                    debug_assert!(manager2
                        .background_task_write_to_level0_is_running
                        .load(Ordering::Acquire));
                    let imm_guard = imm_mem_table.read().unwrap();
                    if let Err(e) = manager2.write_to_table(imm_guard.deref()) {
                        let bt = std::backtrace::Backtrace::capture();
                        error!(
                            "Error in thread `{}`: {:?}",
                            thread::current().name().unwrap(),
                            e
                        );
                        println!("{:#?}", bt);
                    }
                    manager2
                        .background_task_write_to_level0_is_running
                        .store(false, Ordering::Release);
                }
                info!("thread `{}` exit!", thread::current().name().unwrap());
            })
            .unwrap();
        (manager, handle)
    }

    /// Persistently write the `table` to disk.
    fn write_to_table(&self, table: &M) -> Result<()> {
        let mut handle = self.create_table_write_handle(table.len() as u32);
        handle.write_sstable(table)?;
        self.insert_table_handle(handle);
        self.delete_imm_table_log()?;
        self.may_compact();
        Ok(())
    }

    // delete immutable log after writing to level0 sstable
    fn delete_imm_table_log(&self) -> Result<()> {
        let mut wal_guard = self.wal.lock().unwrap();
        wal_guard.clear_imm_log()?;
        Ok(())
    }

    pub fn may_compact(&self) {
        let table_count = self.file_count();
        if table_count > LEVEL0_FILES_THRESHOLD || self.size_over() {
            if let Err(e) = self.sender.send(true) {
                warn!("{:#?}", e);
            }
        }
    }

    fn start_compacting_task(
        level0_manager: Arc<Level0Manager<K, M>>,
        receiver: Receiver<bool>,
    ) -> JoinHandle<()> {
        let table_manager = level0_manager.table_manager.clone();
        std::thread::spawn(move || {
            let table_manager = table_manager;
            let level0_manager = level0_manager;
            info!("compact 0 task start");
            while let Ok(true) = receiver.recv() {
                let table_count = level0_manager.file_count();
                if table_count > LEVEL0_FILES_THRESHOLD {
                    let (level0_tables, min_key, max_key) =
                        level0_manager.assign_level0_tables_to_compact();
                    let level1_tables = table_manager.get_overlap_tables(
                        unsafe { NonZeroUsize::new_unchecked(1) },
                        &min_key,
                        &max_key,
                    );
                    compact_and_insert(
                        &level0_manager,
                        &table_manager,
                        level0_tables,
                        level1_tables,
                    );
                }
            }
            info!("compact 0 task exit!");
        })
    }

    #[inline]
    pub fn get_level0_tables_lock(
        &self,
    ) -> &std::sync::RwLock<BTreeMap<u64, Arc<TableReadHandle>>> {
        &self.level0_tables
    }

    pub fn range_query(
        &self,
        key_start: &UserKey,
        key_end: &UserKey,
        kvs: &mut SkipMap<UserKey, Value>,
    ) {
        let tables_guard = self.level0_tables.read().unwrap();

        // query the latest table first
        for table in tables_guard.values().rev() {
            table.range_query(key_start, key_end, kvs);
        }
    }

    pub fn query(&self, key: &UserKey) -> Result<Option<Value>> {
        let tables_guard = self.level0_tables.read().unwrap();

        // query the latest table first
        for table in tables_guard.values().rev() {
            // get cache
            let entry_tracker = self.index_cache.look_up(&table.table_key(), table.hash());
            let option = if !entry_tracker.0.is_null() {
                let index_cache = unsafe { (*entry_tracker.0).value() };
                table.query_sstable_with_cache(key, &index_cache)
            } else {
                table.query_sstable(key, &self.index_cache)
            };

            if option.is_some() {
                return Ok(option);
            }
        }
        Ok(None)
    }

    fn get_next_table_id(&self) -> u64 {
        let table_guard = self.level0_tables.read().unwrap();
        match table_guard.last_key_value() {
            Some((k, _v)) => k + 1,
            None => 1,
        }
    }

    fn insert_table_handle(&self, handle: TableWriteHandle) {
        let file_size = handle.writer.writer.pos;
        debug_assert!(file_size > 0);
        debug_assert_eq!(handle.level(), 0);

        let handle = Arc::new(TableReadHandle::from_table_write_handle(handle));
        let mut table_guard = self.level0_tables.write().unwrap();

        table_guard.insert(handle.table_id(), handle);
        self.file_size.fetch_add(file_size, Ordering::Release);
    }

    pub fn create_table_write_handle(&self, kv_total: u32) -> TableWriteHandle {
        let next_table_id = self.get_next_table_id();
        TableWriteHandle::new(&self.db_path, 0, next_table_id, kv_total)
    }

    /// Get sstable file count of level 0, used for judging whether need compacting.
    fn file_count(&self) -> usize {
        let guard = self.level0_tables.read().unwrap();
        guard.len()
    }

    pub fn ready_to_delete(&self, table_id: u64) {
        let mut guard = self.level0_tables.write().unwrap();
        let table_handle = guard.remove(&table_id).unwrap();

        self.file_size
            .fetch_sub(table_handle.file_size(), Ordering::Release);

        table_handle.ready_to_delete();
        self.index_cache
            .erase(&table_handle.table_key(), table_handle.hash());
    }

    /// Get total size of sstables in level 0
    #[inline]
    pub(crate) fn level_size(&self) -> u64 {
        self.file_size.load(Ordering::Acquire)
    }

    /// If total size of level 0 is larger than 1 MB, it should be compacted.
    fn size_over(&self) -> bool {
        let size = self.level_size();
        size > ACTIVE_SIZE_THRESHOLD as u64 * LEVEL0_FILES_THRESHOLD as u64 * 10
    }

    pub fn random_handle(&self) -> Arc<TableReadHandle> {
        let guard = self.level0_tables.read().unwrap();
        let mut rng = rand::thread_rng();
        let id = rng.gen_range(0..guard.len());
        let v = guard.values().nth(id).unwrap();
        v.clone()
    }

    /// Return level0 tables to compact
    pub fn assign_level0_tables_to_compact(&self) -> (Vec<Arc<TableReadHandle>>, UserKey, UserKey) {
        let guard = self.level0_tables.read().unwrap();

        let mut tables = Vec::new();
        tables.reserve(NUM_LEVEL0_TABLE_TO_COMPACT);

        let mut count = 0;
        let mut min_key: Option<&UserKey> = None;
        let max = UserKey::default();
        let mut max_key: &UserKey = &max;
        for (_id, table) in guard.iter() {
            if table.test_and_set_compacting() {
                tables.push(table.clone());
                count += 1;
                let keys = table.min_max_key();
                min_key = match min_key {
                    Some(m) => Some(m.min(keys.0)),
                    None => Some(keys.0),
                };
                max_key = max_key.max(keys.1);
                if count >= NUM_LEVEL0_TABLE_TO_COMPACT {
                    break;
                }
            }
        }
        (tables, min_key.unwrap().clone(), max_key.clone())
    }

    pub(crate) fn close(&self) {
        self.sender.send(false).unwrap();
        let mut guard = self.handle.lock().unwrap();
        let handle = guard.take().unwrap();
        handle.join().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::Duration;

    use tempfile::TempDir;

    use crate::db::key_types::UserKey;
    use crate::db::DBCommand;
    use crate::db::ACTIVE_SIZE_THRESHOLD;
    use crate::memory::{SkipMapMemTable, UserKeyValueIterator};
    use crate::sstable::manager::level_0::Level0Manager;
    use crate::sstable::manager::level_n::tests::create_manager;
    use crate::wal::simple_wal::SimpleWriteAheadLog;

    #[test]
    fn test() {
        let _ = env_logger::try_init();

        let temp = TempDir::new().unwrap();
        let path = temp.path().to_str().unwrap().to_string();

        for i in 0..10 {
            test_query(path.clone(), i == 0);
            println!("test {} ok", i);
        }
    }

    fn test_query(path: String, insert_value: bool) {
        let leveln_manager = create_manager(&path);

        let mut mut_mem = SkipMapMemTable::<UserKey>::default();

        let (sender, receiver) = crossbeam_channel::unbounded();
        let wal = SimpleWriteAheadLog::open_and_load_logs(&path, &mut mut_mem).unwrap();

        assert!(mut_mem.is_empty());

        let imm_mem = Arc::new(RwLock::new(SkipMapMemTable::default()));

        let background = Arc::new(AtomicBool::default());
        let (manager, handle) = Level0Manager::start_task_write_level0(
            path,
            leveln_manager.clone(),
            Arc::new(Mutex::new(wal)),
            imm_mem.clone(),
            leveln_manager.index_cache.clone(),
            receiver,
            background,
        );

        if insert_value {
            assert_eq!(manager.level_size(), 0);
            let mut imm_mem_guard = imm_mem.write().unwrap();
            for i in 0..ACTIVE_SIZE_THRESHOLD * 4 {
                imm_mem_guard
                    .set(
                        format!("key{}", i).into_bytes(),
                        format!("value{}", i).into_bytes(),
                    )
                    .unwrap();
            }
            manager
                .background_task_write_to_level0_is_running
                .store(true, std::sync::atomic::Ordering::Release);
            sender.send(()).unwrap();
        }

        // wait for writing data
        std::thread::sleep(Duration::from_secs(1));

        assert!(manager.level_size() > 0);

        for i in 0..ACTIVE_SIZE_THRESHOLD * 4 {
            let key = format!("key{}", i).into_bytes();
            let v = manager
                .query(&key)
                .unwrap()
                .unwrap_or_else(|| leveln_manager.query(&key).unwrap().unwrap());
            assert_eq!(format!("value{}", i).into_bytes(), v);
        }

        drop(sender);
        handle.join().unwrap();
    }
}
