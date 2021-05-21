use crate::cache::ShardLRUCache;
use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::key_types::MemKey;
use crate::db::{Value, ACTIVE_SIZE_THRESHOLD, DB};
use crate::memory::MemTable;
use crate::sstable::manager::level_0::Level0Manager;
use crate::sstable::manager::level_n::LevelNManager;
use crate::wal::WAL;
use crate::Result;
use crossbeam_channel::Sender;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::thread::JoinHandle;

pub struct NoTransactionDB<
    SK: MemKey + 'static,
    UK: MemKey + 'static,
    M: MemTable<SK, UK> + 'static,
    L: WAL<SK, UK> + 'static,
> {
    db_path: String,
    pub(crate) wal: Arc<Mutex<L>>,
    pub(crate) mut_mem_table: RwLock<M>,
    imm_mem_table: Arc<RwLock<M>>,

    level0_manager: Arc<Level0Manager<SK, UK, M, L>>,
    leveln_manager: Arc<LevelNManager>,

    level0_writer_handle: Option<JoinHandle<()>>,
    write_level0_channel: Option<Sender<()>>,
    background_task_write_to_level0_is_running: Arc<AtomicBool>,
}

impl<SK, UK, M, L> DB<SK, UK, M> for NoTransactionDB<SK, UK, M, L>
where
    SK: MemKey + 'static,
    UK: MemKey + From<SK>,
    M: MemTable<SK, UK> + 'static,
    L: WAL<SK, UK> + 'static,
{
    fn open(db_path: impl AsRef<Path>) -> Result<Self> {
        let db_path = db_path.as_ref().as_os_str().to_str().unwrap().to_string();

        let index_cache = Arc::new(ShardLRUCache::default());
        let leveln_manager = LevelNManager::open_tables(db_path.clone(), index_cache.clone());

        let mut mut_mem_table = M::default();

        let wal = Arc::new(Mutex::new(
            L::open_and_load_logs(&db_path, &mut mut_mem_table).unwrap(),
        ));

        let imm_mem_table = Arc::new(RwLock::new(M::default()));
        let channel = crossbeam_channel::unbounded();

        let background_task_write_to_level0_is_running = Arc::new(AtomicBool::default());
        let (level0_manager, level0_writer_handle) =
            Level0Manager::<SK, UK, M, L>::start_task_write_level0(
                db_path.clone(),
                leveln_manager.clone(),
                wal.clone(),
                imm_mem_table.clone(),
                index_cache,
                channel.1,
                background_task_write_to_level0_is_running.clone(),
            );

        Ok(NoTransactionDB {
            db_path,
            wal,
            mut_mem_table: RwLock::new(mut_mem_table),
            imm_mem_table,
            leveln_manager,
            level0_manager,
            level0_writer_handle: Some(level0_writer_handle),
            write_level0_channel: Some(channel.0),
            background_task_write_to_level0_is_running,
        })
    }

    fn get(&self, key: &SK) -> Result<Option<Value>> {
        match self.query(key)? {
            Some(v) => {
                if v.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(v))
                }
            }
            None => Ok(None),
        }
    }

    fn set(&self, key: SK, value: Value) -> Result<()> {
        let mem_table_guard = self.set_locked(key, value)?;
        if self.should_freeze(mem_table_guard.len()) {
            self.freeze(mem_table_guard);
        }
        Ok(())
    }

    fn remove(&self, key: SK) -> Result<()> {
        let mem_table_guard = self.remove_locked(key)?;
        if self.should_freeze(mem_table_guard.len()) {
            self.freeze(mem_table_guard);
        }
        Ok(())
    }

    fn range_get(&self, key_start: &SK, key_end: &SK) -> Result<SkipMap<UK, Value>> {
        let mut skip_map: SkipMap<UK, Value> = SkipMap::new();
        self.leveln_manager.range_query(
            key_start.internal_key(),
            key_end.internal_key(),
            &mut skip_map,
        );
        self.level0_manager.range_query(
            key_start.internal_key(),
            key_end.internal_key(),
            &mut skip_map,
        );
        {
            let imm_guard = self.imm_mem_table.read().unwrap();
            imm_guard.range_get(key_start, key_end, &mut skip_map);
        }
        {
            let mem_table_guard = self.mut_mem_table.read().unwrap();
            mem_table_guard.range_get(key_start, key_end, &mut skip_map);
        }
        Ok(skip_map)
    }
}

impl<SK, UK, M, L: 'static> NoTransactionDB<SK, UK, M, L>
where
    SK: MemKey + 'static,
    UK: MemKey,
    M: MemTable<SK, UK> + 'static,
    L: WAL<SK, UK>,
{
    pub(crate) fn set_locked(&self, key: SK, value: Value) -> Result<RwLockWriteGuard<M>> {
        {
            let mut wal_guard = self.wal.lock().unwrap();
            wal_guard.append(&key, Some(&value))?;
        }

        let mut mem_table_guard = self.mut_mem_table.write().unwrap();

        mem_table_guard.set(key, value)?;

        Ok(mem_table_guard)
    }

    pub(crate) fn remove_locked(&self, key: SK) -> Result<RwLockWriteGuard<M>> {
        let mut wal_writer_lock = self.wal.lock().unwrap();
        wal_writer_lock.append(&key, None)?;

        let mut mem_table_guard = self.mut_mem_table.write().unwrap();
        mem_table_guard.remove(key)?;
        Ok(mem_table_guard)
    }

    pub(crate) fn should_freeze(&self, table_size: usize) -> bool {
        table_size >= ACTIVE_SIZE_THRESHOLD
            && !self
                .background_task_write_to_level0_is_running
                .load(Ordering::Acquire)
    }

    pub(crate) fn freeze(&self, mut mem_guard: RwLockWriteGuard<M>) {
        self.background_task_write_to_level0_is_running
            .store(true, Ordering::Release);
        {
            // new log before writing to level0 sstable
            let mut wal_guard = self.wal.lock().unwrap();
            wal_guard.freeze_mut_log().unwrap();
        }

        let mut imm_guard = self
            .imm_mem_table
            .write()
            .expect("error in RwLock on imm_tables");

        let imm_table = std::mem::take(mem_guard.deref_mut());
        *imm_guard = imm_table;
        drop(mem_guard);
        if let Some(chan) = &self.write_level0_channel {
            if let Err(e) = chan.send(()) {
                warn!("{}", e);
            }
        }
    }

    fn query(&self, key: &SK) -> Result<Option<Value>> {
        // query mutable memory table
        {
            let mem_table_guard = self.mut_mem_table.read().unwrap();
            let option = mem_table_guard.get(key)?;
            if option.is_some() {
                return Ok(option);
            }
        }

        // query immutable memory table
        {
            let imm_guard = self
                .imm_mem_table
                .read()
                .expect("error in RwLock on imm_tables");
            let option = imm_guard.get(key)?;
            if option.is_some() {
                return Ok(option);
            }
        }

        // query level0 sstables
        let option = self.level0_manager.query(key.internal_key()).unwrap();
        if option.is_some() {
            return Ok(option);
        }

        // query sstables
        let option = self.leveln_manager.query(key.internal_key()).unwrap();
        Ok(option)
    }

    pub fn db_path(&self) -> &String {
        &self.db_path
    }
}

impl<SK, UK, M, L> Drop for NoTransactionDB<SK, UK, M, L>
where
    SK: MemKey + 'static,
    UK: MemKey,
    M: MemTable<SK, UK> + 'static,
    L: WAL<SK, UK> + 'static,
{
    fn drop(&mut self) {
        self.write_level0_channel.take();
        if let Some(handle) = self.level0_writer_handle.take() {
            handle.join().unwrap();
        }
        self.level0_manager.close();
        self.leveln_manager.close();
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::db::key_types::{I32UserKey, InternalKey, MemKey};
    use crate::db::no_transaction_db::NoTransactionDB;
    use crate::db::{ACTIVE_SIZE_THRESHOLD, DB, MAX_LEVEL};
    use crate::memory::{BTreeMemTable, MemTable, SkipMapMemTable};
    use crate::sstable::manager::level_n::tests::create_manager;
    use crate::wal::simple_wal::SimpleWriteAheadLog;
    use log::info;
    use rand::Rng;
    use std::collections::HashMap;
    use std::convert::TryInto;
    use std::num::NonZeroUsize;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    const TEST_CMD_TIMES: usize = 40;

    #[test]
    fn test_command() {
        let _ = env_logger::try_init();

        for j in 0..2 {
            let temp_dir = tempfile::Builder::new()
                .prefix("test_command")
                .tempdir()
                .unwrap();
            let path = temp_dir.path();
            // let path_buf = PathBuf::from(format!("test_command_{}", j));
            // let path = path_buf.as_path();
            info!("{:?}", path);
            for i in 0..2 {
                _test_command::<BTreeMemTable<InternalKey>>(path, i);
                check(path);
                _test_command::<SkipMapMemTable<InternalKey>>(path, i);
                check(path);
            }
        }
    }

    fn query(
        db1: Arc<
            NoTransactionDB<
                InternalKey,
                InternalKey,
                impl MemTable<InternalKey, InternalKey>,
                SimpleWriteAheadLog,
            >,
        >,
        value_prefix: u32,
    ) {
        let mut not_found_key = vec![];
        for i in 0..ACTIVE_SIZE_THRESHOLD * TEST_CMD_TIMES {
            let v = db1.get(&format!("key{}", i).into_bytes());
            let value = v.unwrap();
            if let Some(value) = value {
                if format!("value{}_{}", i, value_prefix).as_bytes().ne(&value) {
                    not_found_key.push(i);
                }
            } else {
                not_found_key.push(i);
            }
        }

        if !not_found_key.is_empty() {
            let mut count = 0;
            let length = not_found_key.len();
            warn!("{} keys not found", length);
            std::thread::sleep(Duration::from_secs(5));
            for key in not_found_key {
                println!("{}", key);
                let v = db1.get(&format!("key{}", key).into_bytes());
                let value = v.unwrap();
                if let Some(value) = value {
                    assert_eq!(format!("value{}_{}", key, value_prefix).into_bytes(), value);
                } else {
                    count += 1;
                }
            }
            if count > 0 {
                panic!("{} keys still not found", count);
            } else {
                info!("{} keys now found", length);
            }
        }
    }

    fn _test_command<M: 'static + MemTable<InternalKey, InternalKey>>(
        path: &Path,
        value_prefix: u32,
    ) {
        let db = NoTransactionDB::<InternalKey, InternalKey, M, SimpleWriteAheadLog>::open(path)
            .unwrap();
        db.set(
            "hello".into(),
            format!("world_{}", value_prefix).into_bytes(),
        )
        .unwrap();
        db.remove("no_exist".into()).unwrap();
        let hello = Vec::from("hello");
        assert_eq!(
            format!("world_{}", value_prefix).into_bytes(),
            db.get(&hello).unwrap().unwrap()
        );
        db.remove("hello".into()).unwrap();

        let v = db.get(&hello).unwrap();
        assert!(v.is_none(), "{:?}", v);

        for i in 0..ACTIVE_SIZE_THRESHOLD * TEST_CMD_TIMES {
            db.set(
                format!("key{}", if i < ACTIVE_SIZE_THRESHOLD { 0 } else { i }).into_bytes(),
                format!("value{}_{}", i, value_prefix).into_bytes(),
            )
            .unwrap();
        }
        for i in 0..ACTIVE_SIZE_THRESHOLD {
            db.set(
                format!("key{}", i).into_bytes(),
                format!("value{}_{}", i, value_prefix).into_bytes(),
            )
            .unwrap();
        }

        assert_eq!(
            db.get(&Vec::from("key3")).unwrap().unwrap(),
            format!("value3_{}", value_prefix).as_bytes()
        );

        info!("start query");

        let db1 = Arc::new(db);
        let db2 = db1.clone();
        let handle1 = std::thread::spawn(move || {
            query(db1, value_prefix);
        });

        let handle2 = std::thread::spawn(move || {
            query(db2, value_prefix);
        });

        handle1.join().unwrap();
        handle2.join().unwrap();

        info!("db done");
    }

    fn check(path: &Path) {
        let min = InternalKey::default();

        let db_path = path.to_str().unwrap();
        let leveln_manager = create_manager(db_path);
        for i in 1..=MAX_LEVEL {
            let lock =
                leveln_manager.get_level_tables_lock(unsafe { NonZeroUsize::new_unchecked(i) });
            let read_guard = lock.read().unwrap();
            let mut last_min_key;
            let mut last_max_key: &InternalKey = &min;
            for (_, table) in read_guard.iter() {
                let (min_key, max_key) = table.min_max_key();
                assert!(last_max_key.lt(min_key));
                assert!(min_key <= max_key);
                last_min_key = min_key;
                last_max_key = max_key;
            }
        }
        leveln_manager.close();
    }

    #[test]
    fn test_range_query() {
        let temp_dir = tempfile::Builder::new()
            .prefix("range_query")
            .tempdir()
            .unwrap();
        let path = temp_dir.path();
        let db = NoTransactionDB::<
            InternalKey,
            InternalKey,
            SkipMapMemTable<InternalKey>,
            SimpleWriteAheadLog,
        >::open(path)
        .unwrap();
        for i in 1i32..(ACTIVE_SIZE_THRESHOLD * 5) as i32 {
            db.set(Vec::from(i.to_be_bytes()), Vec::from((i + 1).to_be_bytes()))
                .unwrap();
        }

        let skip_map = db
            .range_get(
                &Vec::from(1i32.to_be_bytes()),
                &Vec::from(100i32.to_be_bytes()),
            )
            .unwrap();
        assert_eq!(100, skip_map.len());
        for node in skip_map.iter_ptr() {
            unsafe {
                assert_eq!(
                    i32::from_be_bytes((*node).entry.key.clone().try_into().unwrap()) + 1,
                    i32::from_be_bytes((*node).entry.value.clone().try_into().unwrap())
                );
            }
        }
    }

    #[test]
    fn test_read_log() {
        let temp_dir = tempfile::Builder::new()
            .prefix("read_log")
            .tempdir()
            .unwrap();
        let path = temp_dir.path();

        let db = NoTransactionDB::<
            InternalKey,
            InternalKey,
            SkipMapMemTable<InternalKey>,
            SimpleWriteAheadLog,
        >::open(path)
        .unwrap();
        for i in 0..ACTIVE_SIZE_THRESHOLD - 1 {
            db.set(
                format!("{}", i).into_bytes(),
                format!("value{}", i).into_bytes(),
            )
            .unwrap();
        }
        drop(db);

        let db = NoTransactionDB::<
            InternalKey,
            InternalKey,
            BTreeMemTable<InternalKey>,
            SimpleWriteAheadLog,
        >::open(path)
        .unwrap();

        for i in 0..ACTIVE_SIZE_THRESHOLD - 1 {
            assert_eq!(
                Some(format!("value{}", i).into_bytes()),
                db.get(&format!("{}", i).into_bytes()).unwrap()
            );
        }
        for i in ACTIVE_SIZE_THRESHOLD..ACTIVE_SIZE_THRESHOLD + 30 {
            db.set(
                format!("{}", i).into_bytes(),
                format!("value{}", i).into_bytes(),
            )
            .unwrap();
            assert_eq!(
                Some(format!("value{}", i).into_bytes()),
                db.get(&format!("{}", i).into_bytes()).unwrap()
            );
        }

        drop(db);
        let db = Arc::new(
            NoTransactionDB::<
                InternalKey,
                InternalKey,
                SkipMapMemTable<InternalKey>,
                SimpleWriteAheadLog,
            >::open(path)
            .unwrap(),
        );
        for _ in 0..4 {
            test_log(db.clone());
        }

        let thread_cnt = 4;

        for _ in 0..10 {
            let barrier = Arc::new(Barrier::new(thread_cnt));
            let mut handles = vec![];
            for _ in 0..thread_cnt {
                let db = db.clone();
                let barrier = barrier.clone();
                let handle = std::thread::spawn(move || {
                    barrier.wait();
                    test_log(db);
                });
                handles.push(handle);
            }
            for handle in handles {
                handle.join().unwrap();
            }
        }
    }

    fn test_log<M: MemTable<InternalKey, InternalKey> + 'static>(
        db: Arc<NoTransactionDB<InternalKey, InternalKey, M, SimpleWriteAheadLog>>,
    ) {
        for _ in 0..3 {
            for i in ACTIVE_SIZE_THRESHOLD..ACTIVE_SIZE_THRESHOLD + 30 {
                assert_eq!(
                    Some(format!("value{}", i).into_bytes()),
                    db.get(&format!("{}", i).into_bytes())
                        .expect("error in read thread1")
                );
            }
        }
    }

    pub(crate) fn create_random_map(size: usize) -> HashMap<i32, usize> {
        let mut map = HashMap::new();
        let rng = rand::thread_rng();
        let distribution = rand::distributions::uniform::Uniform::new(0, i32::MAX);
        for (cnt, i) in rng.sample_iter(distribution).enumerate() {
            map.insert(i, cnt);
            if cnt >= size {
                break;
            }
        }
        map
    }

    #[test]
    fn test_random() {
        let _ = env_logger::try_init();
        let temp_dir = tempfile::Builder::new()
            .prefix("random_test")
            .tempdir()
            .unwrap();
        let path = temp_dir.path();

        let db = NoTransactionDB::<
            InternalKey,
            InternalKey,
            SkipMapMemTable<InternalKey>,
            SimpleWriteAheadLog,
        >::open(path)
        .unwrap();

        let map = create_random_map(20000);
        for (k, v) in map.iter() {
            db.set(Vec::from(k.to_le_bytes()), Vec::from(v.to_le_bytes()))
                .unwrap();
        }
        info!("start query");
        let mut not_found_map = HashMap::new();
        for (i, (k, v)) in map.iter().enumerate() {
            if let Some(s) = db.get(&Vec::from(k.to_le_bytes())).unwrap() {
                assert_eq!(s, v.to_le_bytes());
            } else {
                not_found_map.insert(*k, *v);
            }
            if i % 10000 == 0 {
                info!("{}", i);
            }
        }
        if !not_found_map.is_empty() {
            warn!("{} keys not found", not_found_map.len());
            std::thread::sleep(Duration::from_secs(5));
            for (k, v) in not_found_map {
                if let Some(s) = db.get(&Vec::from(k.to_le_bytes())).unwrap() {
                    assert_eq!(s, v.to_le_bytes());
                } else {
                    panic!("{} {}", k, v);
                }
            }
        }
    }
}
