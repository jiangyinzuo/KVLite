use crate::cache::ShardLRUCache;
use crate::collections::skip_list::skipmap::{ReadWriteMode, SrSwSkipMap};
use crate::collections::skip_list::MemoryAllocator;
use crate::db::db_iter::DBIterator;
use crate::db::key_types::{DBKey, RawUserKey};
use crate::db::options::WriteOptions;
use crate::db::{Value, DB, WRITE_BUFFER_SIZE};
use crate::memory::{MemTable, MemTableCloneIterator, SkipMapMemTable};
use crate::sstable::manager::level_0::Level0Manager;
use crate::sstable::manager::level_n::LevelNManager;
use crate::wal::WAL;
use crate::Result;
use arc_swap::ArcSwap;
use crossbeam_channel::Sender;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

pub struct DBImpl<
    SK: DBKey + 'static,
    UK: DBKey + 'static,
    M: MemTable<SK, UK> + 'static,
    L: WAL<SK, UK> + 'static,
> {
    db_path: String,
    pub(crate) wal: Arc<Mutex<L>>,
    pub(crate) mut_mem_table: ArcSwap<M>,
    imm_mem_table: Arc<ArcSwap<M>>,

    level0_manager: Arc<Level0Manager<SK, UK, M, L>>,
    leveln_manager: Arc<LevelNManager>,

    level0_writer_handle: Option<JoinHandle<()>>,
    write_level0_channel: Option<Sender<()>>,
    background_task_write_to_level0_is_running: Arc<AtomicBool>,
}

impl<SK, UK, M, L> DB<SK, UK, M> for DBImpl<SK, UK, M, L>
where
    SK: DBKey + 'static,
    UK: DBKey + From<SK>,
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

        let imm_mem_table = Arc::new(ArcSwap::new(Arc::new(M::default())));
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

        Ok(DBImpl {
            db_path,
            wal,
            mut_mem_table: ArcSwap::new(Arc::new(mut_mem_table)),
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

    fn set(&self, write_options: &WriteOptions, key: SK, value: Value) -> Result<()> {
        {
            let mut wal_guard = self.wal.lock().unwrap();
            wal_guard.append(write_options, &key, Some(&value))?;
        }

        let mut_mem_table = self.get_mut_mem_table();
        mut_mem_table.set(key, value)?;
        if self.should_freeze(mut_mem_table.approximate_memory_usage()) {
            self.freeze();
        }
        Ok(())
    }

    fn remove(&self, write_options: &WriteOptions, key: SK) -> Result<()> {
        {
            let mut wal_guard = self.wal.lock().unwrap();
            wal_guard.append(write_options, &key, None)?;
        }

        let mut_mem_table = self.get_mut_mem_table();
        mut_mem_table.remove(key)?;

        if self.should_freeze(mut_mem_table.approximate_memory_usage()) {
            self.freeze();
        }
        Ok(())
    }

    fn range_get(&self, key_start: &SK, key_end: &SK) -> Result<SrSwSkipMap<UK, Value>> {
        let mut skip_map = SrSwSkipMap::new();
        self.leveln_manager.range_query(
            key_start.raw_user_key(),
            key_end.raw_user_key(),
            &mut skip_map,
        );
        self.level0_manager.range_query(
            key_start.raw_user_key(),
            key_end.raw_user_key(),
            &mut skip_map,
        );

        let imm_mem_table = self.get_imm_mem_table();
        imm_mem_table.range_get(key_start, key_end, &mut skip_map);

        let mut_mem_table = self.get_mut_mem_table();
        mut_mem_table.range_get(key_start, key_end, &mut skip_map);
        Ok(skip_map)
    }

    fn db_path(&self) -> &String {
        &self.db_path
    }
}

impl<SK, UK, M, L: 'static> DBImpl<SK, UK, M, L>
where
    SK: DBKey + 'static,
    UK: DBKey,
    M: MemTable<SK, UK> + 'static,
    L: WAL<SK, UK>,
{
    pub(crate) fn should_freeze(&self, table_size: u64) -> bool {
        table_size >= WRITE_BUFFER_SIZE
            && !self
                .background_task_write_to_level0_is_running
                .load(Ordering::Acquire)
    }

    pub(crate) fn freeze(&self) {
        self.background_task_write_to_level0_is_running
            .store(true, Ordering::Release);
        {
            // new log before writing to level0 sstable
            let mut wal_guard = self.wal.lock().unwrap();
            wal_guard.freeze_mut_log().unwrap();
        }

        let imm = self.mut_mem_table.swap(Arc::new(M::default()));
        self.imm_mem_table.store(imm);

        if let Some(chan) = &self.write_level0_channel {
            if let Err(e) = chan.send(()) {
                warn!("{}", e);
            }
        }
    }

    pub(crate) fn get_mut_mem_table(&self) -> Arc<M> {
        let guard = self.mut_mem_table.load();
        guard.clone()
    }

    pub(crate) fn get_imm_mem_table(&self) -> Arc<M> {
        let guard = self.imm_mem_table.load();
        guard.clone()
    }

    fn query(&self, key: &SK) -> Result<Option<Value>> {
        // query mutable memory table
        {
            let mut_mem = self.get_mut_mem_table();
            let option = mut_mem.get(key)?;
            if option.is_some() {
                return Ok(option);
            }
        }

        // query immutable memory table
        {
            let imm_mem = self.get_imm_mem_table();
            let option = imm_mem.get(key)?;
            if option.is_some() {
                return Ok(option);
            }
        }

        // query level0 sstables
        let option = self.level0_manager.query(key.raw_user_key()).unwrap();
        if option.is_some() {
            return Ok(option);
        }

        // query sstables
        let option = self.leveln_manager.query(key.raw_user_key()).unwrap();
        Ok(option)
    }

    /// Get an iterator for all the valid key-value pairs in databases.
    pub fn get_db_iterator<const RW_MODE: ReadWriteMode>(&self) -> DBIterator
    where
        M: SkipMapMemTable<RawUserKey, RawUserKey, { RW_MODE }>,
    {
        let imm_mem = self.get_imm_mem_table();
        let imm_mem_iterator = MemTableCloneIterator::new(imm_mem);

        let mut_mem = self.get_mut_mem_table();
        let mut_mem_iterator = MemTableCloneIterator::new(mut_mem);

        let level0_iterator = self.level0_manager.get_level0_iterator();
        let leveln_iterators = self.leveln_manager.get_iterators();
        DBIterator::new(
            imm_mem_iterator,
            mut_mem_iterator,
            level0_iterator,
            leveln_iterators,
        )
    }
}

impl<SK, UK, M, L> Drop for DBImpl<SK, UK, M, L>
where
    SK: DBKey + 'static,
    UK: DBKey,
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
    use crate::collections::skip_list::skipmap::ReadWriteMode::{MrMw, MrSw, SrSw};
    use crate::db::dbimpl::DBImpl;
    use crate::db::key_types::RawUserKey;
    use crate::db::options::WriteOptions;
    use crate::db::{DB, MAX_LEVEL};
    use crate::memory::{
        BTreeMemTable, MemTable, MrMwSkipMapMemTable, MrSwSkipMapMemTable, MutexSkipMapMemTable,
    };
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

    const NUM_KEYS: u64 = 100000;

    #[test]
    fn test_command() {
        let _ = env_logger::try_init();

        for _ in 0..2 {
            let temp_dir = tempfile::Builder::new()
                .prefix("test_command")
                .tempdir()
                .unwrap();
            let path = temp_dir.path();
            // let path_buf = PathBuf::from(format!("test_command_{}", j));
            // let path = path_buf.as_path();
            info!("{:?}", path);
            _test_command::<MrMwSkipMapMemTable<RawUserKey>>(path, 5);
            check(path);
            for i in 0..2 {
                _test_command::<BTreeMemTable<RawUserKey>>(path, i);
                check(path);
                _test_command::<MutexSkipMapMemTable<RawUserKey>>(path, i);
                check(path);
                _test_command::<MrSwSkipMapMemTable<RawUserKey>>(path, i);
                check(path);
            }
            _test_command::<MrMwSkipMapMemTable<RawUserKey>>(path, 5);
            check(path);
        }
    }

    fn query(
        db1: Arc<
            DBImpl<
                RawUserKey,
                RawUserKey,
                impl MemTable<RawUserKey, RawUserKey>,
                SimpleWriteAheadLog,
            >,
        >,
        value_prefix: u32,
    ) {
        let mut not_found_key = vec![];
        for i in 0..NUM_KEYS {
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

    fn _test_command<M: 'static + MemTable<RawUserKey, RawUserKey>>(
        path: &Path,
        value_prefix: u32,
    ) {
        let wo = WriteOptions { sync: false };
        let db = DBImpl::<RawUserKey, RawUserKey, M, SimpleWriteAheadLog>::open(path).unwrap();
        db.set(
            &wo,
            "hello".into(),
            format!("world_{}", value_prefix).into_bytes(),
        )
        .unwrap();
        db.remove(&wo, "no_exist".into()).unwrap();
        let hello = Vec::from("hello");
        assert_eq!(
            format!("world_{}", value_prefix).into_bytes(),
            db.get(&hello).unwrap().unwrap()
        );
        db.remove(&wo, "hello".into()).unwrap();

        let v = db.get(&hello).unwrap();
        assert!(v.is_none(), "{:?}", v);

        let num_keys_zero = NUM_KEYS / 10;
        for i in 0..NUM_KEYS {
            db.set(
                &wo,
                format!("key{}", if i < num_keys_zero { 0 } else { i }).into_bytes(),
                format!("value{}_{}", i, value_prefix).into_bytes(),
            )
            .unwrap();
        }
        for i in 0..num_keys_zero {
            db.set(
                &wo,
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
        let min = RawUserKey::default();

        let db_path = path.to_str().unwrap();
        let leveln_manager = create_manager(db_path);
        for i in 1..=MAX_LEVEL {
            let lock =
                leveln_manager.get_level_tables_lock(unsafe { NonZeroUsize::new_unchecked(i) });
            let read_guard = lock.read().unwrap();
            let mut last_min_key;
            let mut last_max_key: &RawUserKey = &min;
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
        let wo = WriteOptions { sync: false };
        let temp_dir = tempfile::Builder::new()
            .prefix("range_query")
            .tempdir()
            .unwrap();
        let path = temp_dir.path();
        let db = DBImpl::<
            RawUserKey,
            RawUserKey,
            MrSwSkipMapMemTable<RawUserKey>,
            SimpleWriteAheadLog,
        >::open(path)
        .unwrap();
        for i in 1i32..NUM_KEYS as i32 {
            db.set(
                &wo,
                Vec::from(i.to_be_bytes()),
                Vec::from((i + 1).to_be_bytes()),
            )
            .unwrap();
        }

        for start in [32, 2203, 1234, 123, 1234, 121, 9982] {
            let skip_map = db
                .range_get(
                    &Vec::from((start + 1i32).to_be_bytes()),
                    &Vec::from((start + 100i32).to_be_bytes()),
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
    }

    #[test]
    fn test_read_log() {
        let temp_dir = tempfile::Builder::new()
            .prefix("read_log")
            .tempdir()
            .unwrap();
        let path = temp_dir.path();

        let db = DBImpl::<
            RawUserKey,
            RawUserKey,
            MrSwSkipMapMemTable<RawUserKey>,
            SimpleWriteAheadLog,
        >::open(path)
        .unwrap();
        let wo = WriteOptions { sync: false };
        for i in 0..NUM_KEYS - 1 {
            db.set(
                &wo,
                format!("{}", i).into_bytes(),
                format!("value{}", i).into_bytes(),
            )
            .unwrap();
        }
        drop(db);

        let db =
            DBImpl::<RawUserKey, RawUserKey, BTreeMemTable<RawUserKey>, SimpleWriteAheadLog>::open(
                path,
            )
            .unwrap();

        for i in 0..NUM_KEYS - 1 {
            assert_eq!(
                Some(format!("value{}", i).into_bytes()),
                db.get(&format!("{}", i).into_bytes()).unwrap()
            );
        }
        for i in NUM_KEYS..NUM_KEYS + 30 {
            db.set(
                &wo,
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
        let db =
            Arc::new(
                DBImpl::<
                    RawUserKey,
                    RawUserKey,
                    MrMwSkipMapMemTable<RawUserKey>,
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

    fn test_log<M: MemTable<RawUserKey, RawUserKey> + 'static>(
        db: Arc<DBImpl<RawUserKey, RawUserKey, M, SimpleWriteAheadLog>>,
    ) {
        for _ in 0..3 {
            for i in NUM_KEYS..NUM_KEYS + 30 {
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
        let write_option = WriteOptions { sync: false };
        let db = DBImpl::<
            RawUserKey,
            RawUserKey,
            MrMwSkipMapMemTable<RawUserKey>,
            SimpleWriteAheadLog,
        >::open(path)
        .unwrap();

        let map = create_random_map(20000);
        for (k, v) in map.iter() {
            db.set(
                &write_option,
                Vec::from(k.to_le_bytes()),
                Vec::from(v.to_le_bytes()),
            )
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

    fn setup_iterate1(
        db: &DBImpl<RawUserKey, RawUserKey, MrMwSkipMapMemTable<RawUserKey>, SimpleWriteAheadLog>,
        write_option: &WriteOptions,
    ) -> usize {
        for _ in 0..3 {
            for i in 0..10000u128 {
                db.set(
                    write_option,
                    Vec::from(i.to_be_bytes()),
                    Vec::from((i + 1).to_be_bytes()),
                )
                .unwrap();
            }
        }
        10000
    }

    fn setup_iterate2(
        db: &DBImpl<RawUserKey, RawUserKey, MrMwSkipMapMemTable<RawUserKey>, SimpleWriteAheadLog>,
        write_option: &WriteOptions,
    ) -> usize {
        for i in 0..1000000u128 {
            db.set(
                write_option,
                Vec::from(i.to_be_bytes()),
                Vec::from((i + 1).to_be_bytes()),
            )
            .unwrap();
        }
        1000000
    }

    #[test]
    fn test_iterate() {
        for f in [setup_iterate1, setup_iterate2] {
            let temp_dir = tempfile::Builder::new()
                .prefix("iterate")
                .tempdir()
                .unwrap();
            let path = temp_dir.path();
            let write_option = WriteOptions { sync: false };
            let db = DBImpl::<
                RawUserKey,
                RawUserKey,
                MrMwSkipMapMemTable<RawUserKey>,
                SimpleWriteAheadLog,
            >::open(path)
            .unwrap();

            let expected_count = f(&db, &write_option);
            let iterator = db.get_db_iterator();
            let mut count = 0;
            for _ in iterator {
                count += 1;
            }
            assert_eq!(expected_count, count);

            let iterator = db.get_db_iterator();
            for (i, (k, v)) in iterator.enumerate() {
                let i = i as u128;
                assert_eq!(Vec::from(i.to_be_bytes()), k);
                assert_eq!(Vec::from((i + 1).to_be_bytes()), v);
            }
        }
    }
}
