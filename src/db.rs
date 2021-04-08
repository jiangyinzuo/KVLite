use crate::command::WriteCommand;
use crate::memory::MemTable;
use crate::sstable::manager::level_0::Level0Manager;
use crate::sstable::manager::level_n::LevelNManager;
use crate::wal::WriteAheadLog;
use crate::Result;
use crossbeam_channel::Sender;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::thread::JoinHandle;

pub const ACTIVE_SIZE_THRESHOLD: usize = 300;
pub const MAX_LEVEL: usize = 7;

pub trait DBCommandMut {
    fn get(&self, key: &str) -> Result<Option<String>>;
    fn set(&mut self, key: String, value: String) -> crate::Result<()>;
    fn remove(&mut self, key: String) -> crate::Result<()>;
}

pub struct KVLite<T: MemTable> {
    db_path: String,
    wal: Arc<Mutex<WriteAheadLog>>,
    mut_mem_table: RwLock<T>,
    imm_mem_table: Arc<RwLock<T>>,

    level0_manager: Arc<Level0Manager>,
    leveln_manager: Arc<LevelNManager>,

    level0_writer_handle: Option<JoinHandle<()>>,
    write_level0_channel: Option<Sender<()>>,
}

impl<T: 'static + MemTable> KVLite<T> {
    pub fn open(db_path: impl AsRef<Path>) -> Result<KVLite<T>> {
        let db_path = db_path.as_ref().as_os_str().to_str().unwrap().to_string();

        let leveln_manager = LevelNManager::open_tables(db_path.clone());

        let mut mut_mem_table = T::default();
        let mut imm_mem_table = T::default();

        let wal = Arc::new(Mutex::new(
            WriteAheadLog::open_and_load_logs(&db_path, &mut mut_mem_table, &mut imm_mem_table)
                .unwrap(),
        ));

        let imm_mem_table = Arc::new(RwLock::new(imm_mem_table));
        let channel = crossbeam_channel::unbounded();

        let (level0_manager, level0_writer_handle) = Level0Manager::start_task_write_level0(
            db_path.clone(),
            leveln_manager.clone(),
            wal.clone(),
            imm_mem_table.clone(),
            channel.1,
        );

        Ok(KVLite {
            db_path,
            wal,
            mut_mem_table: RwLock::new(mut_mem_table),
            imm_mem_table,
            leveln_manager,
            level0_manager,
            level0_writer_handle: Some(level0_writer_handle),
            write_level0_channel: Some(channel.0),
        })
    }

    fn may_freeze(&self, mut mem_table_guard: RwLockWriteGuard<T>) {
        if mem_table_guard.len() >= ACTIVE_SIZE_THRESHOLD {
            {
                // new log before writing to level0 sstable
                let mut wal_guard = self.wal.lock().unwrap();
                wal_guard.freeze_mut_log().unwrap();
            }

            let imm_table = std::mem::take(mem_table_guard.deref_mut());
            drop(mem_table_guard);
            let mut lock = self
                .imm_mem_table
                .write()
                .expect("error in RwLock on imm_tables");

            *lock = imm_table;
            if let Some(chan) = &self.write_level0_channel {
                if let Err(e) = chan.send(()) {
                    warn!("{}", e);
                }
            }
        }
    }

    fn query(&self, key: &String) -> Result<Option<String>> {
        // query mutable memory table
        let mem_table_lock = self.mut_mem_table.read().unwrap();
        let option = mem_table_lock.get(key)?;
        if option.is_some() {
            return Ok(option);
        }

        // query immutable memory table
        let imm_lock_guard = self
            .imm_mem_table
            .read()
            .expect("error in RwLock on imm_tables");

        let option = imm_lock_guard.get(key)?;
        if option.is_some() {
            return Ok(option);
        }

        // query level0 sstables
        let option = self.level0_manager.query_level0_tables(key).unwrap();
        if option.is_some() {
            return Ok(option);
        }

        // query sstables
        let option = self.leveln_manager.query_tables(key).unwrap();
        Ok(option)
    }

    pub fn db_path(&self) -> &String {
        &self.db_path
    }
}

impl<T: 'static + MemTable> KVLite<T> {
    pub fn get(&self, key: &String) -> Result<Option<String>> {
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

    pub fn set(&self, key: String, value: String) -> Result<()> {
        let cmd = WriteCommand::set(key, value);

        {
            let mut wal_guard = self.wal.lock().unwrap();
            wal_guard.append(&cmd)?;
        }

        let mut mem_table_guard = self.mut_mem_table.write().unwrap();
        if let WriteCommand::Set { key, value } = cmd {
            mem_table_guard.set(key, value)?;
        }

        self.may_freeze(mem_table_guard);

        Ok(())
    }

    pub fn remove(&self, key: String) -> Result<()> {
        let cmd = WriteCommand::remove(key);
        let mut wal_writer_lock = self.wal.lock().unwrap();
        wal_writer_lock.append(&cmd)?;

        let mut mem_table_guard = self.mut_mem_table.write().unwrap();
        if let WriteCommand::Remove { key } = cmd {
            mem_table_guard.remove(key)?;
            self.may_freeze(mem_table_guard);
        }
        Ok(())
    }
}

impl<M: MemTable> Drop for KVLite<M> {
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
mod tests {
    use crate::db::ACTIVE_SIZE_THRESHOLD;
    use crate::db::{KVLite, MAX_LEVEL};
    use crate::error::KVLiteError;
    use crate::memory::{BTreeMemTable, MemTable, SkipMapMemTable};
    use crate::sstable::manager::level_n::LevelNManager;
    use log::info;
    use rand::Rng;
    use std::collections::HashMap;
    use std::num::NonZeroUsize;
    use std::path::Path;
    use std::sync::{Arc, Barrier};
    use tempfile::TempDir;

    const TEST_CMD_TIMES: usize = 40;

    #[test]
    fn test_command() {
        let _ = env_logger::try_init();

        for _ in 0..2 {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let path = temp_dir.path();
            info!("{:?}", path);
            for i in 0..2 {
                _test_command::<BTreeMemTable>(path, i);
                check(path);
                _test_command::<SkipMapMemTable>(path, i);
                check(path);
            }
        }
    }

    fn _test_command<M: 'static + MemTable>(path: &Path, value_prefix: u32) {
        let db = KVLite::<M>::open(path).unwrap();
        db.set("hello".into(), format!("world_{}", value_prefix))
            .unwrap();
        assert_eq!(
            KVLiteError::KeyNotFound,
            db.remove("no_exist".into()).unwrap_err()
        );
        assert_eq!(
            format!("world_{}", value_prefix),
            db.get(&"hello".to_owned()).unwrap().unwrap()
        );
        db.remove("hello".into()).unwrap();

        let v = db.get(&"hello".to_owned()).unwrap();
        assert!(v.is_none(), "{:?}", v);

        for i in 0..ACTIVE_SIZE_THRESHOLD * TEST_CMD_TIMES {
            db.set(
                format!("key{}", if i < ACTIVE_SIZE_THRESHOLD { 0 } else { i }),
                format!("value{}_{}", i, value_prefix),
            )
            .unwrap();
        }
        for i in 0..ACTIVE_SIZE_THRESHOLD {
            db.set(format!("key{}", i), format!("value{}_{}", i, value_prefix))
                .unwrap();
        }

        assert_eq!(
            db.get(&"key3".to_string()).unwrap().unwrap(),
            format!("value3_{}", value_prefix)
        );

        let mut not_found_key = vec![];
        for i in 0..ACTIVE_SIZE_THRESHOLD * TEST_CMD_TIMES {
            let v = db.get(&format!("key{}", i));
            let value = v.unwrap();
            if let Some(value) = value {
                assert_eq!(format!("value{}_{}", i, value_prefix), value);
            } else {
                not_found_key.push(i);
            }
        }

        if !not_found_key.is_empty() {
            let mut count = 0;
            let length = not_found_key.len();
            warn!("{} keys not found", length);
            for key in not_found_key {
                println!("{}", key);
                let v = db.get(&format!("key{}", key));
                let value = v.unwrap();
                if let Some(value) = value {
                    assert_eq!(format!("value{}_{}", key, value_prefix), value);
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
        info!("db done");
    }

    fn check(path: &Path) {
        let db_path = path.to_str().unwrap();
        let leveln_manager = LevelNManager::open_tables(db_path.to_string());
        for i in 1..=MAX_LEVEL {
            let lock =
                leveln_manager.get_level_tables_lock(unsafe { NonZeroUsize::new_unchecked(i) });
            let read_guard = lock.read().unwrap();
            let mut last_min_key = "";
            let mut last_max_key = "";
            for (_, table) in read_guard.iter() {
                let (min_key, max_key) = table.min_max_key();
                assert!(
                    last_max_key.to_string().lt(min_key),
                    "last_max_key: {}, min_key: {}",
                    last_max_key,
                    min_key
                );
                assert!(min_key <= max_key);
                last_min_key = min_key;
                last_max_key = max_key;
            }
        }
        leveln_manager.close();
    }

    #[test]
    fn test_read_log() {
        let temp_dir = tempfile::Builder::new()
            .prefix("read_log")
            .tempdir()
            .unwrap();
        let path = temp_dir.path();

        let db = KVLite::<SkipMapMemTable>::open(path).unwrap();
        for i in 0..ACTIVE_SIZE_THRESHOLD - 1 {
            db.set(format!("{}", i), format!("value{}", i)).unwrap();
        }
        drop(db);

        let db = KVLite::<BTreeMemTable>::open(path).unwrap();

        for i in 0..ACTIVE_SIZE_THRESHOLD - 1 {
            assert_eq!(
                Some(format!("value{}", i)),
                db.get(&format!("{}", i)).unwrap()
            );
        }
        for i in ACTIVE_SIZE_THRESHOLD..ACTIVE_SIZE_THRESHOLD + 30 {
            db.set(format!("{}", i), format!("value{}", i)).unwrap();
            assert_eq!(
                Some(format!("value{}", i)),
                db.get(&format!("{}", i)).unwrap()
            );
        }

        drop(db);
        let db = Arc::new(KVLite::<SkipMapMemTable>::open(path).unwrap());
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

    fn test_log<M: MemTable + 'static>(db: Arc<KVLite<M>>) {
        for _ in 0..3 {
            for i in ACTIVE_SIZE_THRESHOLD..ACTIVE_SIZE_THRESHOLD + 30 {
                assert_eq!(
                    Some(format!("value{}", i)),
                    db.get(&format!("{}", i)).expect("error in read thread1")
                );
            }
        }
    }

    #[test]
    fn test_random() {
        let _ = env_logger::try_init();
        let temp_dir = tempfile::Builder::new()
            .prefix("random_test")
            .tempdir()
            .unwrap();
        let path = temp_dir.path();

        let db = KVLite::<SkipMapMemTable>::open(path).unwrap();
        let rng = rand::thread_rng();
        let distribution = rand::distributions::uniform::Uniform::new(0, i32::MAX);
        let mut map = HashMap::new();
        for (cnt, i) in rng.sample_iter(distribution).enumerate() {
            db.set(i.to_string(), cnt.to_string()).unwrap();
            map.insert(i, cnt);
            if cnt > 20000 {
                break;
            }
        }
        info!("start query");
        for (i, (k, v)) in map.iter().enumerate() {
            assert_eq!(db.get(&k.to_string()).unwrap().unwrap(), v.to_string());
            if i % 10000 == 0 {
                info!("{}", i);
            }
        }
    }
}
