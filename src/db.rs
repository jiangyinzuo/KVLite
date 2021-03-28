use crate::command::WriteCommand;
use crate::memory::MemTable;
use crate::sstable::level0_table::Level0Manager;
use crate::wal::WriteAheadLog;
use crate::Result;
use crossbeam_channel::Sender;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::thread::JoinHandle;

pub const ACTIVE_SIZE_THRESHOLD: usize = 300;
pub const MAX_LEVEL: usize = 7;

pub trait DBCommand {
    fn get(&self, key: &String) -> Result<Option<String>>;
    fn set(&self, key: String, value: String) -> crate::Result<()>;
    fn remove(&self, key: String) -> crate::Result<()>;
}

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
    level0_writer_handle: JoinHandle<()>,
    write_level0_channel: Sender<()>,
}

impl<T: 'static + MemTable> KVLite<T> {
    pub fn open(db_path: impl AsRef<Path>) -> Result<KVLite<T>> {
        let db_path = db_path.as_ref().as_os_str().to_str().unwrap().to_string();

        for i in 0..=MAX_LEVEL {
            std::fs::create_dir_all(format!("{}/{}", db_path, i)).unwrap();
        }

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
            wal.clone(),
            imm_mem_table.clone(),
            channel.1,
        );

        Ok(KVLite {
            db_path,
            wal,
            mut_mem_table: RwLock::new(mut_mem_table),
            imm_mem_table,
            level0_manager,
            level0_writer_handle,
            write_level0_channel: channel.0,
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
            self.write_level0_channel.send(()).unwrap();
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

        // query sstable
        let option = self.level0_manager.query_level0_table(key).unwrap();
        if option.is_some() {
            return Ok(option);
        }

        Ok(None)
    }
}

impl<T: 'static + MemTable> DBCommand for KVLite<T> {
    fn get(&self, key: &String) -> Result<Option<String>> {
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

    fn set(&self, key: String, value: String) -> Result<()> {
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

    fn remove(&self, key: String) -> Result<()> {
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
