use crate::command::WriteCommand;
use crate::error::KVLiteError;
use crate::memory::MemTable;
use crate::sstable::SSTableManager;
use crate::wal::WalWriter;
use crate::Result;
use crossbeam_channel::{Receiver, Sender};
use std::ops::DerefMut;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

pub const ACTIVE_SIZE_THRESHOLD: usize = 300;

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
    inner: Arc<DBImpl<T>>,
}

impl<T: MemTable + 'static> KVLite<T> {
    pub fn open(db_path: impl AsRef<Path>) -> Result<KVLite<T>> {
        let db_impl = Arc::new(DBImpl::open(db_path)?);

        let kv_lite = KVLite { inner: db_impl };
        kv_lite.task_write_to_level0_sstable();
        Ok(kv_lite)
    }

    /// Create a thread to write immutable memory table to level0 sstable.
    fn task_write_to_level0_sstable(&self) {
        let db = self.inner.clone();
        thread::Builder::new()
            .name("write_to_level0_sstable".to_owned())
            .spawn(move || {
                let thread_name = "write_to_level0_sstable";
                info!("start thread `{}`", thread_name);
                while let Ok(()) = db.do_write_to_level0_sstable.1.recv() {
                    debug!("thread `{}`: start writing", thread_name);
                    let imm_guard = db.imm_mem_table.read().unwrap();

                    let mut iter = imm_guard.iter();
                    db.sstable_manager
                        .write_level0_sstable(&mut iter, imm_guard.len())
                        .unwrap();

                    debug!("thread `{}`: write done", thread_name);
                }
                debug!("thread `{}`: shutdown", thread_name);
            })
            .unwrap();
    }
}

impl<T: MemTable> DBCommand for KVLite<T> {
    fn get(&self, key: &String) -> Result<Option<String>> {
        match self.inner.get(key) {
            Ok(option) => match option {
                Some(s) if s.is_empty() => Ok(None),
                o => Ok(o),
            },
            Err(e) => Err(e),
        }
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        self.inner.set(key, value)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.inner.remove(key)
    }
}

pub struct DBImpl<T: MemTable> {
    mem_table: RwLock<T>,
    imm_mem_table: Arc<RwLock<T>>,
    wal_writer: Mutex<WalWriter>,
    sstable_manager: SSTableManager,

    /// channels
    do_write_to_level0_sstable: (Sender<()>, Receiver<()>),
}

impl<T: MemTable> DBImpl<T> {
    pub fn open(db_path: impl AsRef<Path>) -> Result<DBImpl<T>> {
        let db_path = match db_path.as_ref().to_owned().into_os_string().into_string() {
            Ok(s) => s,
            Err(_) => {
                return Err(KVLiteError::Custom(
                    "Invalid db path. Expect to use Unicode db path.".to_owned(),
                ))
            }
        };

        for level in 0..=4 {
            std::fs::create_dir_all(format!("{}/{}", db_path, level))?;
        }

        Ok(DBImpl {
            mem_table: RwLock::default(),
            imm_mem_table: Arc::new(RwLock::default()),
            wal_writer: Mutex::new(WalWriter::open(db_path.clone())?),
            sstable_manager: SSTableManager::new(db_path)?,
            do_write_to_level0_sstable: crossbeam_channel::unbounded(),
        })
    }
}

impl<T: MemTable> DBCommand for DBImpl<T> {
    fn get(&self, key: &String) -> Result<Option<String>> {
        // query memory table
        let mem_table_lock = self.mem_table.read().unwrap();
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
        let option = self.sstable_manager.get(key)?;
        if option.is_some() {
            return Ok(option);
        }

        Ok(None)
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        let cmd = WriteCommand::set(&key, &value);
        let mut wal_writer_lock = self.wal_writer.lock().unwrap();
        wal_writer_lock.append(&cmd)?;

        let mut mem_table_lock = self.mem_table.write().unwrap();
        mem_table_lock.set(key, value)?;

        if mem_table_lock.len() >= ACTIVE_SIZE_THRESHOLD {
            let imm_table = std::mem::take(mem_table_lock.deref_mut());
            drop(mem_table_lock);
            let mut lock = self
                .imm_mem_table
                .write()
                .expect("error in RwLock on imm_tables");

            *lock = imm_table;
            self.do_write_to_level0_sstable.0.send(())?;
        }

        Ok(())
    }

    fn remove(&self, key: String) -> Result<()> {
        let cmd = WriteCommand::remove(&key);
        let mut wal_writer_lock = self.wal_writer.lock().unwrap();
        wal_writer_lock.append(&cmd)?;
        let mut mem_table_lock = self.mem_table.write().unwrap();
        mem_table_lock.remove(key)?;
        Ok(())
    }
}
