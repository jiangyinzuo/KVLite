use crate::command::WriteCommand;
use crate::config::ACTIVE_SIZE_THRESHOLD;
use crate::error::KVLiteError;
use crate::memory::MemTable;
use crate::sstable::SSTableWriter;
use crate::version::versions::Versions;
use crate::wal::WalWriter;
use crate::Result;
use crossbeam_channel::{Receiver, Sender};
use std::collections::HashMap;
use std::ops::DerefMut;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::thread;

pub trait Query {
    fn get(&self, key: &str) -> Result<Option<String>>;
}

pub trait DB {
    fn get(&self, key: &str) -> Result<Option<String>>;
    fn set(&self, key: String, value: String) -> crate::Result<()>;
    fn remove(&self, key: &str) -> crate::Result<()>;
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
                    let mut versions_guard = db.versions.lock().unwrap();
                    versions_guard.write_level0_sstable(&mut iter).unwrap();

                    debug!("thread `{}`: done", thread_name);
                }
            })
            .unwrap();
    }

    /// Write immutable memory table to level0 sstable
    fn write_to_level0_sstable(&self) -> Result<()> {
        Ok(())
    }
}

impl<T: MemTable> DB for KVLite<T> {
    fn get(&self, key: &str) -> Result<Option<String>> {
        self.inner.get(key)
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        self.inner.set(key, value)
    }

    fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key)
    }
}

pub struct DBImpl<T: MemTable> {
    mem_table: RwLock<T>,
    imm_mem_table: Arc<RwLock<T>>,
    wal_writer: Mutex<WalWriter>,
    sstable_writer: SSTableWriter,
    versions: Mutex<Versions>,

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
            sstable_writer: SSTableWriter::default(),
            versions: Mutex::new(Versions::new(db_path)),
            do_write_to_level0_sstable: crossbeam_channel::unbounded(),
        })
    }

    fn schedule_to_write_sstable(&self) {
        let imm_tables = self.imm_mem_table.write().unwrap();
    }

    fn random_imm_table_key(imm_tables: &RwLockWriteGuard<HashMap<u128, T>>) -> u128 {
        for _ in 0..10_0000_0000u128 {
            let key = rand::random();
            if !imm_tables.contains_key(&key) {
                return key;
            }
        }
        panic!("no suitable key")
    }
}

impl<T: MemTable> DB for DBImpl<T> {
    fn get(&self, key: &str) -> Result<Option<String>> {
        // query memory table
        let mem_table_lock = self.mem_table.read().unwrap();
        let result = mem_table_lock.get(key)?;
        if result.is_some() {
            return Ok(result);
        }

        // query immutable memory table
        let imm_lock_guard = self
            .imm_mem_table
            .read()
            .expect("error in RwLock on imm_tables");

        let result = imm_lock_guard.get(key)?;
        if result.is_some() {
            return Ok(result);
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

    fn remove(&self, key: &str) -> Result<()> {
        let cmd = WriteCommand::remove(key);
        let mut wal_writer_lock = self.wal_writer.lock().unwrap();
        wal_writer_lock.append(&cmd)?;
        let mut mem_table_lock = self.mem_table.write().unwrap();
        mem_table_lock.remove(key)?;
        Ok(())
    }
}
