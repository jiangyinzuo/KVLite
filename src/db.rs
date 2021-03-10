use crate::command::{WriteCmdOp, WriteCommand};
use crate::config::ACTIVE_SIZE_THRESHOLD;
use crate::memory::MemTable;
use crate::sstable::SSTableWriter;
use crate::wal::WalWriter;
use crate::Result;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};

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

impl<T: MemTable> KVLite<T> {
    pub fn open(log_path: impl Into<PathBuf>) -> Result<KVLite<T>> {
        let db_impl = DBImpl::open(log_path)?;
        Ok(KVLite {
            inner: Arc::new(db_impl),
        })
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
    imm_mem_tables: Arc<RwLock<HashMap<u128, T>>>,
    wal_writer: Mutex<WalWriter>,
    sstable_writer: SSTableWriter,
}

impl<T: MemTable> DBImpl<T> {
    pub fn open(log_path: impl Into<PathBuf>) -> Result<DBImpl<T>> {
        Ok(DBImpl {
            mem_table: RwLock::default(),
            imm_mem_tables: Arc::new(RwLock::default()),
            wal_writer: Mutex::new(WalWriter::open(log_path)?),
            sstable_writer: SSTableWriter::default(),
        })
    }

    fn schedule_to_write_sstable(&self, key: u128) {
        println!("schedule {}", key);
        let imm_tables = self.imm_mem_tables.write().unwrap();
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
        // Search in memory table
        let mem_table_lock = self.mem_table.read().unwrap();
        let result = mem_table_lock.get(key)?;
        if result.is_some() {
            return Ok(result);
        }

        // Search in immutable memory tables
        let l = self
            .imm_mem_tables
            .read()
            .expect("error in RwLock on imm_tables");
        for (_key, table) in l.iter() {
            let result = table.get(key)?;
            if result.is_some() {
                return Ok(result);
            }
        }
        Ok(None)
    }
    fn set(&self, key: String, value: String) -> Result<()> {
        let cmd = WriteCommand::set(&key, &value);
        let mut wal_writer_lock = self.wal_writer.lock().unwrap();
        wal_writer_lock.append(&cmd)?;

        {
            let mut mem_table_lock = self.mem_table.write().unwrap();
            mem_table_lock.set(key, value)?;

            if mem_table_lock.len() >= ACTIVE_SIZE_THRESHOLD {
                let key = {
                    let mut lock = self
                        .imm_mem_tables
                        .write()
                        .expect("error in RwLock on imm_tables");
                    let imm_table = std::mem::take(mem_table_lock.deref_mut());
                    let key = Self::random_imm_table_key(&lock);
                    (*lock).insert(key, imm_table);
                    key
                };
                self.schedule_to_write_sstable(key);
            }
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
