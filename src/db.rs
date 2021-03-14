use crate::command::WriteCommand;
use crate::error::KVLiteError;
use crate::ioutils::BufReaderWithPos;
use crate::memory::MemTable;
use crate::sstable::SSTableManager;
use crate::wal::WalWriter;
use crate::Result;
use crossbeam_channel::{Receiver, Sender};
use serde_json::Deserializer;
use std::io::{Seek, SeekFrom};
use std::ops::DerefMut;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::{fs, thread};

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
    wal_writer: Arc<Mutex<WalWriter>>,
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

        for level in 0..=MAX_LEVEL {
            fs::create_dir_all(format!("{}/{}", db_path, level))?;
        }
        let log_path = format!("{}/log", db_path);
        fs::create_dir_all(&log_path)?;

        let mut mem_table = T::default();
        Self::load_logs(&mut mem_table, &log_path)?;

        let wal_writer = Arc::new(Mutex::new(WalWriter::open(log_path)?));
        let db_impl = DBImpl {
            mem_table: RwLock::new(mem_table),
            imm_mem_table: Arc::new(RwLock::default()),
            wal_writer: wal_writer.clone(),
            sstable_manager: SSTableManager::new(db_path, wal_writer)?,
            do_write_to_level0_sstable: crossbeam_channel::unbounded(),
        };

        Ok(db_impl)
    }

    fn load_logs(mem_table: &mut impl MemTable, log_path: &str) -> Result<()> {
        let read_dir = fs::read_dir(log_path)?;
        for f in read_dir {
            let file_path = f.unwrap().path();
            {
                let file = fs::File::open(&file_path)?;
                let mut reader = BufReaderWithPos::new(file)?;
                reader.seek(SeekFrom::Start(0))?;
                let stream = Deserializer::from_reader(reader).into_iter::<WriteCommand>();
                for cmd in stream {
                    match cmd? {
                        WriteCommand::Set { key, value } => {
                            mem_table.set(key.to_string(), value.to_string())?;
                        }
                        WriteCommand::Remove { key } => {
                            mem_table.remove(key.to_string())?;
                        }
                    }
                }
            }
            std::fs::remove_file(file_path)?;
        }
        Ok(())
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
        let cmd = WriteCommand::set(key, value);

        {
            let mut wal_guard = self.wal_writer.lock().unwrap();
            wal_guard.append(&cmd)?;
        }

        let mut mem_table_guard = self.mem_table.write().unwrap();
        if let WriteCommand::Set { key, value } = cmd {
            mem_table_guard.set(key, value)?;
        }

        if mem_table_guard.len() >= ACTIVE_SIZE_THRESHOLD {
            {
                // new log before writing to level0 sstable
                let mut wal_guard = self.wal_writer.lock().unwrap();
                wal_guard.new_log().unwrap();
            }
            let imm_table = std::mem::take(mem_table_guard.deref_mut());
            drop(mem_table_guard);
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
        let cmd = WriteCommand::remove(key);
        let mut wal_writer_lock = self.wal_writer.lock().unwrap();
        wal_writer_lock.append(&cmd)?;
        let mut mem_table_lock = self.mem_table.write().unwrap();
        if let WriteCommand::Remove { key } = cmd {
            mem_table_lock.remove(key)?;
        }
        Ok(())
    }
}
