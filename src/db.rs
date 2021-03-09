use crate::command::{WriteCmdOp, WriteCommand};
use crate::memory::{MemTable, Memtables};
use crate::wal::WalWriter;
use crate::Result;
use std::path::PathBuf;

pub struct KVLite<T: MemTable> {
    mem_tables: Memtables<T>,
    wal_writer: WalWriter,
}

impl<T: MemTable> KVLite<T> {
    pub fn new(log_path: impl Into<PathBuf>) -> Result<KVLite<T>> {
        Ok(KVLite {
            mem_tables: Memtables::default(),
            wal_writer: WalWriter::open(log_path)?,
        })
    }
}

impl<T: MemTable> WriteCmdOp for KVLite<T> {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = WriteCommand::set(&key, &value);
        self.wal_writer.append(&cmd)?;
        self.mem_tables.set(key, value)?;
        Ok(())
    }

    fn remove(&mut self, key: &str) -> Result<()> {
        let cmd = WriteCommand::remove(key);
        self.wal_writer.append(&cmd)?;
        self.mem_tables.remove(key)?;
        Ok(())
    }
}
