//! Memory table

use crate::command::WriteCmdOp;
use crate::Result;
mod btree_mem_table;

pub use btree_mem_table::BTreeMemTable;

pub trait MemTable: WriteCmdOp + Default {}

pub struct Memtables<T: MemTable> {
    active_tables: T,
    imm_tables: Vec<T>,
}

impl<T: MemTable> Default for Memtables<T> {
    fn default() -> Memtables<T> {
        Memtables {
            active_tables: T::default(),
            imm_tables: Vec::new(),
        }
    }
}

impl<T: MemTable> WriteCmdOp for Memtables<T> {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.active_tables.set(key, value)?;
        Ok(())
    }

    fn remove(&mut self, key: &str) -> Result<()> {
        self.active_tables.remove(key)?;
        Ok(())
    }
}
