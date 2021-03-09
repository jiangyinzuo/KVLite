//! Memory table

mod btree_mem_table;

use crate::command::WriteCmdOp;
use crate::db::Query;
use crate::Result;

pub use btree_mem_table::BTreeMemTable;

/// Table in Memory
pub trait MemTable: WriteCmdOp + Query + Default {}

pub struct MemTables<T: MemTable> {
    active_tables: T,
    imm_tables: Vec<T>,
}

impl<T: MemTable> Default for MemTables<T> {
    fn default() -> MemTables<T> {
        MemTables {
            active_tables: T::default(),
            imm_tables: Vec::new(),
        }
    }
}

impl<T: MemTable> WriteCmdOp for MemTables<T> {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.active_tables.set(key, value)?;
        Ok(())
    }

    fn remove(&mut self, key: &str) -> Result<()> {
        self.active_tables.remove(key)?;
        Ok(())
    }
}

impl<T: MemTable> Query for MemTables<T> {
    fn get(&self, key: &str) -> Result<Option<String>> {
        let result = self.active_tables.get(key)?;
        if result.is_some() {
            return Ok(result);
        }
        for table in &self.imm_tables {
            let result = table.get(key)?;
            if result.is_some() {
                return Ok(result);
            }
        }
        Ok(None)
    }
}
