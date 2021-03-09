//! Memory table

mod btree_mem_table;

use crate::command::WriteCmdOp;
use crate::config::ACTIVE_SIZE_THRESHOLD;
use crate::db::Query;
use crate::Result;

pub use btree_mem_table::BTreeMemTable;

/// Table in Memory
pub trait MemTable: WriteCmdOp + Query + Default {
    fn len(&self) -> usize;

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_>;
}

pub struct MemTables<T: MemTable> {
    active_table: T,
    imm_tables: Vec<T>,
}

impl<T: MemTable> Default for MemTables<T> {
    fn default() -> MemTables<T> {
        MemTables {
            active_table: T::default(),
            imm_tables: Vec::new(),
        }
    }
}

impl<T: MemTable> WriteCmdOp for MemTables<T> {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.active_table.set(key, value)?;
        if self.active_table.len() >= ACTIVE_SIZE_THRESHOLD {
            let imm_table = std::mem::take(&mut self.active_table);
            self.imm_tables.push(imm_table);
        }
        Ok(())
    }

    fn remove(&mut self, key: &str) -> Result<()> {
        self.active_table.remove(key)?;
        Ok(())
    }
}

impl<T: MemTable> Query for MemTables<T> {
    fn get(&self, key: &str) -> Result<Option<String>> {
        let result = self.active_table.get(key)?;
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
