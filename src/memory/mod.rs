//! Memory table

mod btree_mem_table;

use crate::Result;
use crate::config::NUM_ACTIVE_TABLE;

pub trait Command {
    fn get(&self, key: &str) -> Option<&String>;

    fn set(&mut self, key: String, value: String);

    fn remove(&mut self, key: &str) -> Result<()>;
}

pub trait MemTable: Command {}


pub struct Memtables<T: MemTable> {
    active_tables: [T; NUM_ACTIVE_TABLE],
    imm_tables: Vec<T>,
}

impl<T: MemTable> Command for Memtables<T> {
    fn get(&self, key: &str) -> Option<&String> {
        unimplemented!()
    }

    fn set(&mut self, key: String, value: String) {
        unimplemented!()
    }

    fn remove(&mut self, key: &str) -> Result<()> {
        unimplemented!()
    }
}
