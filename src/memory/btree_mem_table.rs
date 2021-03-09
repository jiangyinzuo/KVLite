use crate::command::WriteCmdOp;
use crate::db::Query;
use crate::error::KVLiteError::KeyNotFound;
use crate::memory::MemTable;
use crate::Result;
use std::collections::BTreeMap;

/// Wrapper of `BTreeMap<String, String>`
pub struct BTreeMemTable {
    inner: BTreeMap<String, String>,
}

impl WriteCmdOp for BTreeMemTable {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.inner.insert(key, value);
        Ok(())
    }

    fn remove(&mut self, key: &str) -> Result<()> {
        match self.inner.remove(key) {
            Some(_) => Ok(()),
            None => Err(KeyNotFound),
        }
    }
}

impl Default for BTreeMemTable {
    fn default() -> Self {
        BTreeMemTable {
            inner: BTreeMap::new(),
        }
    }
}

impl Query for BTreeMemTable {
    fn get(&self, key: &str) -> Result<Option<String>> {
        Ok(self.inner.get(key).cloned())
    }
}

impl MemTable for BTreeMemTable {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_> {
        Box::new(self.inner.iter())
    }
}
