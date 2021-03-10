use crate::command::WriteCmdOp;
use crate::db::Query;
use crate::error::KVLiteError::KeyNotFound;
use crate::memory::MemTable;
use crate::Result;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::RwLock;

/// Wrapper of `BTreeMap<String, String>`
pub struct BTreeMemTable {
    rw_lock: RwLock<()>,
    inner: BTreeMap<String, String>,
}

impl WriteCmdOp for BTreeMemTable {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let _lock = self.rw_lock.read().unwrap();
        self.inner.insert(key, value);
        Ok(())
    }

    fn remove(&mut self, key: &str) -> Result<()> {
        let _lock = self.rw_lock.write().unwrap();
        match self.inner.remove(key) {
            Some(_) => Ok(()),
            None => Err(KeyNotFound),
        }
    }
}

impl Default for BTreeMemTable {
    fn default() -> Self {
        BTreeMemTable {
            rw_lock: RwLock::default(),
            inner: BTreeMap::default(),
        }
    }
}

impl Query for BTreeMemTable {
    fn get(&self, key: &str) -> Result<Option<String>> {
        let _lock = self.rw_lock.read().unwrap();
        Ok(self.inner.get(key).cloned())
    }
}

impl PartialEq for BTreeMemTable {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl Eq for BTreeMemTable {}

impl Hash for BTreeMemTable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl MemTable for BTreeMemTable {
    fn len(&self) -> usize {
        let _lock = self.rw_lock.read().unwrap();
        self.inner.len()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_> {
        let _lock = self.rw_lock.read().unwrap();
        Box::new(self.inner.iter())
    }
}
