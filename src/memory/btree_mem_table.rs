use crate::db::DBCommandMut;
use crate::error::KVLiteError::KeyNotFound;
use crate::memory::{KeyValue, MemTable};
use crate::Result;
use std::collections::BTreeMap;
use std::sync::RwLock;

/// Wrapper of `BTreeMap<String, String>`
pub struct BTreeMemTable {
    rw_lock: RwLock<()>,
    inner: BTreeMap<String, String>,
}

impl DBCommandMut for BTreeMemTable {
    fn get(&self, key: &str) -> Result<Option<String>> {
        let _lock = self.rw_lock.read().unwrap();
        Ok(self.inner.get(key).cloned())
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let _lock = self.rw_lock.read().unwrap();
        self.inner.insert(key, value);
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let _lock = self.rw_lock.write().unwrap();
        match self.inner.insert(key, String::new()) {
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

impl KeyValue for BTreeMemTable {
    fn len(&self) -> usize {
        let _lock = self.rw_lock.read().unwrap();
        self.inner.len()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_> {
        let _lock = self.rw_lock.read().unwrap();
        Box::new(self.inner.iter())
    }

    fn first_key(&self) -> Option<&String> {
        let _lock = self.rw_lock.read().unwrap();
        match self.inner.first_key_value() {
            Some((k, v)) => Some(k),
            None => None,
        }
    }

    fn last_key(&self) -> Option<&String> {
        let _lock = self.rw_lock.read().unwrap();
        match self.inner.last_key_value() {
            Some((k, v)) => Some(k),
            None => None,
        }
    }
}

impl MemTable for BTreeMemTable {}

#[cfg(test)]
mod tests {
    use crate::db::DBCommandMut;
    use crate::memory::{BTreeMemTable, KeyValue};
    use crate::Result;

    #[test]
    fn test_iter() -> Result<()> {
        let mut mem_table = BTreeMemTable::default();
        for i in 0..100 {
            mem_table.set(format!("a{}", i), i.to_string())?;
        }

        for (key, value) in mem_table.iter() {
            assert_eq!(key, &format!("a{}", value));
        }
        Ok(())
    }
}
