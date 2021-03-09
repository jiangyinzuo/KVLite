use crate::error::KVLiteError::KeyNotFound;
use crate::memory::{Command, MemTable};
use crate::Result;
use std::collections::BTreeMap;

/// Wrapper of `BTreeMap<String, String>`
pub struct BTreeMemTable {
    inner: BTreeMap<String, String>,
}

impl Command for BTreeMemTable {
    fn get(&self, key: &str) -> Option<&String> {
        self.inner.get(key)
    }

    fn set(&mut self, key: String, value: String) {
        self.inner.insert(key, value);
    }

    fn remove(&mut self, key: &str) -> Result<()> {
        match self.inner.remove(key) {
            Some(_) => Ok(()),
            None => Err(KeyNotFound),
        }
    }
}

impl MemTable for BTreeMemTable {}
