use crate::collections::skiplist::MultiSkipMap;
use crate::db::DBCommandMut;
use crate::error::KVLiteError::KeyNotFound;
use crate::memory::MemTable;
use crate::Result;

#[derive(Default)]
pub struct SkipMapMemTable {
    inner: MultiSkipMap<String, String>,
}

impl DBCommandMut for SkipMapMemTable {
    fn get(&self, key: &str) -> Result<Option<String>> {
        let node = self.inner.find_first_ge(&key.to_string(), None);
        if node.is_null() {
            Ok(None)
        } else {
            let node = unsafe { node.as_mut().unwrap() };
            let k = &node.entry.key;
            if k.eq(key) {
                Ok(Some(node.entry.value.clone()))
            } else {
                Ok(None)
            }
        }
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.inner.insert(key, value);
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.inner.insert(key, String::new()) {
            Ok(())
        } else {
            Err(KeyNotFound)
        }
    }
}

impl MemTable for SkipMapMemTable {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_> {
        Box::new(
            self.inner
                .iter()
                .map(|n| unsafe { (&(*n).entry.key, &(*n).entry.value) }),
        )
    }

    fn first_key(&self) -> Option<&String> {
        self.inner.first_key_value().map(|entry| &entry.key)
    }

    fn last_key(&self) -> Option<&String> {
        self.inner.last_key_value().map(|entry| &entry.key)
    }
}
