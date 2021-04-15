use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::DBCommandMut;
use crate::memory::{KeyValue, MemTable};
use crate::Result;
use std::sync::RwLock;

#[derive(Default)]
pub struct SkipMapMemTable {
    rw_lock: RwLock<()>,
    inner: SkipMap<String, String>,
}

impl DBCommandMut for SkipMapMemTable {
    fn get(&self, key: &str) -> Result<Option<String>> {
        let _guard = self.rw_lock.read().unwrap();
        let node = self.inner.find_first_ge(&key.to_string(), None);
        if node.is_null() {
            Ok(None)
        } else {
            let node = unsafe { node.as_mut().unwrap() };
            let k = &node.entry.key;
            if k.eq(key) && !node.entry.value.is_empty() {
                Ok(Some(node.entry.value.clone()))
            } else {
                Ok(None)
            }
        }
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let _guard = self.rw_lock.write().unwrap();
        self.inner.insert(key, value);
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let _guard = self.rw_lock.write().unwrap();
        self.inner.insert(key, String::new());
        Ok(())
    }
}

impl KeyValue for SkipMapMemTable {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_> {
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

impl MemTable for SkipMapMemTable {}

#[cfg(test)]
mod tests {
    use crate::db::DBCommandMut;
    use crate::memory::SkipMapMemTable;

    #[test]
    fn test_insert() {
        let mut table = SkipMapMemTable::default();
        for i in 0..10 {
            table.set("1".into(), i.to_string()).unwrap();
        }

        assert_eq!("9", table.get("1").unwrap().unwrap());
        table.remove("1".to_string()).unwrap();
        assert!(table.get("1").unwrap().is_none());
    }
}
