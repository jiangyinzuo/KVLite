use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::{DBCommandMut, Key, Value};
use crate::memory::{KeyValue, MemTable};
use crate::Result;
use std::sync::RwLock;

#[derive(Default)]
pub struct SkipMapMemTable {
    rw_lock: RwLock<()>,
    inner: SkipMap<Key, Value>,
}

impl DBCommandMut for SkipMapMemTable {
    fn get(&self, key: &Key) -> Result<Option<Value>> {
        let _guard = self.rw_lock.read().unwrap();
        let node = self.inner.find_first_ge(key, None);
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

    fn set(&mut self, key: Key, value: Value) -> Result<()> {
        let _guard = self.rw_lock.write().unwrap();
        self.inner.insert(key, value);
        Ok(())
    }

    fn remove(&mut self, key: Key) -> Result<()> {
        let _guard = self.rw_lock.write().unwrap();
        self.inner.insert(key, Key::new());
        Ok(())
    }
}

impl KeyValue for SkipMapMemTable {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&Key, &Key)> + '_> {
        Box::new(
            self.inner
                .iter()
                .map(|n| unsafe { (&(*n).entry.key, &(*n).entry.value) }),
        )
    }

    fn first_key(&self) -> Option<&Key> {
        self.inner.first_key_value().map(|entry| &entry.key)
    }

    fn last_key(&self) -> Option<&Key> {
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

        let one = Vec::from(1i32.to_le_bytes());
        for i in 0..10i32 {
            table.set(one.clone(), Vec::from(i.to_le_bytes())).unwrap();
        }

        assert_eq!(
            Vec::from(9i32.to_le_bytes()),
            table.get(&one).unwrap().unwrap()
        );
        table.remove(one.clone()).unwrap();
        assert!(table.get(&one).unwrap().is_none());
    }
}
