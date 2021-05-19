use std::sync::RwLock;

use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::key_types::{MemKey, UserKey};
use crate::db::{DBCommand, Value};
use crate::memory::{KeyValue, MemTable};
use crate::Result;

#[derive(Default)]
pub struct SkipMapMemTable<K: MemKey> {
    rw_lock: RwLock<()>,
    inner: SkipMap<K, Value>,
}

impl DBCommand<UserKey> for SkipMapMemTable<UserKey> {
    fn range_get(&self, key_start: &UserKey, key_end: &UserKey, kvs: &mut SkipMap<UserKey, Value>) {
        let _guard = self.rw_lock.read().unwrap();
        self.inner.range_get(key_start, key_end, kvs);
    }

    fn get(&self, key: &UserKey) -> Result<Option<Value>> {
        let _guard = self.rw_lock.read().unwrap();
        Ok(self.inner.get_clone(key))
    }

    fn set(&mut self, key: UserKey, value: Value) -> Result<()> {
        let _guard = self.rw_lock.write().unwrap();
        self.inner.insert(key, value);
        Ok(())
    }

    fn remove(&mut self, key: UserKey) -> Result<()> {
        let _guard = self.rw_lock.write().unwrap();
        self.inner.insert(key, UserKey::new());
        Ok(())
    }
}

impl KeyValue for SkipMapMemTable<UserKey> {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&UserKey, &UserKey)> + '_> {
        Box::new(
            self.inner
                .iter_ptr()
                .map(|n| unsafe { (&(*n).entry.key, &(*n).entry.value) }),
        )
    }

    fn first_key(&self) -> Option<&UserKey> {
        self.inner.first_key_value().map(|entry| &entry.key)
    }

    fn last_key(&self) -> Option<&UserKey> {
        self.inner.last_key_value().map(|entry| &entry.key)
    }
}

impl MemTable<UserKey> for SkipMapMemTable<UserKey> {
    fn merge(&mut self, kvs: SkipMap<UserKey, Value>) {
        let _guard = self.rw_lock.write().unwrap();
        self.inner.merge(kvs);
    }
}

#[cfg(test)]
mod tests {
    use crate::db::DBCommand;
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
        assert_eq!(table.get(&one).unwrap().unwrap(), vec![]);
    }
}
