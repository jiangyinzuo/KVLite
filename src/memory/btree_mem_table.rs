use std::collections::BTreeMap;
use std::sync::RwLock;

use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::key_types::{MemKey, UserKey};
use crate::db::{DBCommand, Value};
use crate::memory::{KeyValue, MemTable};
use crate::Result;

/// Wrapper of `BTreeMap<String, String>`
pub struct BTreeMemTable<K: MemKey> {
    rw_lock: RwLock<()>,
    inner: BTreeMap<K, Value>,
}

impl DBCommand<UserKey> for BTreeMemTable<UserKey> {
    fn range_get(&self, key_start: &UserKey, key_end: &UserKey, kvs: &mut SkipMap<UserKey, Value>) {
        let _guard = self.rw_lock.read().unwrap();
        self.inner.get_key_value(key_end);
        for (k, v) in self.inner.range::<UserKey, _>(key_start..=key_end) {
            kvs.insert(k.clone(), v.clone());
        }
    }

    fn get(&self, key: &UserKey) -> Result<Option<Value>> {
        let _lock = self.rw_lock.read().unwrap();
        Ok(self.inner.get(key).cloned())
    }

    fn set(&mut self, key: UserKey, value: Value) -> Result<()> {
        let _lock = self.rw_lock.read().unwrap();
        self.inner.insert(key, value);
        Ok(())
    }

    fn remove(&mut self, key: UserKey) -> Result<()> {
        let _lock = self.rw_lock.write().unwrap();
        self.inner.insert(key, UserKey::default());
        Ok(())
    }
}

impl<K: MemKey> Default for BTreeMemTable<K> {
    fn default() -> Self {
        BTreeMemTable {
            rw_lock: RwLock::default(),
            inner: BTreeMap::default(),
        }
    }
}

impl KeyValue for BTreeMemTable<UserKey> {
    fn len(&self) -> usize {
        let _lock = self.rw_lock.read().unwrap();
        self.inner.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&UserKey, &Value)> + '_> {
        let _lock = self.rw_lock.read().unwrap();
        Box::new(self.inner.iter())
    }

    fn first_key(&self) -> Option<&UserKey> {
        let _lock = self.rw_lock.read().unwrap();
        self.inner.first_key_value().map(|(k, v)| k)
    }

    fn last_key(&self) -> Option<&UserKey> {
        let _lock = self.rw_lock.read().unwrap();
        self.inner.last_key_value().map(|(k, v)| k)
    }
}

impl MemTable<UserKey> for BTreeMemTable<UserKey> {
    fn merge(&mut self, kvs: SkipMap<UserKey, Value>) {
        let _guard = self.rw_lock.write().unwrap();
        self.inner.extend(kvs.into_iter());
    }
}

#[cfg(test)]
mod tests {
    use crate::db::DBCommand;
    use crate::memory::{BTreeMemTable, KeyValue};
    use crate::Result;

    #[test]
    fn test_iter() -> Result<()> {
        let mut mem_table = BTreeMemTable::default();
        for i in 0..100i8 {
            mem_table.set(Vec::from(i.to_le_bytes()), Vec::from(i.to_le_bytes()))?;
        }

        for (key, value) in mem_table.kv_iter() {
            assert_eq!(key, value);
        }
        Ok(())
    }
}
