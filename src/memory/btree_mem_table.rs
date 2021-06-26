use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::key_types::{InternalKey, MemKey};
use crate::db::{DBCommand, Value};
use crate::memory::{InternalKeyValueIterator, MemTable};
use crate::Result;
use std::collections::BTreeMap;
use std::sync::RwLock;

/// Wrapper of `BTreeMap<String, String>`
pub struct BTreeMemTable<SK: MemKey> {
    rw_lock: RwLock<()>,
    inner: BTreeMap<SK, Value>,
    mem_usage: u64,
}

impl DBCommand<InternalKey, InternalKey> for BTreeMemTable<InternalKey> {
    fn range_get(
        &self,
        key_start: &InternalKey,
        key_end: &InternalKey,
        kvs: &mut SkipMap<InternalKey, Value>,
    ) {
        let _guard = self.rw_lock.read().unwrap();
        self.inner.get_key_value(key_end);
        for (k, v) in self.inner.range::<InternalKey, _>(key_start..=key_end) {
            kvs.insert(k.clone(), v.clone());
        }
    }

    fn get(&self, key: &InternalKey) -> Result<Option<Value>> {
        let _lock = self.rw_lock.read().unwrap();
        Ok(self.inner.get(key).cloned())
    }

    fn set(&mut self, key: InternalKey, value: Value) -> Result<()> {
        let _lock = self.rw_lock.write().unwrap();
        let mem_usage = key.len() * std::mem::size_of::<u8>();
        self.inner.insert(key, value);
        self.mem_usage += mem_usage as u64;
        Ok(())
    }

    fn remove(&mut self, key: InternalKey) -> Result<()> {
        let _lock = self.rw_lock.write().unwrap();
        let mem_usage = key.len() * std::mem::size_of::<u8>();
        self.inner.insert(key, InternalKey::default());
        self.mem_usage += mem_usage as u64;
        Ok(())
    }
}

impl<K: MemKey> Default for BTreeMemTable<K> {
    fn default() -> Self {
        BTreeMemTable {
            rw_lock: RwLock::default(),
            inner: BTreeMap::default(),
            mem_usage: 0,
        }
    }
}

impl InternalKeyValueIterator for BTreeMemTable<InternalKey> {
    fn len(&self) -> usize {
        let _lock = self.rw_lock.read().unwrap();
        self.inner.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&InternalKey, &Value)> + '_> {
        let _lock = self.rw_lock.read().unwrap();
        Box::new(self.inner.iter())
    }
}

impl MemTable<InternalKey, InternalKey> for BTreeMemTable<InternalKey> {
    fn merge(&mut self, kvs: SkipMap<InternalKey, Value>) {
        let _guard = self.rw_lock.write().unwrap();
        self.inner.extend(kvs.into_iter());
    }

    fn approximate_memory_usage(&self) -> u64 {
        self.mem_usage
    }
}

#[cfg(test)]
mod tests {
    use crate::db::DBCommand;
    use crate::memory::{BTreeMemTable, InternalKeyValueIterator};
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
