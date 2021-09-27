use crate::collections::skip_list::skipmap::SrSwSkipMap;
use crate::db::key_types::{DBKey, RawUserKey, SequenceNumber};
use crate::db::{DBCommand, Value};
use crate::memory::{InternalKeyValueIterator, MemTable};
use crate::Result;
use std::cell::UnsafeCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::RwLock;

/// Wrapper of `BTreeMap<String, String>`
pub struct BTreeMemTable<SK: DBKey> {
    rw_lock: RwLock<()>,
    inner: UnsafeCell<BTreeMap<SK, Value>>,
    mem_usage: AtomicI64,
}

unsafe impl<SK: DBKey> Sync for BTreeMemTable<SK> {}

impl DBCommand<RawUserKey, RawUserKey> for BTreeMemTable<RawUserKey> {
    fn range_get(
        &self,
        key_start: &RawUserKey,
        key_end: &RawUserKey,
        kvs: &mut SrSwSkipMap<RawUserKey, Value>,
    ) {
        let _guard = self.rw_lock.read().unwrap();
        let inner_ptr = self.inner.get();
        unsafe {
            (*inner_ptr).get_key_value(key_end);
            for (k, v) in (*inner_ptr).range::<RawUserKey, _>(key_start..=key_end) {
                kvs.insert(k.clone(), v.clone());
            }
        }
    }

    fn get(&self, key: &RawUserKey) -> Result<Option<Value>> {
        let _lock = self.rw_lock.read().unwrap();
        Ok(unsafe { (*self.inner.get()).get(key).cloned() })
    }

    fn set(&self, key: RawUserKey, value: Value) -> Result<()> {
        let _lock = self.rw_lock.write().unwrap();
        let key_length = key.len();
        let value_length = value.len();
        let option = unsafe { (*self.inner.get()).insert(key, value) };
        let mem_add = match option {
            Some(v) => (value_length as i64 - v.len() as i64) * std::mem::size_of::<u8>() as i64,
            None => ((key_length + value_length) * std::mem::size_of::<u8>()) as i64,
        };
        self.mem_usage.fetch_add(mem_add, Ordering::Release);
        Ok(())
    }

    fn remove(&self, key: RawUserKey) -> Result<()> {
        let _lock = self.rw_lock.write().unwrap();
        unsafe {
            let key_len = key.len();
            let option = (*self.inner.get()).insert(key, RawUserKey::default());
            let mem_add = match option {
                Some(v) => -(v.len() as i64),
                None => key_len as i64 * std::mem::size_of::<u8>() as i64,
            };

            self.mem_usage.fetch_add(mem_add, Ordering::Release);
        }
        Ok(())
    }
}

impl<K: DBKey> Default for BTreeMemTable<K> {
    fn default() -> Self {
        BTreeMemTable {
            rw_lock: RwLock::default(),
            inner: UnsafeCell::new(BTreeMap::default()),
            mem_usage: AtomicI64::default(),
        }
    }
}

impl InternalKeyValueIterator for BTreeMemTable<RawUserKey> {
    fn len(&self) -> usize {
        let _lock = self.rw_lock.read().unwrap();
        unsafe { (*self.inner.get()).len() }
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&RawUserKey, &Value)> + '_> {
        let _lock = self.rw_lock.read().unwrap();
        Box::new(unsafe { (*self.inner.get()).iter() })
    }
}

impl MemTable<RawUserKey, RawUserKey> for BTreeMemTable<RawUserKey> {
    fn merge(&self, kvs: SrSwSkipMap<RawUserKey, Value>, memory_size: u64) {
        let mut _lock_guard = self.rw_lock.write().unwrap();
        unsafe {
            (*self.inner.get()).extend(kvs.into_iter());
        }
        self.mem_usage
            .fetch_add(memory_size as i64, Ordering::Release);
    }

    fn approximate_memory_usage(&self) -> u64 {
        let mem_size = self.mem_usage.load(Ordering::Acquire);
        debug_assert!(mem_size >= 0);
        mem_size as u64
    }
}

#[cfg(test)]
mod tests {
    use crate::db::DBCommand;
    use crate::memory::{BTreeMemTable, InternalKeyValueIterator};
    use crate::Result;

    #[test]
    fn test_iter() -> Result<()> {
        let mem_table = BTreeMemTable::default();
        for i in 0..100i8 {
            mem_table.set(Vec::from(i.to_le_bytes()), Vec::from(i.to_le_bytes()))?;
        }

        for (key, value) in mem_table.kv_iter() {
            assert_eq!(key, value);
        }
        Ok(())
    }
}
