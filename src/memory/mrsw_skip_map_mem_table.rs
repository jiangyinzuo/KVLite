use crate::collections::skip_list::skipmap::{MrSwSkipMap, SrSwSkipMap};
use crate::db::key_types::{InternalKey, LSNKey, MemKey};
use crate::db::{DBCommand, Value};
use crate::memory::skip_map_mem_table::{get_by_lsn_key, range_get_by_lsn_key};
use crate::memory::{InternalKeyValueIterator, MemTable};
use crate::Result;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

#[derive(Default)]
pub struct MrSwSkipMapMemTable<SK: MemKey> {
    lock: Mutex<()>,
    inner: MrSwSkipMap<SK, Value>,
    mem_usage: AtomicI64,
}

unsafe impl<SK: MemKey> Sync for MrSwSkipMapMemTable<SK> {}

impl DBCommand<InternalKey, InternalKey> for MrSwSkipMapMemTable<InternalKey> {
    fn range_get(
        &self,
        key_start: &InternalKey,
        key_end: &InternalKey,
        kvs: &mut SrSwSkipMap<InternalKey, Value>,
    ) {
        self.inner.range_get(key_start, key_end, kvs)
    }

    fn get(&self, key: &InternalKey) -> Result<Option<Value>> {
        Ok(self.inner.get_clone(key))
    }

    fn set(&self, key: InternalKey, value: Value) -> Result<()> {
        let _guard = self.lock.lock().unwrap();
        let key_mem_size = key.mem_size();
        let value_len = value.len();
        let mem_add = match self.inner.insert(key, value) {
            Some(v) => (value_len as i64 - v.len() as i64),
            None => (key_mem_size + value_len) as i64,
        } * std::mem::size_of::<u8>() as i64;
        self.mem_usage.fetch_add(mem_add, Ordering::Release);
        Ok(())
    }

    fn remove(&self, key: InternalKey) -> Result<()> {
        let _guard = self.lock.lock().unwrap();
        let key_mem_size = key.mem_size();
        let mem_add = match self.inner.insert(key, Value::default()) {
            Some(v) => -((v.len() * std::mem::size_of::<u8>()) as i64),
            None => (key_mem_size * std::mem::size_of::<u8>()) as i64,
        };
        self.mem_usage.fetch_add(mem_add, Ordering::Release);
        Ok(())
    }
}

impl InternalKeyValueIterator for MrSwSkipMapMemTable<InternalKey> {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&InternalKey, &Value)>> {
        Box::new(
            self.inner
                .iter_ptr()
                .map(|n| unsafe { (&(*n).entry.key, &(*n).entry.value) }),
        )
    }
}

impl MemTable<InternalKey, InternalKey> for MrSwSkipMapMemTable<InternalKey> {
    fn merge(&self, kvs: SrSwSkipMap<InternalKey, Value>, mem_usage: u64) {
        let _guard = self.lock.lock().unwrap();
        self.mem_usage
            .fetch_add(mem_usage as i64, Ordering::Release);
        self.inner.merge(kvs);
    }

    fn approximate_memory_usage(&self) -> u64 {
        let mem_usage = self.mem_usage.load(Ordering::Acquire);
        debug_assert!(mem_usage >= 0);
        mem_usage as u64
    }
}

impl<UK: MemKey> DBCommand<LSNKey<UK>, UK> for MrSwSkipMapMemTable<LSNKey<UK>> {
    fn range_get(
        &self,
        key_start: &LSNKey<UK>,
        key_end: &LSNKey<UK>,
        kvs: &mut SrSwSkipMap<UK, Value>,
    ) {
        debug_assert!(key_start.le(key_end));
        debug_assert_eq!(key_start.lsn(), key_end.lsn());

        range_get_by_lsn_key(&self.inner, key_start, key_end, kvs)
    }

    fn get(&self, key: &LSNKey<UK>) -> Result<Option<Value>> {
        get_by_lsn_key(&self.inner, key)
    }

    fn set(&self, key: LSNKey<UK>, value: Value) -> Result<()> {
        let key_mem_size = key.mem_size() as i64;
        let value_len = value.len() as i64;

        let _guard = self.lock.lock().unwrap();
        let mem_add = match self.inner.insert(key, value) {
            Some(v) => (value_len as i64 - v.len() as i64),
            None => (key_mem_size + value_len),
        } * std::mem::size_of::<u8>() as i64;
        self.mem_usage.fetch_add(mem_add, Ordering::Release);
        Ok(())
    }

    fn remove(&self, key: LSNKey<UK>) -> Result<()> {
        let key_mem_size = key.mem_size();
        let _guard = self.lock.lock().unwrap();
        let mem_add = match self.inner.insert(key, Value::default()) {
            Some(v) => -((v.len() * std::mem::size_of::<u8>()) as i64),
            None => (key_mem_size * std::mem::size_of::<u8>()) as i64,
        };
        self.mem_usage.fetch_add(mem_add, Ordering::Release);
        Ok(())
    }
}
