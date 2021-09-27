use crate::collections::skip_list::skipmap::ReadWriteMode::MrMw;
use crate::collections::skip_list::skipmap::{SkipMap, SrSwSkipMap};
use crate::db::key_types::{DBKey, RawUserKey, SeqNumKey, SequenceNumber};
use crate::db::{DBCommand, Value};
use crate::memory::skip_map_mem_table::{get_by_lsn_key, range_get_by_lsn_key};
use crate::memory::{InternalKeyValueIterator, MemTable, SkipMapMemTable};
use crate::Result;
use std::sync::atomic::{AtomicI64, Ordering};

pub struct MrMwSkipMapMemTable<SK: DBKey> {
    inner: SkipMap<SK, Value, { MrMw }>,
    mem_usage: AtomicI64,
}

impl<SK: DBKey> Default for MrMwSkipMapMemTable<SK> {
    fn default() -> Self {
        MrMwSkipMapMemTable {
            inner: SkipMap::default(),
            mem_usage: AtomicI64::default(),
        }
    }
}
impl DBCommand<RawUserKey, RawUserKey> for MrMwSkipMapMemTable<RawUserKey> {
    fn range_get(
        &self,
        key_start: &RawUserKey,
        key_end: &RawUserKey,
        kvs: &mut SrSwSkipMap<RawUserKey, Value>,
    ) where
        RawUserKey: Into<RawUserKey>,
        RawUserKey: From<RawUserKey>,
    {
        self.inner.range_get(key_start, key_end, kvs)
    }

    fn get(&self, key: &RawUserKey) -> crate::Result<Option<Value>> {
        Ok(self.inner.get_clone(key))
    }

    fn set(&self, key: RawUserKey, value: Value) -> crate::Result<()> {
        let key_mem_size = key.mem_size();
        let value_len = value.len();
        let mem_add = match self.inner.insert(key, value) {
            Some(v) => (value_len as i64 - v.len() as i64),
            None => (key_mem_size + value_len) as i64,
        } * std::mem::size_of::<u8>() as i64;
        self.mem_usage.fetch_add(mem_add, Ordering::Release);
        Ok(())
    }

    fn remove(&self, key: RawUserKey) -> crate::Result<()> {
        let key_mem_size = key.mem_size();
        let mem_add = match self.inner.insert(key, Value::default()) {
            Some(v) => -((v.len() * std::mem::size_of::<u8>()) as i64),
            None => (key_mem_size * std::mem::size_of::<u8>()) as i64,
        };
        self.mem_usage.fetch_add(mem_add, Ordering::Release);
        Ok(())
    }
}

impl InternalKeyValueIterator for MrMwSkipMapMemTable<RawUserKey> {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&RawUserKey, &Value)>> {
        Box::new(
            self.inner
                .iter_ptr()
                .map(|n| unsafe { (&(*n).entry.key, &(*n).entry.value) }),
        )
    }
}

impl MemTable<RawUserKey, RawUserKey> for MrMwSkipMapMemTable<RawUserKey> {
    fn merge(&self, kvs: SrSwSkipMap<RawUserKey, Value>, mem_usage: u64) {
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

impl SkipMapMemTable<RawUserKey, RawUserKey, { MrMw }> for MrMwSkipMapMemTable<RawUserKey> {
    fn get_inner(&self) -> &SkipMap<RawUserKey, Value, { MrMw }> {
        &self.inner
    }
}

impl<UK: DBKey> DBCommand<SeqNumKey<UK>, UK> for MrMwSkipMapMemTable<SeqNumKey<UK>> {
    fn range_get(
        &self,
        key_start: &SeqNumKey<UK>,
        key_end: &SeqNumKey<UK>,
        kvs: &mut SrSwSkipMap<UK, Value>,
    ) {
        debug_assert!(key_start.le(key_end));
        debug_assert_eq!(key_start.seq_num(), key_end.seq_num());

        range_get_by_lsn_key(&self.inner, key_start, key_end, kvs)
    }

    fn get(&self, key: &SeqNumKey<UK>) -> Result<Option<Value>> {
        get_by_lsn_key(&self.inner, key)
    }

    fn set(&self, key: SeqNumKey<UK>, value: Value) -> Result<()> {
        let key_mem_size = key.mem_size() as i64;
        let value_len = value.len() as i64;
        let mem_add = match self.inner.insert(key, value) {
            Some(v) => (value_len as i64 - v.len() as i64),
            None => (key_mem_size + value_len),
        } * std::mem::size_of::<u8>() as i64;
        self.mem_usage.fetch_add(mem_add, Ordering::Release);
        Ok(())
    }

    fn remove(&self, key: SeqNumKey<UK>) -> Result<()> {
        let key_mem_size = key.mem_size();

        let mem_add = match self.inner.insert(key, Value::default()) {
            Some(v) => -((v.len() * std::mem::size_of::<u8>()) as i64),
            None => (key_mem_size * std::mem::size_of::<u8>()) as i64,
        };
        self.mem_usage.fetch_add(mem_add, Ordering::Release);
        Ok(())
    }
}
