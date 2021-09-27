use crate::collections::skip_list::skipmap::{Node, ReadWriteMode, SkipMap, SrSwSkipMap};
use crate::collections::skip_list::MemoryAllocator;
use crate::db::key_types::{DBKey, RawUserKey, SeqNumKey};
use crate::db::{DBCommand, Value};
use crate::memory::{InternalKeyValueIterator, MemTable};
use crate::Result;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

#[derive(Default)]
pub struct MutexSkipMapMemTable<SK: DBKey> {
    lock: Mutex<()>,
    inner_guarded: SrSwSkipMap<SK, Value>,
    mem_usage: AtomicI64,
}

impl DBCommand<RawUserKey, RawUserKey> for MutexSkipMapMemTable<RawUserKey> {
    fn range_get(
        &self,
        key_start: &RawUserKey,
        key_end: &RawUserKey,
        kvs: &mut SrSwSkipMap<RawUserKey, Value>,
    ) {
        let _guard = self.lock.lock().unwrap();
        self.inner_guarded.range_get(key_start, key_end, kvs);
    }

    fn get(&self, key: &RawUserKey) -> Result<Option<Value>> {
        let _guard = self.lock.lock().unwrap();
        Ok(self.inner_guarded.get_clone(key))
    }

    fn set(&self, key: RawUserKey, value: Value) -> Result<()> {
        let _guard = self.lock.lock().unwrap();
        let key_len = key.len();
        let value_len = value.len();
        let mem_add = match self.inner_guarded.insert(key, value) {
            Some(v) => ((key_len + value_len - v.len()) * std::mem::size_of::<u8>()) as i64,
            None => ((key_len + value_len) * std::mem::size_of::<u8>()) as i64,
        };
        self.mem_usage.fetch_add(mem_add, Ordering::Release);
        Ok(())
    }

    fn remove(&self, key: RawUserKey) -> Result<()> {
        let _guard = self.lock.lock().unwrap();

        let key_len = key.len();
        let mem_add = match self.inner_guarded.insert(key, Value::default()) {
            Some(v) => -((v.len() * std::mem::size_of::<u8>()) as i64),
            None => (key_len * std::mem::size_of::<u8>()) as i64,
        };
        self.mem_usage.fetch_add(mem_add, Ordering::Release);
        Ok(())
    }
}

impl InternalKeyValueIterator for MutexSkipMapMemTable<RawUserKey> {
    fn len(&self) -> usize {
        self.inner_guarded.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&RawUserKey, &Value)> + '_> {
        Box::new(
            self.inner_guarded
                .iter_ptr()
                .map(|n| unsafe { (&(*n).entry.key, &(*n).entry.value) }),
        )
    }
}

impl MemTable<RawUserKey, RawUserKey> for MutexSkipMapMemTable<RawUserKey> {
    fn merge(&self, kvs: SrSwSkipMap<RawUserKey, Value>, mem_size: u64) {
        let _guard = self.lock.lock().unwrap();
        self.mem_usage.fetch_add(mem_size as i64, Ordering::Release);
        self.inner_guarded.merge(kvs);
    }

    fn approximate_memory_usage(&self) -> u64 {
        let mem_usage = self.mem_usage.load(Ordering::Acquire);
        debug_assert!(mem_usage > 0);
        mem_usage as u64
    }
}

pub(super) fn range_get_by_lsn_key<UK: DBKey, const RW_MODE: ReadWriteMode>(
    skip_map: &SkipMap<SeqNumKey<UK>, Value, RW_MODE>,
    key_start: &SeqNumKey<UK>,
    key_end: &SeqNumKey<UK>,
    kvs: &mut SrSwSkipMap<UK, Value>,
) {
    let mut node = skip_map.find_last_le(key_start);
    if node.is_null() {
        return;
    }
    unsafe {
        let user_key = (*node).entry.key.user_key();
        if user_key.eq(key_start.user_key()) {
            kvs.insert(user_key.clone(), (*node).entry.value.clone());
        }
    }

    loop {
        let lsn_max = unsafe { SeqNumKey::upper_bound(&(*node).entry.key) };
        unsafe {
            // get next user key
            node = Node::find_first_ge_from_node(node, &lsn_max);
            if node.is_null() || (*node).entry.key.user_key().gt(key_end.user_key()) {
                return;
            }

            let lsn_key = SeqNumKey::new((*node).entry.key.user_key().clone(), key_end.seq_num());
            node = Node::find_last_le_from_node(node, &lsn_key);
            debug_assert!(!node.is_null());
            if (*node).entry.key.user_key().eq(lsn_key.user_key()) {
                kvs.insert(lsn_key.user_key().clone(), (*node).entry.value.clone());
            }
        }
    }
}

pub(super) fn get_by_lsn_key<UK: DBKey, const RW_MODE: ReadWriteMode>(
    skip_map: &SkipMap<SeqNumKey<UK>, Value, RW_MODE>,
    key: &SeqNumKey<UK>,
) -> Result<Option<Value>> {
    let node = skip_map.find_last_le(key);
    if node.is_null() {
        return Ok(None);
    }
    unsafe {
        if (*node).entry.key.user_key().eq(key.user_key()) {
            Ok(Some((*node).entry.value.clone()))
        } else {
            Ok(None)
        }
    }
}

impl<UK: DBKey> DBCommand<SeqNumKey<UK>, UK> for MutexSkipMapMemTable<SeqNumKey<UK>> {
    fn range_get(
        &self,
        key_start: &SeqNumKey<UK>,
        key_end: &SeqNumKey<UK>,
        kvs: &mut SrSwSkipMap<UK, Value>,
    ) {
        debug_assert!(key_start.le(key_end));
        debug_assert_eq!(key_start.seq_num(), key_end.seq_num());

        let _guard = self.lock.lock().unwrap();
        range_get_by_lsn_key(&self.inner_guarded, key_start, key_end, kvs)
    }

    fn get(&self, key: &SeqNumKey<UK>) -> Result<Option<Value>> {
        let _guard = self.lock.lock().unwrap();
        get_by_lsn_key(&self.inner_guarded, key)
    }

    fn set(&self, key: SeqNumKey<UK>, value: Value) -> Result<()> {
        let _guard = self.lock.lock().unwrap();

        let key_mem_size = key.mem_size() as i64;
        let value_len = value.len() as i64;
        let mem_add = match self.inner_guarded.insert(key, value) {
            Some(v) => (value_len as i64 - v.len() as i64),
            None => (key_mem_size + value_len),
        } * std::mem::size_of::<u8>() as i64;
        self.mem_usage.fetch_add(mem_add, Ordering::Release);
        Ok(())
    }

    fn remove(&self, key: SeqNumKey<UK>) -> Result<()> {
        let _guard = self.lock.lock().unwrap();
        let key_mem_size = key.mem_size();

        let mem_add = match self.inner_guarded.insert(key, Value::default()) {
            Some(v) => -((v.len() * std::mem::size_of::<u8>()) as i64),
            None => (key_mem_size * std::mem::size_of::<u8>()) as i64,
        };
        self.mem_usage.fetch_add(mem_add, Ordering::Release);
        Ok(())
    }
}

impl<K: DBKey + 'static> InternalKeyValueIterator for MutexSkipMapMemTable<SeqNumKey<K>> {
    fn len(&self) -> usize {
        self.inner_guarded.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&RawUserKey, &Value)>> {
        Box::new(self.inner_guarded.iter_ptr().filter_map(|n| {
            debug_assert!(!n.is_null());
            unsafe {
                let next = (*n).get_next(0);
                let internal_key = (*n).entry.key.raw_user_key();
                if next.is_null() {
                    Some((internal_key, &(*n).entry.value))
                } else {
                    match internal_key.cmp((*next).entry.key.raw_user_key()) {
                        std::cmp::Ordering::Equal => None,
                        _ => Some((internal_key, &(*n).entry.value)),
                    }
                }
            }
        }))
    }
}

impl<UK: 'static + DBKey> MemTable<SeqNumKey<UK>, UK> for MutexSkipMapMemTable<SeqNumKey<UK>> {
    fn merge(&self, kvs: SrSwSkipMap<SeqNumKey<UK>, Value>, mem_size: u64) {
        let _guard = self.lock.lock().unwrap();
        self.mem_usage.fetch_add(mem_size as i64, Ordering::Release);
        self.inner_guarded.merge(kvs);
    }

    fn approximate_memory_usage(&self) -> u64 {
        let mem_usage = self.mem_usage.load(Ordering::Acquire);
        debug_assert!(mem_usage >= 0, "mem_usage: {}", mem_usage);
        mem_usage as u64
    }
}

#[cfg(test)]
mod internal_key_tests {
    use crate::db::DBCommand;
    use crate::memory::MutexSkipMapMemTable;

    #[test]
    fn test_insert() {
        let table = MutexSkipMapMemTable::default();

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

#[cfg(test)]
mod lsn_tests {
    use crate::collections::skip_list::skipmap::SkipMap;
    use crate::db::key_types::{I32UserKey, SeqNumKey};
    use crate::db::{DBCommand, Value};
    use crate::memory::{MutexSkipMapMemTable, SkipMapMemTable};

    #[test]
    fn test_range_get() {
        let table = MutexSkipMapMemTable::<SeqNumKey<I32UserKey>>::default();
        for lsn in 1..8 {
            for k in -100i32..100i32 {
                table
                    .set(
                        SeqNumKey::new(I32UserKey::new(k), lsn),
                        Value::from(k.to_be_bytes()),
                    )
                    .unwrap();
            }
        }

        let mut kvs = SkipMap::new();
        table.range_get(
            &SeqNumKey::new(I32UserKey::new(-10i32), 5),
            &SeqNumKey::new(I32UserKey::new(20i32), 5),
            &mut kvs,
        );
        assert_eq!(kvs.len(), 31);
        let option = table
            .get(&SeqNumKey::new(I32UserKey::new(20i32), 100))
            .unwrap();
        assert_eq!(option, Some(Value::from(20i32.to_be_bytes())));
    }
}
