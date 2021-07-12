use crate::collections::skip_list::skipmap::{ReadWriteMode, SkipMap, SrSwSkipMap};
use crate::db::key_types::{InternalKey, LSNKey, MemKey};
use crate::db::{DBCommand, Value};
use crate::memory::{InternalKeyValueIterator, MemTable};
use crate::Result;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

#[derive(Default)]
pub struct SkipMapMemTable<SK: MemKey> {
    lock: Mutex<()>,
    inner_guarded: SrSwSkipMap<SK, Value>,
    mem_usage: AtomicI64,
}

impl DBCommand<InternalKey, InternalKey> for SkipMapMemTable<InternalKey> {
    fn range_get(
        &self,
        key_start: &InternalKey,
        key_end: &InternalKey,
        kvs: &mut SrSwSkipMap<InternalKey, Value>,
    ) {
        let _guard = self.lock.lock().unwrap();
        self.inner_guarded.range_get(key_start, key_end, kvs);
    }

    fn get(&self, key: &InternalKey) -> Result<Option<Value>> {
        let _guard = self.lock.lock().unwrap();
        Ok(self.inner_guarded.get_clone(key))
    }

    fn set(&self, key: InternalKey, value: Value) -> Result<()> {
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

    fn remove(&self, key: InternalKey) -> Result<()> {
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

impl InternalKeyValueIterator for SkipMapMemTable<InternalKey> {
    fn len(&self) -> usize {
        self.inner_guarded.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&InternalKey, &Value)> + '_> {
        Box::new(
            self.inner_guarded
                .iter_ptr()
                .map(|n| unsafe { (&(*n).entry.key, &(*n).entry.value) }),
        )
    }
}

impl MemTable<InternalKey, InternalKey> for SkipMapMemTable<InternalKey> {
    fn merge(&self, kvs: SrSwSkipMap<InternalKey, Value>, mem_size: u64) {
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

pub(super) fn range_get_by_lsn_key<UK: MemKey, const RW_MODE: ReadWriteMode>(
    skip_map: &SkipMap<LSNKey<UK>, Value, RW_MODE>,
    key_start: &LSNKey<UK>,
    key_end: &LSNKey<UK>,
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
        let lsn_max = unsafe { LSNKey::upper_bound(&(*node).entry.key) };
        unsafe {
            // get next user key
            node = SkipMap::find_first_ge_from_node(node, &lsn_max);
            if node.is_null() || (*node).entry.key.user_key().gt(key_end.user_key()) {
                return;
            }

            let lsn_key = LSNKey::new((*node).entry.key.user_key().clone(), key_end.lsn());
            node = SkipMap::find_last_le_from_node(node, &lsn_key);
            debug_assert!(!node.is_null());
            if (*node).entry.key.user_key().eq(lsn_key.user_key()) {
                kvs.insert(lsn_key.user_key().clone(), (*node).entry.value.clone());
            }
        }
    }
}

pub(super) fn get_by_lsn_key<UK: MemKey, const RW_MODE: ReadWriteMode>(
    skip_map: &SkipMap<LSNKey<UK>, Value, RW_MODE>,
    key: &LSNKey<UK>,
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

impl<UK: MemKey> DBCommand<LSNKey<UK>, UK> for SkipMapMemTable<LSNKey<UK>> {
    fn range_get(
        &self,
        key_start: &LSNKey<UK>,
        key_end: &LSNKey<UK>,
        kvs: &mut SrSwSkipMap<UK, Value>,
    ) {
        debug_assert!(key_start.le(key_end));
        debug_assert_eq!(key_start.lsn(), key_end.lsn());

        let _guard = self.lock.lock().unwrap();
        range_get_by_lsn_key(&self.inner_guarded, key_start, key_end, kvs)
    }

    fn get(&self, key: &LSNKey<UK>) -> Result<Option<Value>> {
        let _guard = self.lock.lock().unwrap();
        get_by_lsn_key(&self.inner_guarded, key)
    }

    fn set(&self, key: LSNKey<UK>, value: Value) -> Result<()> {
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

    fn remove(&self, key: LSNKey<UK>) -> Result<()> {
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

impl<K: MemKey + 'static> InternalKeyValueIterator for SkipMapMemTable<LSNKey<K>> {
    fn len(&self) -> usize {
        self.inner_guarded.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&InternalKey, &Value)>> {
        Box::new(self.inner_guarded.iter_ptr().filter_map(|n| {
            debug_assert!(!n.is_null());
            unsafe {
                let next = (*n).get_next(0);
                let internal_key = (*n).entry.key.internal_key();
                if next.is_null() {
                    Some((internal_key, &(*n).entry.value))
                } else {
                    match internal_key.cmp((*next).entry.key.internal_key()) {
                        std::cmp::Ordering::Equal => None,
                        _ => Some((internal_key, &(*n).entry.value)),
                    }
                }
            }
        }))
    }
}

impl<UK: 'static + MemKey> MemTable<LSNKey<UK>, UK> for SkipMapMemTable<LSNKey<UK>> {
    fn merge(&self, kvs: SrSwSkipMap<LSNKey<UK>, Value>, mem_size: u64) {
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
    use crate::memory::SkipMapMemTable;

    #[test]
    fn test_insert() {
        let table = SkipMapMemTable::default();

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
    use crate::db::key_types::{I32UserKey, LSNKey};
    use crate::db::{DBCommand, Value};
    use crate::memory::SkipMapMemTable;

    #[test]
    fn test_range_get() {
        let table = SkipMapMemTable::<LSNKey<I32UserKey>>::default();
        for lsn in 1..8 {
            for k in -100i32..100i32 {
                table
                    .set(
                        LSNKey::new(I32UserKey::new(k), lsn),
                        Value::from(k.to_be_bytes()),
                    )
                    .unwrap();
            }
        }

        let mut kvs = SkipMap::new();
        table.range_get(
            &LSNKey::new(I32UserKey::new(-10i32), 5),
            &LSNKey::new(I32UserKey::new(20i32), 5),
            &mut kvs,
        );
        assert_eq!(kvs.len(), 31);
        let option = table
            .get(&LSNKey::new(I32UserKey::new(20i32), 100))
            .unwrap();
        assert_eq!(option, Some(Value::from(20i32.to_be_bytes())));
    }
}
