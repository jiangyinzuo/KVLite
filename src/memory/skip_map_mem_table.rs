use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::key_types::{InternalKey, LSNKey, MemKey};
use crate::db::{DBCommand, Value};
use crate::memory::{MemTable, UserKeyValueIterator};
use crate::Result;
use std::sync::RwLock;

#[derive(Default)]
pub struct SkipMapMemTable<SK: MemKey> {
    rw_lock: RwLock<()>,
    inner_guarded: SkipMap<SK, Value>,
}

impl DBCommand<InternalKey, InternalKey> for SkipMapMemTable<InternalKey> {
    fn range_get(
        &self,
        key_start: &InternalKey,
        key_end: &InternalKey,
        kvs: &mut SkipMap<InternalKey, Value>,
    ) {
        let _guard = self.rw_lock.read().unwrap();
        self.inner_guarded.range_get(key_start, key_end, kvs);
    }

    fn get(&self, key: &InternalKey) -> Result<Option<Value>> {
        let _guard = self.rw_lock.read().unwrap();
        Ok(self.inner_guarded.get_clone(key))
    }

    fn set(&mut self, key: InternalKey, value: Value) -> Result<()> {
        let _guard = self.rw_lock.write().unwrap();
        self.inner_guarded.insert(key, value);
        Ok(())
    }

    fn remove(&mut self, key: InternalKey) -> Result<()> {
        let _guard = self.rw_lock.write().unwrap();
        self.inner_guarded.insert(key, InternalKey::new());
        Ok(())
    }
}

impl UserKeyValueIterator for SkipMapMemTable<InternalKey> {
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
    fn merge(&mut self, kvs: SkipMap<InternalKey, Value>) {
        let _guard = self.rw_lock.write().unwrap();
        self.inner_guarded.merge(kvs);
    }
}

impl<UK: MemKey> DBCommand<LSNKey<UK>, UK> for SkipMapMemTable<LSNKey<UK>> {
    fn range_get(
        &self,
        key_start: &LSNKey<UK>,
        key_end: &LSNKey<UK>,
        kvs: &mut SkipMap<UK, Value>,
    ) {
        debug_assert!(key_start.le(key_end));
        debug_assert_eq!(key_start.lsn(), key_end.lsn());

        let _guard = self.rw_lock.read().unwrap();

        let mut node = self.inner_guarded.find_last_le(key_start);
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

    fn get(&self, key: &LSNKey<UK>) -> Result<Option<Value>> {
        let _guard = self.rw_lock.read().unwrap();
        let node = self.inner_guarded.find_last_le(key);
        if node.is_null() {
            return Ok(None);
        }
        unsafe {
            if (*node).entry.key.internal_key().eq(key.internal_key()) {
                Ok(Some((*node).entry.value.clone()))
            } else {
                Ok(None)
            }
        }
    }

    fn set(&mut self, key: LSNKey<UK>, value: Value) -> Result<()> {
        let _guard = self.rw_lock.write().unwrap();
        self.inner_guarded.insert(key, value);
        Ok(())
    }

    fn remove(&mut self, key: LSNKey<UK>) -> Result<()> {
        let _guard = self.rw_lock.write().unwrap();
        self.inner_guarded.insert(key, Value::default());
        Ok(())
    }
}

impl<K: MemKey + 'static> UserKeyValueIterator for SkipMapMemTable<LSNKey<K>> {
    fn len(&self) -> usize {
        self.inner_guarded.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&InternalKey, &Value)>> {
        Box::new(self.inner_guarded.iter_ptr().filter_map(|n| {
            debug_assert!(!n.is_null());
            unsafe {
                let next = (*n).get_next(0);
                let user_key = (*n).entry.key.internal_key();
                if next.is_null() {
                    Some((user_key, &(*n).entry.value))
                } else {
                    match user_key.cmp((*next).entry.key.internal_key()) {
                        std::cmp::Ordering::Equal => None,
                        _ => Some((user_key, &(*n).entry.value)),
                    }
                }
            }
        }))
    }
}

impl<UK: 'static + MemKey> MemTable<LSNKey<UK>, UK> for SkipMapMemTable<LSNKey<UK>> {
    fn merge(&mut self, kvs: SkipMap<LSNKey<UK>, Value>) {
        let _guard = self.rw_lock.write().unwrap();
        self.inner_guarded.merge(kvs);
    }
}

#[cfg(test)]
mod user_key_tests {
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

#[cfg(test)]
mod lsn_tests {
    use crate::collections::skip_list::skipmap::SkipMap;
    use crate::db::key_types::{I32UserKey, LSNKey};
    use crate::db::{DBCommand, Value};
    use crate::memory::SkipMapMemTable;

    #[test]
    fn test_range_get() {
        let mut table = SkipMapMemTable::<LSNKey<I32UserKey>>::default();
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
    }
}
