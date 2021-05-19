use crate::collections::skip_list::skipmap::SkipMap;
use crate::db::key_types::{LSNKey, MemKey, UserKey};
use crate::db::{DBCommand, Value};
use crate::memory::{MemTable, UserKeyValueIterator};
use crate::Result;
use std::sync::RwLock;

#[derive(Default)]
pub struct SkipMapMemTable<K: MemKey> {
    rw_lock: RwLock<()>,
    inner_guarded: SkipMap<K, Value>,
}

impl DBCommand<UserKey> for SkipMapMemTable<UserKey> {
    fn range_get(&self, key_start: &UserKey, key_end: &UserKey, kvs: &mut SkipMap<UserKey, Value>) {
        let _guard = self.rw_lock.read().unwrap();
        self.inner_guarded.range_get(key_start, key_end, kvs);
    }

    fn get(&self, key: &UserKey) -> Result<Option<Value>> {
        let _guard = self.rw_lock.read().unwrap();
        Ok(self.inner_guarded.get_clone(key))
    }

    fn set(&mut self, key: UserKey, value: Value) -> Result<()> {
        let _guard = self.rw_lock.write().unwrap();
        self.inner_guarded.insert(key, value);
        Ok(())
    }

    fn remove(&mut self, key: UserKey) -> Result<()> {
        let _guard = self.rw_lock.write().unwrap();
        self.inner_guarded.insert(key, UserKey::new());
        Ok(())
    }
}

impl UserKeyValueIterator for SkipMapMemTable<UserKey> {
    fn len(&self) -> usize {
        self.inner_guarded.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&UserKey, &Value)> + '_> {
        Box::new(
            self.inner_guarded
                .iter_ptr()
                .map(|n| unsafe { (&(*n).entry.key, &(*n).entry.value) }),
        )
    }
}

impl MemTable<UserKey> for SkipMapMemTable<UserKey> {
    fn merge(&mut self, kvs: SkipMap<UserKey, Value>) {
        let _guard = self.rw_lock.write().unwrap();
        self.inner_guarded.merge(kvs);
    }
}

impl DBCommand<LSNKey> for SkipMapMemTable<LSNKey> {
    fn range_get(&self, key_start: &LSNKey, key_end: &LSNKey, kvs: &mut SkipMap<UserKey, Value>) {
        let _guard = self.rw_lock.read().unwrap();
        todo!()
    }

    fn get(&self, key: &LSNKey) -> Result<Option<Value>> {
        let _guard = self.rw_lock.read().unwrap();
        let node = self.inner_guarded.find_last_le(key);
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

    fn set(&mut self, key: LSNKey, value: Value) -> Result<()> {
        let _guard = self.rw_lock.write().unwrap();
        todo!()
    }

    fn remove(&mut self, key: LSNKey) -> Result<()> {
        let _guard = self.rw_lock.write().unwrap();
        todo!()
    }
}

impl UserKeyValueIterator for SkipMapMemTable<LSNKey> {
    fn len(&self) -> usize {
        self.inner_guarded.len()
    }

    fn kv_iter(&self) -> Box<dyn Iterator<Item = (&UserKey, &Value)>> {
        Box::new(self.inner_guarded.iter_ptr().filter_map(|n| {
            debug_assert!(!n.is_null());
            unsafe {
                let next = (*n).get_next(0);
                let user_key = (*n).entry.key.user_key();
                if next.is_null() {
                    Some((user_key, &(*n).entry.value))
                } else {
                    match user_key.cmp((*next).entry.key.user_key()) {
                        std::cmp::Ordering::Equal => None,
                        _ => Some((user_key, &(*n).entry.value)),
                    }
                }
            }
        }))
    }
}

impl MemTable<LSNKey> for SkipMapMemTable<LSNKey> {
    fn merge(&mut self, kvs: SkipMap<LSNKey, Value>) {
        let _guard = self.rw_lock.write().unwrap();
        self.inner_guarded.merge(kvs);
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
