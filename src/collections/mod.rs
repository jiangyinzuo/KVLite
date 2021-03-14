pub mod skiplist;
pub mod treap;

pub struct Entry<K: Ord, V> {
    pub key: K,
    pub value: V,
}
