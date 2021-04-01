pub mod skip_list;
pub mod treap;

pub struct Entry<K: Ord, V> {
    pub key: K,
    pub value: V,
}
