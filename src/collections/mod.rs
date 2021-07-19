pub mod skip_list;
pub mod treap;

#[derive(Default)]
pub struct Entry<K: Ord + Default, V: Default> {
    pub key: K,
    pub value: V,
}

impl<K: Ord + Default, V: Default> Entry<K, V> {
    pub fn take_key_value(&mut self) -> (K, V) {
        (
            std::mem::take(&mut self.key),
            std::mem::take(&mut self.value),
        )
    }

    pub fn key_value(self) -> (K, V) {
        (self.key, self.value)
    }

    pub fn key_value_clone(&self) -> (K, V)
    where
        K: Clone,
        V: Clone,
    {
        (self.key.clone(), self.value.clone())
    }
}
