//! [https://en.wikipedia.org/wiki/Treap]

use crate::collections::Entry;
use std::cmp::Ordering;
use std::mem;

pub struct TreapNode<K: Ord + Default, V: Default> {
    entry: Entry<K, V>,
    priority: u32,
    left_son: Option<Box<TreapNode<K, V>>>,
    right_son: Option<Box<TreapNode<K, V>>>,
}

impl<K: Ord + Default, V: Default> TreapNode<K, V> {
    fn new(key: K, value: V) -> TreapNode<K, V> {
        TreapNode {
            entry: Entry { key, value },
            priority: rand::random(),
            left_son: None,
            right_son: None,
        }
    }

    //     pessimistic               q
    //    / \             / \
    //   A  q   --->     pessimistic  C
    //     / \          / \
    //    B  C         A  B
    fn left_rotate(&mut self) {
        // Cut right subtree of pessimistic
        let right = mem::replace(&mut self.right_son, None);
        if let Some(mut node) = right {
            // Let subtree q be root and `node` point to pessimistic
            mem::swap(self, &mut *node);
            // Move subtree B from q to right subtree of pessimistic
            mem::swap(&mut self.left_son, &mut node.right_son);
            // Let pessimistic be left child of q
            self.left_son = Some(node);
        }
    }

    //       q               pessimistic
    //      / \             / \
    //     pessimistic  C   --->     A  q
    //    / \                / \
    //   A  B               B  C
    fn right_rotate(&mut self) {
        // Cut left subtree of q
        let left = mem::replace(&mut self.left_son, None);
        if let Some(mut node) = left {
            // Let subtree pessimistic be root and `node` point to q
            mem::swap(self, &mut *node);
            // Move subtree B from pessimistic to left subtree of q
            mem::swap(&mut self.right_son, &mut node.left_son);
            // Let q be right child of pessimistic
            self.right_son = Some(node);
        }
    }

    // rotate to maintain min heap
    fn may_rotate(&mut self) {
        if let Some(left_son) = &self.left_son {
            if self.priority > left_son.priority {
                self.right_rotate();
            }
        }
        if let Some(right_son) = &self.right_son {
            if self.priority > right_son.priority {
                self.left_rotate();
            }
        }
    }

    fn insert_or_replace(&mut self, key: K, value: V) {
        match self.entry.key.cmp(&key) {
            Ordering::Equal => {
                self.entry.value = value;
            }
            Ordering::Greater => match &mut self.left_son {
                Some(son) => son.insert_or_replace(key, value),
                None => {
                    self.left_son = Some(Box::new(TreapNode::new(key, value)));
                }
            },
            Ordering::Less => match &mut self.right_son {
                Some(son) => son.insert_or_replace(key, value),
                None => {
                    self.right_son = Some(Box::new(TreapNode::new(key, value)));
                }
            },
        }
        self.may_rotate();
    }

    fn get(&self, key: &K) -> Option<&V> {
        match self.entry.key.cmp(key) {
            Ordering::Equal => Some(&self.entry.value),
            Ordering::Less => match &self.right_son {
                Some(son) => son.get(key),
                None => None,
            },
            Ordering::Greater => match &self.left_son {
                Some(son) => son.get(key),
                None => None,
            },
        }
    }

    fn remove(node: &mut Option<Box<TreapNode<K, V>>>, key: &K) -> Option<V> {
        match node {
            Some(n) => match n.entry.key.cmp(key) {
                Ordering::Equal => {}
                Ordering::Less => return Self::remove(&mut n.right_son, key),
                Ordering::Greater => return Self::remove(&mut n.left_son, key),
            },
            None => return None,
        }
        TreapNode::rotate_down(node)
    }

    fn rotate_down(node: &mut Option<Box<TreapNode<K, V>>>) -> Option<V> {
        enum RotateCase {
            Left,
            Right,
        }

        let rotate_case = match node {
            None => return None,
            Some(n) => match (&n.left_son, &n.right_son) {
                (None, None) => return node.take().map(|v| v.entry.value),
                (Some(l), Some(r)) => {
                    if l.priority < r.priority {
                        RotateCase::Right
                    } else {
                        RotateCase::Left
                    }
                }
                (Some(_), None) => RotateCase::Right,
                (None, Some(_)) => RotateCase::Left,
            },
        };

        match rotate_case {
            RotateCase::Left => node.as_mut().and_then(|n| {
                n.left_rotate();
                Self::rotate_down(&mut n.left_son)
            }),
            RotateCase::Right => node.as_mut().and_then(|n| {
                n.right_rotate();
                Self::rotate_down(&mut n.right_son)
            }),
        }
    }
}

pub struct TreapMap<K: Ord + Default, V: Default> {
    root: Option<Box<TreapNode<K, V>>>,
    len: usize,
}

impl<K: Ord + Default, V: Default> TreapMap<K, V> {
    pub fn new() -> TreapMap<K, V> {
        TreapMap { root: None, len: 0 }
    }

    pub fn insert(&mut self, key: K, value: V) {
        match &mut self.root {
            Some(root) => TreapNode::insert_or_replace(root, key, value),
            None => self.root = Some(Box::new(TreapNode::new(key, value))),
        }
        self.len += 1;
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        match &self.root {
            Some(root) => root.get(key),
            None => None,
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        TreapNode::remove(&mut self.root, key)
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl<K: Ord + Default, V: Default> Default for TreapMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::collections::treap::TreapMap;

    #[test]
    fn test() {
        let mut treap = TreapMap::new();
        treap.insert(3, 3);
        let value = treap.get(&3).unwrap();
        assert_eq!(3, *value);
        for i in 50..100i32 {
            treap.insert(i, i + 1);
            assert_eq!(treap.len(), 1 + i as usize - 49);
        }
        for i in 30..60 {
            treap.insert(i, i);
        }
        for i in 30..60 {
            let value = treap.get(&i).unwrap();
            assert_eq!(i, *value);
            let value = treap.remove(&i).unwrap();
            assert_eq!(i, value);
        }
        for i in 60..100 {
            let value = treap.get(&i).unwrap();
            assert_eq!(i + 1, *value);
            let value = treap.remove(&i).unwrap();
            assert_eq!(i + 1, value);
        }
        assert!(treap.get(&-3i32).is_none());
    }
}
