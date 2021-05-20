use core::cmp::Ord;
use core::default::Default;
use core::marker::{Send, Sync};
use std::cmp::Ordering;
use std::convert::TryInto;

/// Key stored in memory table
pub trait MemKey:
    Ord + Send + Clone + Sync + Default + Into<InternalKey> + From<InternalKey>
{
    fn internal_key(&self) -> &InternalKey;
    fn user_cmp(&self, other: &Self) -> std::cmp::Ordering;
    fn user_eq(&self, other: &Self) -> bool {
        self.user_cmp(other) == std::cmp::Ordering::Equal
    }
}

/// Raw user key stored in disk
pub type InternalKey = Vec<u8>;

impl MemKey for InternalKey {
    fn internal_key(&self) -> &InternalKey {
        &self
    }

    #[inline]
    fn user_cmp(&self, other: &Self) -> Ordering {
        self.cmp(other)
    }
}

#[derive(Default, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub struct I32UserKey(i32, Vec<u8>);

unsafe impl Sync for I32UserKey {}

impl I32UserKey {
    pub fn new(num: i32) -> I32UserKey {
        I32UserKey(num, Vec::from(num.to_le_bytes()))
    }
}

impl Into<InternalKey> for I32UserKey {
    fn into(self) -> InternalKey {
        self.1
    }
}

impl From<LSNKey<I32UserKey>> for I32UserKey {
    fn from(lsn_key: LSNKey<I32UserKey>) -> Self {
        lsn_key.user_key
    }
}

impl From<InternalKey> for I32UserKey {
    fn from(ik: InternalKey) -> Self {
        let a: [u8; 4] = ik.clone().try_into().unwrap();
        let num = i32::from_le_bytes(a);
        I32UserKey(num, ik)
    }
}

impl MemKey for I32UserKey {
    fn internal_key(&self) -> &InternalKey {
        &self.1
    }

    fn user_cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

pub type LSN = u64;

/// User key with log sequence number(LSN)
#[derive(PartialEq, Eq, Default, Clone)]
pub struct LSNKey<K: MemKey> {
    user_key: K,
    lsn: LSN,
}

impl<K: MemKey> LSNKey<K> {
    pub fn new(user_key: K, lsn: LSN) -> LSNKey<K> {
        LSNKey { user_key, lsn }
    }

    pub fn upper_bound(lsn_key: &Self) -> Self {
        LSNKey::new(lsn_key.user_key.clone(), LSN::MAX)
    }

    pub fn user_key(&self) -> &K {
        &self.user_key
    }

    #[inline]
    pub fn lsn(&self) -> LSN {
        self.lsn
    }
}

impl<K: MemKey> PartialOrd for LSNKey<K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let user_key_order = self.user_key.partial_cmp(&other.user_key)?;
        match user_key_order {
            Ordering::Equal => self.lsn.partial_cmp(&other.lsn),
            o => Some(o),
        }
    }
}

impl<K: MemKey> Ord for LSNKey<K> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.user_key.cmp(&other.user_key) {
            Ordering::Equal => self.lsn.cmp(&other.lsn),
            o => o,
        }
    }
}

impl<K: MemKey> Into<InternalKey> for LSNKey<K> {
    fn into(self) -> InternalKey {
        self.user_key.into()
    }
}

impl<K: MemKey> From<InternalKey> for LSNKey<K> {
    fn from(ik: InternalKey) -> LSNKey<K> {
        LSNKey {
            user_key: K::from(ik),
            lsn: 0,
        }
    }
}

impl<K: MemKey> MemKey for LSNKey<K> {
    fn internal_key(&self) -> &InternalKey {
        self.user_key.internal_key()
    }

    fn user_cmp(&self, other: &Self) -> Ordering {
        todo!()
    }
}
