use core::cmp::Ord;
use core::default::Default;
use core::marker::{Send, Sync};
use std::cmp::Ordering;
use std::convert::TryInto;

pub trait DBKey: Ord + Send + Clone + Sync + Default + Into<RawUserKey> + From<RawUserKey> {
    fn raw_user_key(&self) -> &RawUserKey;
    fn mem_size(&self) -> usize;
}

/// Raw user key stored in disk
pub type RawUserKey = Vec<u8>;

impl DBKey for RawUserKey {
    fn raw_user_key(&self) -> &RawUserKey {
        self
    }

    fn mem_size(&self) -> usize {
        self.len() * std::mem::size_of::<u8>()
    }
}

impl<K: DBKey> From<SeqNumKey<K>> for RawUserKey {
    fn from(lsn_key: SeqNumKey<K>) -> Self {
        lsn_key.user_key.into()
    }
}

#[derive(Default, Ord, PartialOrd, Clone)]
pub struct I32UserKey(i32, Vec<u8>);

unsafe impl Sync for I32UserKey {}

impl I32UserKey {
    pub fn new(num: i32) -> I32UserKey {
        I32UserKey(num, Vec::from(num.to_le_bytes()))
    }
}

impl PartialEq for I32UserKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Eq for I32UserKey {}

impl From<I32UserKey> for RawUserKey {
    fn from(key: I32UserKey) -> Self {
        key.1
    }
}

impl From<SeqNumKey<I32UserKey>> for I32UserKey {
    fn from(seq_num_key: SeqNumKey<I32UserKey>) -> Self {
        seq_num_key.user_key
    }
}

impl From<RawUserKey> for I32UserKey {
    fn from(ik: RawUserKey) -> Self {
        let a: [u8; 4] = ik.clone().try_into().unwrap();
        let num = i32::from_le_bytes(a);
        I32UserKey(num, ik)
    }
}

impl DBKey for I32UserKey {
    fn raw_user_key(&self) -> &RawUserKey {
        &self.1
    }

    fn mem_size(&self) -> usize {
        4 + 4
    }
}

pub type SequenceNumber = u64;

/// User key with log sequence number(LSN)
#[derive(PartialEq, Eq, Default, Clone)]
pub struct SeqNumKey<UK: DBKey> {
    user_key: UK,
    seq_num: SequenceNumber,
}

impl<K: DBKey> SeqNumKey<K> {
    pub fn new(user_key: K, seq_num: SequenceNumber) -> SeqNumKey<K> {
        SeqNumKey { user_key, seq_num }
    }

    pub fn upper_bound(seq_num_key: &Self) -> Self {
        SeqNumKey::new(seq_num_key.user_key.clone(), SequenceNumber::MAX)
    }

    pub fn user_key(&self) -> &K {
        &self.user_key
    }

    #[inline]
    pub fn seq_num(&self) -> SequenceNumber {
        self.seq_num
    }

    #[inline]
    pub fn set_seq_num(&mut self, seq_num: SequenceNumber) {
        self.seq_num = seq_num;
    }
}

impl<K: DBKey> PartialOrd for SeqNumKey<K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let user_key_order = self.user_key.partial_cmp(&other.user_key)?;
        match user_key_order {
            Ordering::Equal => self.seq_num.partial_cmp(&other.seq_num),
            o => Some(o),
        }
    }
}

impl<K: DBKey> Ord for SeqNumKey<K> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.user_key.cmp(&other.user_key) {
            Ordering::Equal => self.seq_num.cmp(&other.seq_num),
            o => o,
        }
    }
}

impl<K: DBKey> From<RawUserKey> for SeqNumKey<K> {
    fn from(ik: RawUserKey) -> SeqNumKey<K> {
        SeqNumKey {
            user_key: K::from(ik),
            seq_num: 0,
        }
    }
}

impl<K: DBKey> DBKey for SeqNumKey<K> {
    fn raw_user_key(&self) -> &RawUserKey {
        self.user_key.raw_user_key()
    }

    fn mem_size(&self) -> usize {
        self.user_key.mem_size() + std::mem::size_of::<SequenceNumber>()
    }
}
