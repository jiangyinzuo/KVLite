use core::cmp::Ord;
use core::default::Default;
use core::marker::{Send, Sync};
use std::cmp::Ordering;

/// Key stored in memory table
pub trait MemKey: Ord + Send + Sync + Default {
    fn user_key(&self) -> &UserKey;
    fn into_user_key(self) -> UserKey;
    fn restore_from_log(key: UserKey) -> Self;
}

/// Raw user key
pub type UserKey = Vec<u8>;

impl MemKey for UserKey {
    fn user_key(&self) -> &UserKey {
        &self
    }

    fn into_user_key(self) -> UserKey {
        self
    }

    #[inline]
    fn restore_from_log(key: UserKey) -> Self {
        key
    }
}

pub type LSN = u64;

/// User key with log sequence number(LSN)
#[derive(PartialEq, Eq, Default)]
pub struct LSNKey {
    user_key: UserKey,
    lsn: LSN,
}

impl LSNKey {
    pub fn new(user_key: UserKey, lsn: LSN) -> LSNKey {
        LSNKey { user_key, lsn }
    }
}

impl PartialOrd for LSNKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let user_key_order = self.user_key.partial_cmp(&other.user_key)?;
        match user_key_order {
            Ordering::Equal => self.lsn.partial_cmp(&other.lsn),
            o => Some(o),
        }
    }
}

impl Ord for LSNKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.user_key.cmp(other.user_key()) {
            Ordering::Equal => self.lsn.cmp(&other.lsn),
            o => o,
        }
    }
}

impl MemKey for LSNKey {
    fn user_key(&self) -> &UserKey {
        &self.user_key
    }

    fn into_user_key(self) -> UserKey {
        self.user_key
    }

    fn restore_from_log(key: UserKey) -> Self {
        LSNKey {
            user_key: key,
            lsn: 0,
        }
    }
}
