use crate::db::key_types::InternalKey;
use crate::db::Value;

pub type InternalKeyValue = (InternalKey, Value);

pub struct DBIterator {}
