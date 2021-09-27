use crate::db::key_types::DBKey;
use crate::db::DB;
use crate::memory::MemTable;

pub(super) mod lock;
pub mod pessimistic_transaction_db;
pub mod transaction;

pub type TransactionID = u64;
pub const TRANSACTION_NULL: TransactionID = 0;

pub trait TransactionDB<SK: DBKey, UK: DBKey, M: MemTable<SK, UK>>: DB<SK, UK, M> {}
