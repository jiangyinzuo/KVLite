//! ```text
//! +-------------------+
//! | START_TRANSACTION | u64
//! +-------------------+
//! | LSN1              | u64
//! +-------------------+
//! | key1 length       | u64
//! +-------------------+
//! | value1 length     | u64
//! +-------------------+
//! | key1              | variant length
//! +-------------------+
//! | value1            | variant length
//! +-------------------+
//! | key2 length       |
//! +-------------------+
//! | value2 length     |
//! +-------------------+
//! | key2              |
//! +-------------------+
//! | value2            |
//! +-------------------+
//! | ...               |
//! +-------------------+
//! | END_TRANSACTION   | u64
//! +-------------------+
//! | LSN2              | transaction with single command can emit `START_TRANSACTION` and
//! +-------------------+ `END_TRANSACTION`
//! | key3 length       |
//! +-------------------+
//! | value3 length     |
//! +-------------------+
//! | key3              |
//! +-------------------+
//! | value3            |
//! +-------------------+
//! ```
use crate::db::key_types::{InternalKey, LSNKey, MemKey, LSN};
use crate::db::Value;
use crate::error::KVLiteError;
use crate::ioutils::{read_bytes_exact, read_u64, BufReaderWithPos};
use crate::memory::MemTable;
use crate::wal::{TransactionWAL, WALInner, WAL};
use crate::Result;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};

const START_TRANSACTION: u64 = u64::MAX;
const END_TRANSACTION: u64 = u64::MIN;

pub struct LSNWriteAheadLog {
    inner: WALInner,
}

impl<UK: MemKey> WAL<LSNKey<UK>, UK> for LSNWriteAheadLog {
    fn open_and_load_logs(
        db_path: &str,
        mut_mem_table: &mut impl MemTable<LSNKey<UK>, UK>,
    ) -> Result<Self> {
        let wal = LSNWriteAheadLog {
            inner: WALInner::open_logs(db_path)?,
        };
        Self::load_log(&wal.inner.log1, mut_mem_table).unwrap();
        Self::load_log(&wal.inner.log0, mut_mem_table).unwrap();
        Ok(wal)
    }

    fn load_log(file: &File, mem_table: &mut impl MemTable<LSNKey<UK>, UK>) -> Result<()> {
        let mut reader = BufReaderWithPos::new(file)?;
        reader.seek(SeekFrom::Start(0))?;
        while let Ok(lsn) = read_u64(&mut reader) {
            match lsn {
                START_TRANSACTION => {
                    let lsn = read_u64(&mut reader)?;
                    if lsn != START_TRANSACTION && lsn != END_TRANSACTION {
                        Self::load_kvs_in_lsn(lsn, &mut reader, mem_table)?;
                    } else {
                        return Err(KVLiteError::Custom(String::from("invalid log")));
                    }
                }
                END_TRANSACTION => return Err(KVLiteError::Custom(String::from("invalid log"))),
                lsn => {
                    let key_length = read_u64(&mut reader)?;
                    let value_length = read_u64(&mut reader)?;
                    let key: InternalKey = read_bytes_exact(&mut reader, key_length)?;
                    let lsn_key = LSNKey::new(UK::from(key), lsn);
                    if value_length > 0 {
                        let value = read_bytes_exact(&mut reader, value_length)?;
                        mem_table.set(lsn_key, value)?;
                    } else {
                        mem_table.remove(lsn_key)?;
                    }
                }
            }
        }
        reader.seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn append(&mut self, key: &LSNKey<UK>, value: Option<&Value>) -> Result<()> {
        let internal_key = key.internal_key();
        let key_length: [u8; 4] = (internal_key.len() as u32).to_le_bytes();
        self.inner.log1.write_all(&key_length)?;
        match value {
            Some(v) => {
                let value_length = (v.len() as u32).to_le_bytes();
                self.inner.log1.write_all(&value_length)?;
                self.inner.log1.write_all(internal_key)?;
                self.inner.log1.write_all(v)?;
            }
            None => {
                self.inner.log1.write_all(&0u32.to_le_bytes())?;
                self.inner.log1.write_all(internal_key)?;
            }
        }
        self.inner.log1.flush()?;
        self.inner.log1.sync_data()?;
        Ok(())
    }

    fn clear_imm_log(&mut self) -> Result<()> {
        self.inner.clear_imm_log()
    }

    fn freeze_mut_log(&mut self) -> Result<()> {
        self.inner.freeze_mut_log()
    }
}

impl<UK: MemKey> TransactionWAL<LSNKey<UK>, UK> for LSNWriteAheadLog {
    fn start_transaction(&mut self) -> Result<()> {
        let bytes = START_TRANSACTION.to_le_bytes();
        self.inner.log1.write_all(&bytes)?;
        Ok(())
    }

    fn end_transaction(&mut self) -> Result<()> {
        let bytes = END_TRANSACTION.to_le_bytes();
        self.inner.log1.write_all(&bytes)?;
        Ok(())
    }
}

impl LSNWriteAheadLog {
    fn load_kvs_in_lsn<UK: MemKey>(
        lsn: LSN,
        reader: &mut BufReaderWithPos<&File>,
        mem_table: &mut impl MemTable<LSNKey<UK>, UK>,
    ) -> Result<()> {
        while let Ok(key_length) = read_u64(reader) {
            match key_length {
                END_TRANSACTION => return Ok(()),
                START_TRANSACTION => return Err(KVLiteError::Custom(String::from("invalid log"))),
                key_length => {
                    let value_length = read_u64(reader)?;
                    let key: InternalKey = read_bytes_exact(reader, key_length)?;
                    let lsn_key = LSNKey::new(UK::from(key), lsn);
                    if value_length > 0 {
                        let value = read_bytes_exact(reader, value_length)?;
                        mem_table.set(lsn_key, value)?;
                    } else {
                        mem_table.remove(lsn_key)?;
                    }
                }
            }
        }
        Err(KVLiteError::Custom(String::from("invalid log")))
    }
}
