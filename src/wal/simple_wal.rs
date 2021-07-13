use crate::db::key_types::{InternalKey, MemKey};
use crate::db::options::WriteOptions;
use crate::db::Value;
use crate::ioutils::{read_bytes_exact, read_u32, BufReaderWithPos};
use crate::memory::MemTable;
use crate::wal::{WALInner, WAL};
use crate::Result;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};

pub struct SimpleWriteAheadLog {
    inner: WALInner,
}

impl<UK: MemKey> WAL<InternalKey, UK> for SimpleWriteAheadLog {
    fn open_and_load_logs(
        db_path: &str,
        mut_mem_table: &mut impl MemTable<InternalKey, UK>,
    ) -> Result<SimpleWriteAheadLog> {
        let wal = SimpleWriteAheadLog {
            inner: WALInner::open_logs(db_path)?,
        };
        Self::load_log(wal.inner.log1.get_ref(), mut_mem_table).unwrap();
        Self::load_log(wal.inner.log0.get_ref(), mut_mem_table).unwrap();
        Ok(wal)
    }

    fn load_log(file: &File, mem_table: &mut impl MemTable<InternalKey, UK>) -> Result<()> {
        let mut reader = BufReaderWithPos::new(file)?;
        reader.seek(SeekFrom::Start(0))?;
        while let Ok(key_length) = read_u32(&mut reader) {
            let value_length = read_u32(&mut reader)?;
            let key = read_bytes_exact(&mut reader, key_length as u64)?;
            if value_length > 0 {
                let value = read_bytes_exact(&mut reader, value_length as u64)?;
                mem_table.set(key, value)?;
            } else {
                mem_table.remove(key)?;
            }
        }
        reader.seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn append(
        &mut self,
        write_options: &WriteOptions,
        key: &InternalKey,
        value: Option<&Value>,
    ) -> Result<()> {
        let key_length: [u8; 4] = (key.len() as u32).to_le_bytes();
        self.inner.log1.write_all(&key_length)?;
        match value {
            Some(v) => {
                let value_length = (v.len() as u32).to_le_bytes();
                self.inner.log1.write_all(&value_length)?;
                self.inner.log1.write_all(key)?;
                self.inner.log1.write_all(v)?;
            }
            None => {
                self.inner.log1.write_all(&0u32.to_le_bytes())?;
                self.inner.log1.write_all(key)?;
            }
        }
        self.inner.log1.flush()?;
        if write_options.sync {
            self.inner.log1.get_mut().sync_data()?;
        }
        Ok(())
    }

    fn clear_imm_log(&mut self) -> Result<()> {
        self.inner.clear_imm_log()
    }

    fn freeze_mut_log(&mut self) -> Result<()> {
        self.inner.freeze_mut_log()
    }
}

#[cfg(test)]
mod tests {
    use crate::db::key_types::InternalKey;
    use crate::db::options::WriteOptions;
    use crate::memory::{InternalKeyValueIterator, SkipMapMemTable};
    use crate::wal::simple_wal::SimpleWriteAheadLog;
    use crate::wal::WAL;
    use tempfile::TempDir;

    #[test]
    fn test() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_str().unwrap();

        let mut mut_mem = SkipMapMemTable::<InternalKey>::default();

        let mut wal: SimpleWriteAheadLog =
            SimpleWriteAheadLog::open_and_load_logs(path, &mut mut_mem).unwrap();
        assert!(mut_mem.is_empty());
        let wo = WriteOptions { sync: false };
        for i in 1..4 {
            mut_mem = SkipMapMemTable::default();
            for j in 0..100 {
                <SimpleWriteAheadLog as WAL<InternalKey, InternalKey>>::append(
                    &mut wal,
                    &wo,
                    &format!("{}key{}", i, j).into_bytes(),
                    Some(&format!("{}value{}", i, j).into_bytes()),
                )
                .unwrap();
                if (j & 1) == 1 {
                    <SimpleWriteAheadLog as WAL<InternalKey, InternalKey>>::append(
                        &mut wal,
                        &wo,
                        &format!("{}key{}", i, j).into_bytes(),
                        None,
                    )
                    .unwrap();
                }
            }
            wal = SimpleWriteAheadLog::open_and_load_logs(path, &mut mut_mem).unwrap();
            assert_eq!(100 * i, mut_mem.len());
        }
        <SimpleWriteAheadLog as WAL<InternalKey, InternalKey>>::freeze_mut_log(&mut wal).unwrap();
        <SimpleWriteAheadLog as WAL<InternalKey, InternalKey>>::clear_imm_log(&mut wal).unwrap();
        mut_mem = SkipMapMemTable::default();
        wal = SimpleWriteAheadLog::open_and_load_logs(path, &mut mut_mem).unwrap();
        assert!(mut_mem.is_empty());
    }
}
