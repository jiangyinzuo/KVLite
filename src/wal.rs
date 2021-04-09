use crate::ioutils::{read_string_exact, read_u32, BufReaderWithPos};
use crate::memory::MemTable;
use crate::Result;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct WriteAheadLog {
    log_path: PathBuf,
    log0: File,
    log1: File,
}

impl WriteAheadLog {
    /// Open the logs at `db_path` and load to memory tables
    pub fn open_and_load_logs(
        db_path: &str,
        mut_mem_table: &mut impl MemTable,
    ) -> Result<WriteAheadLog> {
        let log_path = log_path(db_path.as_ref());
        fs::create_dir_all(&log_path)?;

        let imm_log = imm_log_file(log_path.as_ref());
        let mut_log = mut_log_file(log_path.as_ref());

        let log0 = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .append(true)
            .open(&imm_log)
            .unwrap();

        let log1 = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .append(true)
            .open(&mut_log)
            .unwrap();

        load_log(&log1, mut_mem_table).unwrap();
        load_log(&log0, mut_mem_table).unwrap();

        Ok(WriteAheadLog {
            log_path,
            log0,
            log1,
        })
    }

    /// Append a [WriteCommand] to `mut_log`
    pub fn append(&mut self, key: &String, value: Option<&String>) -> Result<()> {
        let key_length = (key.len() as u32).to_le_bytes();
        self.log1.write_all(&key_length)?;
        match value {
            Some(v) => {
                let value_length = (v.len() as u32).to_le_bytes();
                self.log1.write_all(&value_length)?;
                self.log1.write_all(key.as_bytes())?;
                self.log1.write_all(v.as_bytes())?;
            }
            None => {
                self.log1.write_all(&0u32.to_le_bytes())?;
                self.log1.write_all(key.as_bytes())?;
            }
        }
        self.log1.flush()?;
        Ok(())
    }

    pub fn clear_imm_log(&mut self) -> Result<()> {
        self.log0.set_len(0)?;
        Ok(())
    }

    pub fn freeze_mut_log(&mut self) -> Result<()> {
        std::mem::swap(&mut self.log0, &mut self.log1);
        self.log1.set_len(0)?;
        Ok(())
    }
}

fn log_path(db_path: &Path) -> PathBuf {
    db_path.join("log")
}

fn imm_log_file(dir: &Path) -> PathBuf {
    dir.join("0.log")
}

fn mut_log_file(dir: &Path) -> PathBuf {
    dir.join("1.log")
}

// load log to mem_table
fn load_log(file: &File, mem_table: &mut impl MemTable) -> Result<()> {
    let mut reader = BufReaderWithPos::new(file)?;
    reader.seek(SeekFrom::Start(0))?;
    while let Ok(key_length) = read_u32(&mut reader) {
        let value_length = read_u32(&mut reader)?;
        let key = read_string_exact(&mut reader, key_length)?;
        if value_length > 0 {
            let value = read_string_exact(&mut reader, value_length)?;
            mem_table.set(key, value)?;
        } else {
            mem_table.remove(key)?;
        }
    }
    reader.seek(SeekFrom::End(0))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::memory::{KeyValue, SkipMapMemTable};
    use crate::wal::WriteAheadLog;
    use tempfile::TempDir;

    #[test]
    fn test() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_str().unwrap();

        let mut mut_mem = SkipMapMemTable::default();
        let imm_mem = SkipMapMemTable::default();

        let mut wal = WriteAheadLog::open_and_load_logs(path, &mut mut_mem).unwrap();
        for i in 1..4 {
            mut_mem = SkipMapMemTable::default();
            for j in 0..100 {
                wal.append(&format!("{}key{}", i, j), Some(&format!("{}value{}", i, j)))
                    .unwrap();
                if (j & 1) == 1 {
                    wal.append(&format!("{}key{}", i, j), None).unwrap();
                }
            }
            wal = WriteAheadLog::open_and_load_logs(path, &mut mut_mem).unwrap();
            assert_eq!(100 * i, mut_mem.len());
        }
        wal.freeze_mut_log().unwrap();
        wal.clear_imm_log().unwrap();
        mut_mem = SkipMapMemTable::default();
        wal = WriteAheadLog::open_and_load_logs(path, &mut mut_mem).unwrap();
        assert!(mut_mem.is_empty());
    }
}
