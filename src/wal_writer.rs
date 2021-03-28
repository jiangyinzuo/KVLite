use crate::command::WriteCommand;
use crate::ioutils::BufReaderWithPos;
use crate::memory::MemTable;
use crate::Result;
use serde_json::Deserializer;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct WriteAheadLog {
    log_path: PathBuf,
    imm_log: File,
    mut_log: File,
}

impl WriteAheadLog {
    /// Open the logs at `db_path` and load to memory tables
    pub fn open_and_load_logs(
        db_path: &str,
        mut_mem_table: &mut impl MemTable,
        imm_mem_table: &mut impl MemTable,
    ) -> Result<WriteAheadLog> {
        let log_path = log_path(db_path.as_ref());
        fs::create_dir_all(&log_path)?;

        let imm_log = imm_log_file(log_path.as_ref());
        let mut_log = mut_log_file(log_path.as_ref());

        let imm_log = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .append(true)
            .open(&imm_log)
            .unwrap();

        let mut_log = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .append(true)
            .open(&mut_log)
            .unwrap();

        load_log(&mut_log, mut_mem_table)?;
        load_log(&mut_log, imm_mem_table)?;

        Ok(WriteAheadLog {
            log_path,
            imm_log,
            mut_log,
        })
    }

    /// Append a [WriteCommand] to `mut_log`
    pub fn append(&mut self, cmd: &WriteCommand) -> Result<()> {
        serde_json::to_writer(&mut self.mut_log, cmd)?;
        self.mut_log.flush()?;
        Ok(())
    }

    pub fn clear_imm_log(&mut self) -> Result<()> {
        self.imm_log.set_len(0)?;
        Ok(())
    }

    pub fn freeze_mut_log(&mut self) -> Result<()> {
        std::mem::swap(&mut self.imm_log, &mut self.mut_log);
        self.mut_log.set_len(0)?;
        Ok(())
    }
}

fn log_path(db_path: &Path) -> PathBuf {
    db_path.join("log")
}

fn imm_log_file(dir: &Path) -> PathBuf {
    dir.join("imm.log")
}

fn mut_log_file(dir: &Path) -> PathBuf {
    dir.join("mut.log")
}

// load log to mem_table
fn load_log(file: &File, mem_table: &mut impl MemTable) -> Result<()> {
    let mut reader = BufReaderWithPos::new(file)?;
    reader.seek(SeekFrom::Start(0))?;
    let stream = Deserializer::from_reader(reader).into_iter::<WriteCommand>();
    for cmd in stream {
        match cmd? {
            WriteCommand::Set { key, value } => {
                mem_table.set(key.to_string(), value.to_string())?;
            }
            WriteCommand::Remove { key } => {
                mem_table.remove(key.to_string())?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::command::WriteCommand;
    use crate::memory::{MemTable, SkipMapMemTable};
    use crate::wal_writer::WriteAheadLog;
    use tempfile::TempDir;

    #[test]
    fn test() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_str().unwrap();

        let mut mut_mem = SkipMapMemTable::default();
        let mut imm_mem = SkipMapMemTable::default();

        let mut wal = WriteAheadLog::open_and_load_logs(path.clone(), &mut mut_mem, &mut imm_mem).unwrap();
        for i in 1..4 {
            mut_mem = SkipMapMemTable::default();
            for j in 0..100 {
                wal.append(&WriteCommand::set(
                    format!("{}key{}", i, j),
                    format!("{}value{}", i, j),
                ))
                .unwrap();
            }
            wal = WriteAheadLog::open_and_load_logs(path, &mut mut_mem, &mut imm_mem).unwrap();
            assert_eq!(100 * i, mut_mem.len());
        }
        wal.freeze_mut_log().unwrap();
        wal.clear_imm_log().unwrap();
        mut_mem = SkipMapMemTable::default();
        wal = WriteAheadLog::open_and_load_logs(path, &mut mut_mem, &mut imm_mem).unwrap();
        assert!(mut_mem.is_empty());
    }
}
