use crate::db::key_types::MemKey;
use crate::db::options::WriteOptions;
use crate::db::Value;
use crate::memory::MemTable;
use crate::Result;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::path::{Path, PathBuf};

pub mod lsn_wal;
pub mod simple_wal;

pub trait WAL<SK: MemKey, UK: MemKey>: Sized + Sync + Send {
    /// Open the logs at `db_path` and load to memory tables
    fn open_and_load_logs(db_path: &str, mut_mem_table: &mut impl MemTable<SK, UK>)
        -> Result<Self>;
    fn load_log(file: &File, mem_table: &mut impl MemTable<SK, UK>) -> Result<()>;

    /// Append a key-value pair to `mut_log`
    fn append(
        &mut self,
        write_options: &WriteOptions,
        key: &SK,
        value: Option<&Value>,
    ) -> Result<()>;

    fn clear_imm_log(&mut self) -> Result<()>;

    fn freeze_mut_log(&mut self) -> Result<()>;
}

pub trait TransactionWAL<SK: MemKey, UK: MemKey>: WAL<SK, UK> {
    fn start_transaction(&mut self) -> Result<()>;
    fn end_transaction(&mut self) -> Result<()>;
}

struct WALInner {
    log_path: PathBuf,
    log0: BufWriter<File>,
    log1: BufWriter<File>,
}

impl WALInner {
    fn open_logs(db_path: &str) -> Result<WALInner> {
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

        Ok(WALInner {
            log_path,
            log0: BufWriter::new(log0),
            log1: BufWriter::new(log1),
        })
    }

    fn clear_imm_log(&mut self) -> Result<()> {
        self.log0.get_mut().set_len(0)?;
        self.log0.get_mut().sync_data()?;
        Ok(())
    }

    fn freeze_mut_log(&mut self) -> Result<()> {
        std::mem::swap(&mut self.log0, &mut self.log1);
        self.log1.get_mut().set_len(0)?;
        self.log1.get_mut().sync_data()?;
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
