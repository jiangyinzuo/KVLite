use crate::command::WriteCommand;
use crate::Result;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// Write Ahead Log Writer
pub struct WalWriter {
    log_path: String,
    cur_log_file: PathBuf,
    next_log_id: u128,
    writer: BufWriter<File>,
    log_to_delete: VecDeque<PathBuf>,
}

impl WalWriter {
    pub fn open(log_path: String) -> Result<WalWriter> {
        let log_file = log_file(log_path.as_ref(), 0);

        let writer = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&log_file)?,
        );
        Ok(WalWriter {
            log_path,
            cur_log_file: log_file,
            next_log_id: 1,
            writer,
            log_to_delete: VecDeque::default(),
        })
    }

    pub fn append(&mut self, cmd: &WriteCommand) -> Result<()> {
        serde_json::to_writer(&mut self.writer, cmd)?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn new_log(&mut self) -> Result<()> {
        let new_log_path = log_file(self.log_path.as_ref(), self.next_log_id);
        self.writer = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&new_log_path)?,
        );

        let old_log_path = std::mem::replace(&mut self.cur_log_file, new_log_path);
        self.log_to_delete.push_back(old_log_path);
        Ok(())
    }

    pub fn remove_log(&mut self) -> Result<()> {
        if let Some(log_to_remove) = self.log_to_delete.pop_front() {
            std::fs::remove_file(log_to_remove)?;
        }
        Ok(())
    }
}

fn log_file(dir: &Path, log_id: u128) -> PathBuf {
    dir.join(format!("{}.log", log_id))
}
