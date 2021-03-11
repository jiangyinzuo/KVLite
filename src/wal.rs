use crate::command::WriteCommand;
use crate::Result;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::path::{Path, PathBuf};

/// Write Ahead Log Writer
pub struct WalWriter {
    log_path: String,
    writer: BufWriter<File>,
}

impl WalWriter {
    pub fn open(log_path: String) -> Result<WalWriter> {
        fs::create_dir_all(&log_path)?;

        let log_file = log_file(log_path.as_ref(), 0);

        let writer = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&log_file)?,
        );
        Ok(WalWriter { log_path, writer })
    }

    pub fn append(&mut self, cmd: &WriteCommand) -> Result<()> {
        serde_json::to_writer(&mut self.writer, cmd)?;
        Ok(())
    }
}

fn log_file(dir: &Path, log_id: u64) -> PathBuf {
    dir.join(format!("{}.log", log_id))
}
