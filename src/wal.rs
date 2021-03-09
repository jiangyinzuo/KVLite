use crate::command::WriteCmdOp;
use crate::command::WriteCommand;
use crate::Result;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

/// Write Ahead Log Writer
pub struct WalWriter {
    log_path: PathBuf,
    writer: BufWriter<File>,
}

impl WalWriter {
    pub fn open(log_path: impl Into<PathBuf>) -> Result<WalWriter> {
        let log_path = log_path.into();
        let writer = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&log_path)?,
        );
        Ok(WalWriter { log_path, writer })
    }

    pub fn append(&mut self, cmd: &WriteCommand) -> Result<()> {
        serde_json::to_writer(&mut self.writer, cmd)?;
        Ok(())
    }
}
