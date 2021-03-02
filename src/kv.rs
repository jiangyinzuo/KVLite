use crate::buffer::{BufReaderWithPos, BufWriterWithPos};
use crate::command::Command;
use crate::error::KVLiteError;
use crate::Result;
use serde_json::Deserializer;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

pub struct KvStore {
    reader: BufReaderWithPos<File>,
    writer: BufWriterWithPos<File>,
    index: HashMap<String, CommandPos>,
}

impl KvStore {
    /// Open a `KvStore` with the given path.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;
        let path = path.join("log.txt");

        let writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&path)?,
        )?;
        let mut reader = BufReaderWithPos::new(OpenOptions::new().read(true).open(&path)?)?;

        let mut index = HashMap::<String, CommandPos>::new();
        load(&mut reader, &mut index)?;
        Ok(KvStore {
            reader,
            writer,
            index,
        })
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key, value);
        let pos = self.writer.pos;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        if let Command::Set { key, .. } = cmd {
            let cmd_pos = CommandPos {
                pos,
                len: self.writer.pos - pos,
            };
            self.index.insert(key, cmd_pos);
        }

        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&mut self, key: &str) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(key) {
            let reader = &mut self.reader;
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let cmd_reader = reader.take(cmd_pos.len);
            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KVLiteError::InvalidCommand)
            }
        } else {
            Ok(None)
        }
    }

    /// Remove a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::remove(key);
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;
            if let Command::Remove { key } = cmd {
                self.index.remove(&key).expect("key not found");
            }
            Ok(())
        } else {
            Err(KVLiteError::KeyNotFound)
        }
    }
}

/// Record start position and length of a command
struct CommandPos {
    pos: u64,
    len: u64,
}

fn load(
    reader: &mut BufReaderWithPos<File>,
    index: &mut HashMap<String, CommandPos>,
) -> Result<()> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                index.insert(
                    key,
                    CommandPos {
                        pos,
                        len: new_pos - pos,
                    },
                );
            }
            Command::Remove { key } => {
                index.remove(&key);
            }
        }
        pos = new_pos;
    }
    Ok(())
}
