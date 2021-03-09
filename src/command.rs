use serde::{Deserialize, Serialize};

/// Write Command
#[derive(Serialize, Deserialize, Debug)]
pub enum WriteCommand<'a> {
    Set { key: &'a str, value: &'a str },
    Remove { key: &'a str },
}

impl<'a> WriteCommand<'a> {
    pub fn set(key: &'a str, value: &'a str) -> WriteCommand<'a> {
        WriteCommand::Set { key, value }
    }

    pub fn remove(key: &'a str) -> WriteCommand<'a> {
        WriteCommand::Remove { key }
    }
}

/// Process "set" and "rm" commands.
pub trait WriteCmdOp {
    fn set(&mut self, key: String, value: String) -> crate::Result<()>;

    fn remove(&mut self, key: &str) -> crate::Result<()>;
}

/// Process "get" command
pub trait ReadCmdOp {
    fn get(&mut self, key: &str) -> crate::Result<&String>;
}
