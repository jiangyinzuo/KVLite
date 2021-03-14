use serde::{Deserialize, Serialize};

/// Write Command
#[derive(Serialize, Deserialize, Debug)]
pub enum WriteCommand {
    Set { key: String, value: String },
    Remove { key: String },
}

impl WriteCommand {
    pub fn set(key: String, value: String) -> WriteCommand {
        WriteCommand::Set { key, value }
    }

    pub fn remove(key: String) -> WriteCommand {
        WriteCommand::Remove { key }
    }
}
