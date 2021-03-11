use crate::Result;
use std::fs::OpenOptions;
use std::io::{BufWriter, Seek, SeekFrom, Write};

/// The collection of all the Versions produced
pub struct Versions {
    db_path: String,
    next_sstable_id: u64,
}

impl Versions {
    pub fn new(db_path: String) -> Versions {
        Versions {
            db_path,
            next_sstable_id: 0,
        }
    }

    /// Persistently write the immutable memory table to level0 sstable.
    pub(crate) fn write_level0_sstable(
        &mut self,
        mem_table_iter: &mut dyn Iterator<Item = (&String, &String)>,
    ) -> Result<()> {
        let next_sstable_id = self.get_next_sstable_id();
        let sstable_path = sstable_path(&self.db_path, next_sstable_id);
        let mut writer = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&sstable_path)?,
        );
        writer.seek(SeekFrom::Start(0))?;

        for (k, v) in mem_table_iter {
            write!(writer, "{}{}", k, v)?;
        }
        Ok(())
    }

    fn get_next_sstable_id(&mut self) -> u64 {
        let id = self.next_sstable_id;
        self.next_sstable_id += 1;
        id
    }
}

fn sstable_path(db_path: &str, sstable_id: u64) -> String {
    format!("{}/{}.sst", db_path, sstable_id)
}

/// Store sstable's meta information, it should not be altered once created.
pub struct SSTableMeta {
    pub sstable_id: u64,
}
