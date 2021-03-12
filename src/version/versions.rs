use crate::buffer::BufWriterWithPos;
use crate::sstable::footer::Footer;
use crate::sstable::index_block::IndexBlock;
use crate::Result;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

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
    pub fn write_level0_sstable(
        &mut self,
        mem_table_iter: &mut dyn Iterator<Item = (&String, &String)>,
    ) -> Result<()> {
        let next_sstable_id = self.get_next_sstable_id();
        let sstable_path = sstable_path(&self.db_path, 0, next_sstable_id);
        let mut writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(&sstable_path)?,
        )?;
        writer.seek(SeekFrom::Start(0))?;

        let mut last_pos = 0;
        let mut index_block = IndexBlock::default();

        // write Data Blocks
        for (k, v) in mem_table_iter {
            let (k, v) = (k.as_bytes(), v.as_bytes());
            let (k_len, v_len) = (k.len() as u32, v.len() as u32);
            writer.write_all(&k_len.to_be_bytes())?;
            writer.write_all(&v_len.to_be_bytes())?;
            writer.write_all(k)?;
            writer.write_all(v)?;
            if writer.pos - last_pos >= 2 << 12 {
                index_block.add_index(writer.pos as u32, (writer.pos - last_pos) as u32, k);
                last_pos = writer.pos;
            }
        }

        let footer = Footer::new(last_pos as u32);
        index_block.write_to_file(&mut writer)?;

        // write footer
        footer.write_to_file(&mut writer)?;
        Ok(())
    }

    pub fn load_footer(&mut self) {}

    fn get_next_sstable_id(&mut self) -> u64 {
        let id = self.next_sstable_id;
        self.next_sstable_id += 1;
        id
    }
}

fn sstable_path(db_path: &str, level: i32, sstable_id: u64) -> String {
    format!("{}/{}/{}.sst", db_path, level, sstable_id)
}

/// Store sstable's meta information, it should not be altered once created.
pub struct SSTableMeta {
    pub sstable_id: u64,
}
