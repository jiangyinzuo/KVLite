use crate::ioutils::BufWriterWithPos;
use crate::memory::MemTable;
use crate::sstable::footer::Footer;
use crate::sstable::index_block::IndexBlock;
use crate::sstable::{query_sstable, sstable_file, MAX_BLOCK_KV_PAIRS};
use crate::wal::WriteAheadLog;
use crate::Result;
use crossbeam_channel::Receiver;
use std::collections::LinkedList;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::ops::Deref;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::thread::JoinHandle;

/// Struct for read and write level0 sstable.
pub struct Level0Manager {
    db_path: String,

    /// Table ID is increasing order.
    tables: RwLock<LinkedList<u128>>,
    wal: Arc<Mutex<WriteAheadLog>>,
}

impl Level0Manager {
    fn new(db_path: String, wal: Arc<Mutex<WriteAheadLog>>) -> Result<Level0Manager> {
        let dir = std::fs::read_dir(format!("{}/0", db_path))?;
        let mut tables: Vec<u128> = dir
            .map(|d| {
                let s = d
                    .unwrap()
                    .path()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();
                s.parse::<u128>().unwrap()
            })
            .collect();
        tables.sort_unstable();
        Ok(Level0Manager {
            db_path,
            tables: RwLock::new(tables.into_iter().collect::<LinkedList<u128>>()),
            wal,
        })
    }

    /// Start a thread for writing immutable memory table to level0 sstable
    pub fn start_task_write_level0(
        db_path: String,
        wal: Arc<Mutex<WriteAheadLog>>,
        imm_mem_table: Arc<RwLock<impl MemTable>>,
        recv: Receiver<()>,
    ) -> (Arc<Level0Manager>, JoinHandle<()>) {
        let manager = Arc::new(Self::new(db_path, wal).unwrap());
        let manager2 = manager.clone();
        let handle = thread::Builder::new()
            .name("level0 writer".to_owned())
            .spawn(move || {
                info!("thread `{}` start!", thread::current().name().unwrap());
                while let Ok(()) = recv.recv() {
                    let imm_guard = imm_mem_table.read().unwrap();
                    debug!("length of imm table: {}", imm_guard.len());
                    if let Err(e) = manager2.write_to_table(imm_guard.deref()) {
                        let bt = std::backtrace::Backtrace::capture();
                        error!(
                            "Error in thread `{}`: {:?}",
                            thread::current().name().unwrap(),
                            e
                        );
                        println!("{:#?}", bt);
                    }
                }
                info!("thread `{}` exit!", thread::current().name().unwrap());
            })
            .unwrap();
        (manager, handle)
    }

    pub fn query_level0_table(&self, key: &String) -> Result<Option<String>> {
        // query the latest table first
        let table_guard = self.tables.read().unwrap();
        for table in table_guard.iter().rev() {
            let option = query_sstable(&self.db_path, 0, *table, key);
            if option.is_some() {
                return Ok(option);
            }
        }
        Ok(None)
    }

    fn write_to_table(&self, table: &impl MemTable) -> Result<()> {
        let (mut writer, next_table_id) = self.create_table();

        let mut count = 0;
        let mut last_pos = 0;
        let mut index_block = IndexBlock::default();

        // write Data Blocks
        for (i, (k, v)) in table.iter().enumerate() {
            let (k, v) = (k.as_bytes(), v.as_bytes());
            let (k_len, v_len) = (k.len() as u32, v.len() as u32);

            // length of key | length of value | key | value
            writer.write_all(&k_len.to_le_bytes())?;
            writer.write_all(&v_len.to_le_bytes())?;
            writer.write_all(k)?;
            writer.write_all(v)?;
            if count == MAX_BLOCK_KV_PAIRS || i == table.len() - 1 {
                index_block.add_index(last_pos as u32, (writer.pos - last_pos) as u32, k);
                last_pos = writer.pos;
                count = 0;
            } else {
                count += 1;
            }
        }

        let index_block_offset = last_pos as u32;

        index_block.write_to_file(&mut writer)?;

        // write footer
        let footer = Footer {
            index_block_offset,
            index_block_length: writer.pos as u32 - index_block_offset,
        };
        footer.write_to_file(&mut writer)?;
        writer.flush()?;

        {
            let mut table_guard = self.tables.write().unwrap();
            table_guard.push_back(next_table_id);
        }

        {
            // delete immutable log after writing to level0 sstable
            let mut wal_guard = self.wal.lock().unwrap();
            wal_guard.clear_imm_log()?;
        }
        Ok(())
    }

    /// Create sstable file, return file writer and sstable ID
    fn create_table(&self) -> (BufWriterWithPos<File>, u128) {
        let table_guard = self.tables.read().unwrap();
        let next_table_id = table_guard.back().unwrap_or(&0) + 1;

        let file_path = sstable_file(&self.db_path, 0, next_table_id);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&file_path)
            .expect(&file_path);
        debug!("create sstable {}/{}", 0, next_table_id);
        (BufWriterWithPos::new(file).unwrap(), next_table_id)
    }
}

#[cfg(test)]
mod tests {
    use crate::db::{DBCommandMut, ACTIVE_SIZE_THRESHOLD};
    use crate::memory::{MemTable, SkipMapMemTable};
    use crate::sstable::level0_table::Level0Manager;
    use crate::wal::WriteAheadLog;
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn test() {
        env_logger::try_init();

        let temp = TempDir::new().unwrap();
        let path = temp.path().to_str().unwrap().to_string();

        for i in 0..10 {
            test_query(path.clone(), i == 0);
            info!("test {} ok", i);
        }
    }

    fn test_query(path: String, insert_value: bool) {
        let mut mut_mem = SkipMapMemTable::default();
        let mut imm_mem = SkipMapMemTable::default();

        std::fs::create_dir_all(format!("{}/0", path)).unwrap();
        let (sender, receiver) = crossbeam_channel::unbounded();
        let wal = WriteAheadLog::open_and_load_logs(&path, &mut mut_mem, &mut imm_mem).unwrap();

        assert!(imm_mem.is_empty());
        assert!(mut_mem.is_empty());

        let imm_mem = Arc::new(RwLock::new(imm_mem));

        let (manager, handle) = Level0Manager::start_task_write_level0(
            path,
            Arc::new(Mutex::new(wal)),
            imm_mem.clone(),
            receiver,
        );

        if insert_value {
            let mut imm_mem_guard = imm_mem.write().unwrap();
            for i in 0..ACTIVE_SIZE_THRESHOLD * 4 {
                imm_mem_guard
                    .set(format!("key{}", i), format!("value{}", i))
                    .unwrap();
            }
            sender.send(()).unwrap();
        }

        std::thread::sleep(Duration::from_secs(1));

        for i in 0..ACTIVE_SIZE_THRESHOLD * 4 {
            let v = manager.query_level0_table(&format!("key{}", i)).unwrap();
            assert_eq!(format!("value{}", i), v.unwrap());
        }

        std::thread::sleep(Duration::from_secs(1));
        drop(sender);
        handle.join().unwrap();
    }
}
