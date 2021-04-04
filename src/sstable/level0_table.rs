use crate::memory::MemTable;
use crate::sstable::compact::Level0Compacter;
use crate::sstable::manager::TableManager;
use crate::wal::WriteAheadLog;
use crate::Result;
use crossbeam_channel::Receiver;
use std::ops::Deref;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::thread::JoinHandle;

/// Struct for read and write level0 sstable.
pub struct Level0Manager {
    db_path: String,

    table_manager: Arc<TableManager>,
    level0_compactor: Arc<Level0Compacter>,

    /// Table ID is increasing order.
    wal: Arc<Mutex<WriteAheadLog>>,
}

impl Level0Manager {
    fn new(
        db_path: String,
        table_manager: Arc<TableManager>,
        wal: Arc<Mutex<WriteAheadLog>>,
    ) -> Result<Level0Manager> {
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
        let level0_compactor = Arc::new(Level0Compacter::new(table_manager.clone()));
        Ok(Level0Manager {
            db_path,
            table_manager,
            level0_compactor,
            wal,
        })
    }

    /// Start a thread for writing immutable memory table to level0 sstable
    pub(crate) fn start_task_write_level0(
        db_path: String,
        table_manager: Arc<TableManager>,
        wal: Arc<Mutex<WriteAheadLog>>,
        imm_mem_table: Arc<RwLock<impl MemTable + 'static>>,
        recv: Receiver<()>,
    ) -> (Arc<Level0Manager>, JoinHandle<()>) {
        let manager = Arc::new(Self::new(db_path, table_manager, wal).unwrap());
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

    /// Persistently write the `table` to disk.
    fn write_to_table(&self, table: &impl MemTable) -> Result<()> {
        let handle = self.table_manager.create_table_write_handle(0);
        handle.write_sstable(table)?;
        self.table_manager.insert_table_handle(
            handle,
            table.first_key().unwrap().to_string(),
            table.last_key().unwrap().to_string(),
        );
        self.delete_imm_table_log()?;
        self.level0_compactor.may_compact();
        Ok(())
    }

    // delete immutable log after writing to level0 sstable
    fn delete_imm_table_log(&self) -> Result<()> {
        let mut wal_guard = self.wal.lock().unwrap();
        wal_guard.clear_imm_log()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::db::{DBCommandMut, ACTIVE_SIZE_THRESHOLD};
    use crate::memory::{KeyValue, SkipMapMemTable};
    use crate::sstable::level0_table::Level0Manager;
    use crate::sstable::manager::TableManager;
    use crate::wal::WriteAheadLog;
    use std::sync::{Arc, Mutex, RwLock};
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn test() {
        let _ = env_logger::try_init();

        let temp = TempDir::new().unwrap();
        let path = temp.path().to_str().unwrap().to_string();

        for i in 0..10 {
            test_query(path.clone(), i == 0);
            println!("test {} ok", i);
        }
    }

    fn test_query(path: String, insert_value: bool) {
        let table_manager = Arc::new(TableManager::open_tables(path.clone()));
        let mut mut_mem = SkipMapMemTable::default();
        let mut imm_mem = SkipMapMemTable::default();

        let (sender, receiver) = crossbeam_channel::unbounded();
        let wal = WriteAheadLog::open_and_load_logs(&path, &mut mut_mem, &mut imm_mem).unwrap();

        assert!(imm_mem.is_empty());
        assert!(mut_mem.is_empty());

        let imm_mem = Arc::new(RwLock::new(imm_mem));

        let (manager, handle) = Level0Manager::start_task_write_level0(
            path,
            table_manager.clone(),
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

        // wait for writing data
        std::thread::sleep(Duration::from_secs(1));

        for i in 0..ACTIVE_SIZE_THRESHOLD * 4 {
            let v = table_manager.query_tables(&format!("key{}", i)).unwrap();
            assert_eq!(format!("value{}", i), v.unwrap());
        }

        drop(sender);
        handle.join().unwrap();
    }
}
