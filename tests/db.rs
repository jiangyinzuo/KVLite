use kvlite::db::KVLite;
use kvlite::db::ACTIVE_SIZE_THRESHOLD;
use kvlite::error::KVLiteError;
use kvlite::memory::{BTreeMemTable, MemTable, SkipMapMemTable};
use kvlite::sstable::manager::level_n::LevelNManager;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{Arc, Barrier};
use tempfile::TempDir;

const TEST_CMD_TIMES: usize = 20;

#[test]
fn test_command() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let path = temp_dir.path();
    let _ = env_logger::try_init();
    _test_command::<BTreeMemTable>(path);
    _test_command::<SkipMapMemTable>(path);
}

fn _test_command<M: 'static + MemTable>(path: &Path) {
    let db = KVLite::<M>::open(path).unwrap();
    db.set("hello".into(), "world".into()).unwrap();
    assert_eq!(
        KVLiteError::KeyNotFound,
        db.remove("no_exist".into()).unwrap_err()
    );
    assert_eq!("world", db.get(&"hello".to_owned()).unwrap().unwrap());
    db.remove("hello".into()).unwrap();

    let v = db.get(&"hello".to_owned()).unwrap();
    assert!(v.is_none(), "{:?}", v);

    for i in 0..ACTIVE_SIZE_THRESHOLD * TEST_CMD_TIMES {
        db.set(
            format!("key{}", if i < ACTIVE_SIZE_THRESHOLD { 0 } else { i }),
            format!("value{}", i),
        )
        .unwrap();
    }
    for i in 0..ACTIVE_SIZE_THRESHOLD {
        db.set(format!("key{}", i), format!("value{}", i)).unwrap();
    }

    db.get(&"key3".to_string()).unwrap().unwrap();

    for i in 0..ACTIVE_SIZE_THRESHOLD * TEST_CMD_TIMES {
        let v = db.get(&format!("key{}", i));
        let value = v.unwrap();
        assert_eq!(
            format!("value{}", i),
            value.unwrap_or_else(|| panic!("kv{}", i)),
            "kv {}",
            i
        );
    }

    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let leveln_manager = LevelNManager::open_tables(db.db_path().clone(), rt);
    let lock = leveln_manager.get_level_tables_lock(unsafe { NonZeroUsize::new_unchecked(1) });
    let read_guard = lock.read().unwrap();
    let mut last_min_key = "";
    let mut last_max_key = "";
    for (_, table) in read_guard.iter() {
        let (min_key, max_key) = table.min_max_key();
        assert!(
            last_max_key.to_string().lt(min_key),
            "last_max_key: {}, min_key: {}",
            last_max_key,
            min_key
        );
        assert!(min_key <= max_key);
        last_min_key = min_key;
        last_max_key = max_key;
    }
}

#[test]
fn test_read_log() {
    let temp_dir = tempfile::Builder::new()
        .prefix("read_log")
        .tempdir()
        .unwrap();
    let path = temp_dir.path();

    let db = KVLite::<SkipMapMemTable>::open(path).unwrap();
    for i in 0..ACTIVE_SIZE_THRESHOLD - 1 {
        db.set(format!("{}", i), format!("value{}", i)).unwrap();
    }
    drop(db);

    let db = KVLite::<BTreeMemTable>::open(path).unwrap();

    for i in 0..ACTIVE_SIZE_THRESHOLD - 1 {
        assert_eq!(
            Some(format!("value{}", i)),
            db.get(&format!("{}", i)).unwrap()
        );
    }
    for i in ACTIVE_SIZE_THRESHOLD..ACTIVE_SIZE_THRESHOLD + 30 {
        db.set(format!("{}", i), format!("value{}", i)).unwrap();
        assert_eq!(
            Some(format!("value{}", i)),
            db.get(&format!("{}", i)).unwrap()
        );
    }

    drop(db);
    let db = Arc::new(KVLite::<SkipMapMemTable>::open(path).unwrap());
    for _ in 0..4 {
        test_log(db.clone());
    }

    let thread_cnt = 4;

    for _ in 0..10 {
        let barrier = Arc::new(Barrier::new(thread_cnt));
        let mut handles = vec![];
        for _ in 0..thread_cnt {
            let db = db.clone();
            let barrier = barrier.clone();
            let handle = std::thread::spawn(move || {
                barrier.wait();
                test_log(db);
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }
}

fn test_log<M: MemTable + 'static>(db: Arc<KVLite<M>>) {
    for _ in 0..3 {
        for i in ACTIVE_SIZE_THRESHOLD..ACTIVE_SIZE_THRESHOLD + 30 {
            assert_eq!(
                Some(format!("value{}", i)),
                db.get(&format!("{}", i)).expect("error in read thread1")
            );
        }
    }
}
