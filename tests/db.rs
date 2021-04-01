use kvlite::db::KVLite;
use kvlite::db::ACTIVE_SIZE_THRESHOLD;
use kvlite::error::KVLiteError;
use kvlite::memory::{BTreeMemTable, MemTable, SkipMapMemTable};
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn test_command() {
    let _ = env_logger::try_init();
    _test_command::<BTreeMemTable>();
    _test_command::<SkipMapMemTable>();
}

fn _test_command<M: 'static + MemTable>() {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let db = KVLite::<M>::open(temp_dir.path()).unwrap();
    db.set("hello".into(), "world".into()).unwrap();
    assert_eq!(
        KVLiteError::KeyNotFound,
        db.remove("no_exist".into()).unwrap_err()
    );
    assert_eq!("world", db.get(&"hello".to_owned()).unwrap().unwrap());
    db.remove("hello".into()).unwrap();

    let v = db.get(&"hello".to_owned()).unwrap();
    assert!(v.is_none(), "{:?}", v);

    for i in 0..ACTIVE_SIZE_THRESHOLD * 10 {
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
    for i in 0..ACTIVE_SIZE_THRESHOLD * 10 {
        let v = db.get(&format!("key{}", i));
        let value = v.unwrap();
        assert_eq!(format!("value{}", i), value.unwrap(), "kv {}", i);
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

    let db = Arc::new(KVLite::<SkipMapMemTable>::open(path).unwrap());
    let db1 = db.clone();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let handle = std::thread::spawn(|| {
        test_log(db);
    });
    let handle2 = std::thread::spawn(|| {
        test_log(db1);
    });
    handle.join().unwrap();
    handle2.join().unwrap();
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
