use kvlite::db::ACTIVE_SIZE_THRESHOLD;
use kvlite::db::{DBCommand, KVLite};
use kvlite::error::KVLiteError;
use kvlite::memory::{BTreeMemTable, MemTable, SkipMapMemTable};
use kvlite::Result;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn test_command() {
    env_logger::try_init();
    _test_command::<BTreeMemTable>();
    std::thread::sleep(Duration::from_secs(2));
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
        db.set(format!("key{}", i), format!("value{}", i)).unwrap();
    }

    std::thread::sleep(Duration::from_secs(2));

    db.get(&"key3".to_string()).unwrap().unwrap();
    for i in 0..ACTIVE_SIZE_THRESHOLD * 10 {
        assert_eq!(
            format!("value{}", i),
            db.get(&format!("key{}", i))
                .unwrap()
                .expect(&*format!("{}", i)),
            "kv {}",
            i
        );
    }
}

#[test]
fn test_read_log() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let path = temp_dir.path();

    {
        let db = KVLite::<SkipMapMemTable>::open(path)?;
        for i in 0..ACTIVE_SIZE_THRESHOLD - 1 {
            db.set(format!("{}", i), format!("value{}", i))?;
        }
    }
    std::thread::sleep(Duration::from_secs(2));

    let db = KVLite::<BTreeMemTable>::open(path)?;

    for i in 0..ACTIVE_SIZE_THRESHOLD - 1 {
        assert_eq!(Some(format!("value{}", i)), db.get(&format!("{}", i))?);
    }
    for i in ACTIVE_SIZE_THRESHOLD..ACTIVE_SIZE_THRESHOLD + 30 {
        db.set(format!("{}", i), format!("value{}", i))?;
        assert_eq!(Some(format!("value{}", i)), db.get(&format!("{}", i))?);
    }
    std::thread::sleep(Duration::from_secs(2));

    let db = Arc::new(KVLite::<SkipMapMemTable>::open(path).unwrap());
    let db1 = db.clone();
    let handle1 = std::thread::spawn(move || {
        test_log(db);
    });
    let handle2 = std::thread::spawn(move || {
        test_log(db1);
    });
    handle1.join().unwrap();
    handle2.join().unwrap();
    Ok(())
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
