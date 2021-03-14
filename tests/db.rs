use kvlite::db::ACTIVE_SIZE_THRESHOLD;
use kvlite::db::{DBCommand, KVLite};
use kvlite::error::KVLiteError;
use kvlite::memory::{BTreeMemTable, MemTable, SkipMapMemTable};
use kvlite::Result;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_command() -> Result<()> {
    env_logger::init();

    _test_command::<BTreeMemTable>().unwrap();
    _test_command::<SkipMapMemTable>().unwrap();
    Ok(())
}

fn _test_command<M: 'static + MemTable>() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    // let db = KVLite::<M>::open("temp_test")?;
    let db = KVLite::<M>::open(temp_dir.path())?;
    db.set("hello".into(), "world".into())?;
    assert_eq!(
        KVLiteError::KeyNotFound,
        db.remove("no_exist".into()).unwrap_err()
    );
    assert_eq!("world", db.get(&"hello".to_owned())?.unwrap());
    db.remove("hello".into())?;
    assert!(db.get(&"hello".to_owned())?.is_none());

    for i in 0..ACTIVE_SIZE_THRESHOLD * 10 {
        db.set(format!("key{}", i), format!("value{}", i))?;
    }
    db.get(&"key3".to_string())?.unwrap();
    for i in 0..ACTIVE_SIZE_THRESHOLD * 10 {
        assert_eq!(
            format!("value{}", i),
            db.get(&format!("key{}", i))?.expect(&*format!("{}", i)),
            "kv {}",
            i
        );
    }
    Ok(())
}

// FIXME: no such file or directory
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_read_log() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    {
        let db = KVLite::<SkipMapMemTable>::open(temp_dir.path())?;
        for i in 0..ACTIVE_SIZE_THRESHOLD - 1 {
            db.set(format!("{}", i), format!("value{}", i))?;
        }
    }
    let db = Arc::new(KVLite::<BTreeMemTable>::open(temp_dir.path())?);

    for i in 0..ACTIVE_SIZE_THRESHOLD - 1 {
        assert_eq!(Some(format!("value{}", i)), db.get(&format!("{}", i))?);
    }
    for i in ACTIVE_SIZE_THRESHOLD..ACTIVE_SIZE_THRESHOLD + 30 {
        db.set(format!("{}", i), format!("value{}", i))?;
        assert_eq!(Some(format!("value{}", i)), db.get(&format!("{}", i))?);
    }
    let db = Arc::new(KVLite::<SkipMapMemTable>::open(temp_dir.path())?);
    let db1 = db.clone();
    let handle1 = std::thread::spawn(move || {
        for _ in 0..3 {
            for i in ACTIVE_SIZE_THRESHOLD..ACTIVE_SIZE_THRESHOLD + 30 {
                assert_eq!(
                    Some(format!("value{}", i)),
                    db1.get(&format!("{}", i)).unwrap()
                );
            }
        }
    });
    let handle2 = std::thread::spawn(move || {
        for _ in 0..3 {
            for i in ACTIVE_SIZE_THRESHOLD..ACTIVE_SIZE_THRESHOLD + 30 {
                assert_eq!(
                    Some(format!("value{}", i)),
                    db.get(&format!("{}", i)).unwrap()
                );
            }
        }
    });
    handle1.join().unwrap();
    handle2.join().unwrap();
    Ok(())
}
