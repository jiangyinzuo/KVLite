use kvlite::db::ACTIVE_SIZE_THRESHOLD;
use kvlite::db::{DBCommand, KVLite};
use kvlite::error::KVLiteError;
use kvlite::memory::BTreeMemTable;
use kvlite::Result;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_command() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    env_logger::init();
    let db = KVLite::<BTreeMemTable>::open(temp_dir.path())?;
    // let db = KVLite::<BTreeMemTable>::open("temp_test")?;

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
    db.get(&format!("key{}", 3))?.unwrap();
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
