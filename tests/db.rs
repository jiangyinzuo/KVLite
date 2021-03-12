use kvlite::config::ACTIVE_SIZE_THRESHOLD;
use kvlite::db::{DBCommand, KVLite};
use kvlite::error::KVLiteError;
use kvlite::memory::BTreeMemTable;
use kvlite::Result;
use std::time;
use tempfile::TempDir;

#[test]
fn test_command() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    env_logger::init();
    // let db = KVLite::<BTreeMemTable>::open(temp_dir.path())?;
    let db = KVLite::<BTreeMemTable>::open("temp_test")?;

    db.set("hello".into(), "world".into())?;
    assert_eq!(
        KVLiteError::KeyNotFound,
        db.remove("no_exist".into()).unwrap_err()
    );
    assert_eq!("world", db.get(&"hello".to_owned())?.unwrap());
    db.remove("hello".into())?;
    assert!(db.get(&"hello".to_owned())?.is_none());

    for i in 0..ACTIVE_SIZE_THRESHOLD * 3 {
        db.set(format!("key{}", i), format!("value{}", i))?;
    }

    // for i in 0..ACTIVE_SIZE_THRESHOLD * 3 {
    //     assert_eq!(
    //         format!("value{}", i),
    //         db.get(&format!("key{}", i))?.unwrap()
    //     );
    // }

    std::thread::sleep(time::Duration::from_secs(2));
    Ok(())
}
