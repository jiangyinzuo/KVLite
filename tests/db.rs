use kvlite::command::WriteCmdOp;
use kvlite::db::Query;
use kvlite::error::KVLiteError;
use kvlite::memory::BTreeMemTable;
use kvlite::KVLite;
use kvlite::Result;
use tempfile::TempDir;

#[test]
fn test_command() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");

    let mut db = KVLite::<BTreeMemTable>::open(temp_dir.path())?;

    db.set("hello".into(), "world".into())?;
    assert_eq!(KVLiteError::KeyNotFound, db.remove("no_exist").unwrap_err());
    assert_eq!("world", db.get("hello")?.unwrap());
    db.remove("hello")?;
    assert!(db.get("hello")?.is_none());
    Ok(())
}
