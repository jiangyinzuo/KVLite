use kvlite::command::WriteCmdOp;
use kvlite::memory::BTreeMemTable;
use kvlite::KVLite;
use kvlite::Result;

#[test]
fn test_command() -> Result<()> {
    let mut db = KVLite::<BTreeMemTable>::new("./temp_file.log")?;
    db.set("hello".into(), "world".into())?;

    Ok(())
}
