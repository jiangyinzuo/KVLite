use kvlite::db::dbimpl::DBImpl;
use kvlite::db::key_types::RawUserKey;
use kvlite::db::options::WriteOptions;
use kvlite::db::DB;
use kvlite::memory::MutexSkipMapMemTable;
use kvlite::wal::simple_wal::SimpleWriteAheadLog;
use tempfile::TempDir;

fn main() {
    let temp_dir = TempDir::new().unwrap();
    let db = DBImpl::<
        RawUserKey,
        RawUserKey,
        MutexSkipMapMemTable<RawUserKey>,
        SimpleWriteAheadLog,
    >::open(temp_dir.path())
    .unwrap();
    let write_option = WriteOptions { sync: false };
    let hello = Vec::from("hello");
    let value = Vec::from("value1");
    db.set(&write_option, hello.clone(), value).unwrap();

    println!("{:?}", db.get(&"hello".into()).unwrap());
    db.remove(&write_option, hello).unwrap();
    assert!(db.get(&"hello".into()).unwrap().is_none());
}
