use kvlite::db::key_types::InternalKey;
use kvlite::db::no_transaction_db::NoTransactionDB;
use kvlite::db::options::WriteOptions;
use kvlite::db::DB;
use kvlite::memory::SkipMapMemTable;
use kvlite::wal::simple_wal::SimpleWriteAheadLog;

const NUM_KVS: i32 = 100000;

fn main() {
    let path = tempfile::tempdir().unwrap();
    let db = NoTransactionDB::<
        InternalKey,
        InternalKey,
        SkipMapMemTable<InternalKey>,
        SimpleWriteAheadLog,
    >::open(path.path())
    .unwrap();
    let write_options = WriteOptions { sync: false };
    let start = std::time::Instant::now();
    for i in 0i32..NUM_KVS {
        db.set(
            &write_options,
            Vec::from(i.to_le_bytes()),
            Vec::from(i.to_le_bytes()),
        )
        .unwrap();
    }
    let end = std::time::Instant::now();
    println!("{:?}", end - start);

    let start = std::time::Instant::now();
    for i in 0..NUM_KVS {
        db.get(&Vec::from(i.to_le_bytes())).unwrap();
    }

    let end = std::time::Instant::now();
    println!("{:?}", end - start);

    drop(db);
}
