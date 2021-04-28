use kvlite::db::no_transaction_db::NoTransactionDB;
use kvlite::db::DB;
use kvlite::memory::SkipMapMemTable;

const NUM_KVS: i32 = 1000000;

fn main() {
    let path = tempfile::tempdir().unwrap();
    let db = NoTransactionDB::<SkipMapMemTable>::open(path.path()).unwrap();
    let start = std::time::Instant::now();
    for i in 0i32..NUM_KVS {
        db.set(Vec::from(i.to_le_bytes()), Vec::from(i.to_le_bytes()))
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
