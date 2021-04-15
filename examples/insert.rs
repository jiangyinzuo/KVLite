use kvlite::memory::SkipMapMemTable;

const NUM_KVS: i32 = 1000000;

fn main() {
    let path = tempfile::tempdir().unwrap();
    let db = kvlite::KVLite::<SkipMapMemTable>::open(path.path()).unwrap();
    let start = std::time::Instant::now();
    for i in 0..NUM_KVS {
        db.set(i.to_string(), i.to_string()).unwrap();
    }
    let end = std::time::Instant::now();
    println!("{:?}", end - start);

    let start = std::time::Instant::now();
    for i in 0..NUM_KVS {
        db.get(&i.to_string()).unwrap();
    }

    let end = std::time::Instant::now();
    println!("{:?}", end - start);

    drop(db);
}
