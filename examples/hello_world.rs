use kvlite::memory::SkipMapMemTable;
use kvlite::KVLite;
use tempfile::TempDir;

fn main() {
    let temp_dir = TempDir::new().unwrap();
    let db = KVLite::<SkipMapMemTable>::open(temp_dir.path()).unwrap();

    let hello = Vec::from("hello");
    let value = Vec::from("value1");
    db.set(hello.clone(), value).unwrap();

    println!("{:?}", db.get(&"hello".into()).unwrap()); // Some("value1")
    db.remove(hello).unwrap();
    assert!(db.get(&"hello".into()).unwrap().is_none());
}
