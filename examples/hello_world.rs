use kvlite::memory::SkipMapMemTable;
use kvlite::KVLite;
use tempfile::TempDir;

fn main() {
    let temp_dir = TempDir::new().unwrap();
    let db = KVLite::<SkipMapMemTable>::open(temp_dir.path()).unwrap();
    db.set("hello".to_string(), "value1".to_string()).unwrap();

    println!("{:?}", db.get(&"hello".into()).unwrap()); // Some("value1")
}
