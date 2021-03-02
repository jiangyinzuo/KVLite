mod kv_test;

use crate::KvStore;

#[test]
fn test_open() {
    let mut store = KvStore::open("./test_temp").unwrap();
    store.set("hello".into(), "world".into()).unwrap();
    store.set("foo".into(), "bar".into()).unwrap();
    store.set("foo".into(), "bar2".into()).unwrap();
    assert_eq!("bar2", store.get("foo").unwrap().unwrap());
    assert_eq!("world", store.get("hello").unwrap().unwrap());
    store.remove("hello").unwrap();
    assert!(store.get("hello").unwrap().is_none());
}
