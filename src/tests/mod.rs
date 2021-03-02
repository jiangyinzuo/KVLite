mod kv_test;

use crate::KvStore;

#[test]
fn test_open() {
    let mut store = KvStore::open("./test_temp").unwrap();
    store.set("hello".into(), "world".into());
    store.set("foo".into(), "bar".into());
    store.set("foo".into(), "bar2".into());
    assert_eq!("bar2", store.get("foo".into()).unwrap().unwrap());
}
