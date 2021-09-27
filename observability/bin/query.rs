use kvlite::db::dbimpl::DBImpl;
use kvlite::db::key_types::RawUserKey;
use kvlite::db::options::WriteOptions;
use kvlite::db::DB;
use kvlite::memory::MrSwSkipMapMemTable;
use kvlite::wal::simple_wal::SimpleWriteAheadLog;
use minitrace_jaeger::Reporter;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

type DataBase =
    DBImpl<RawUserKey, RawUserKey, MrSwSkipMapMemTable<RawUserKey>, SimpleWriteAheadLog>;

fn trace(db: DataBase) {
    let wo = WriteOptions { sync: false };
    let key = RawUserKey::from([1, 2, 3, 4, 5]);
    let value = RawUserKey::from([4, 5, 6, 7, 8]);
    db.set(&wo, key, value).unwrap();
}

fn main() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = DataBase::open(temp_dir.path()).unwrap();

    use minitrace::*;

    let collector = {
        let (root_span, collector) = Span::root("root");
        let _span_guard = root_span.enter();

        let _local_span_guard = LocalSpan::enter("child");

        // do something ...
        trace(db);
        collector
    };

    let spans: Vec<span::Span> = collector.collect();
    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 6831);

    const TRACE_ID: u64 = 42;
    const SPAN_ID_PREFIX: u32 = 42;
    const ROOT_PARENT_SPAN_ID: u64 = 0;

    let bytes = Reporter::encode(
        String::from("service name"),
        TRACE_ID,
        ROOT_PARENT_SPAN_ID,
        SPAN_ID_PREFIX,
        &spans,
    )
    .expect("report error");
    Reporter::report(socket, &bytes).unwrap();
}
