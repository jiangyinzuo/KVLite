use kvlite::db::key_types::InternalKey;
use kvlite::db::no_transaction_db::NoTransactionDB;
use kvlite::db::options::WriteOptions;
use kvlite::db::DB;
use kvlite::memory::SkipMapMemTable;
use kvlite::wal::simple_wal::SimpleWriteAheadLog;
use procfs::CpuInfo;
use rand::distributions::Uniform;
use rand::Rng;
use tempfile::TempDir;

const NUM_KVS: i128 = 1000000;
const KEY_SIZE: usize = std::mem::size_of::<i128>();
const VALUE_SIZE: usize = 100;
const RAW_SIZE: f64 = ((KEY_SIZE + VALUE_SIZE) * NUM_KVS as usize) as f64 / 1024f64 / 1024f64;

fn print_environment() {
    println!("KVLite: version {}", env!("CARGO_PKG_VERSION"));

    let datetime = chrono::Utc::now();
    println!("Date: {:?}", datetime.naive_utc());

    let cpu_info = CpuInfo::new().unwrap();
    println!(
        "CPU: {} * {}",
        cpu_info.cpus.len(),
        cpu_info.fields.get("model name").unwrap()
    );
    println!("CPU Cache: {}", cpu_info.fields.get("cache size").unwrap());
}

fn print_arguments() {
    println!("Keys: {} bytes each", KEY_SIZE);
    println!("Values: {} bytes each", VALUE_SIZE);
    println!("Entries: {}", NUM_KVS);
    println!("RawSize: {} MB (estimated)", RAW_SIZE);
}

struct BenchMark {
    _temp_dir: TempDir,
    db: NoTransactionDB<
        InternalKey,
        InternalKey,
        SkipMapMemTable<InternalKey>,
        SimpleWriteAheadLog,
    >,
}

impl BenchMark {
    fn new() -> BenchMark {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = NoTransactionDB::<
            InternalKey,
            InternalKey,
            SkipMapMemTable<InternalKey>,
            SimpleWriteAheadLog,
        >::open(temp_dir.path())
        .unwrap();
        BenchMark {
            _temp_dir: temp_dir,
            db,
        }
    }

    fn reopen_db(&mut self) {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = NoTransactionDB::<
            InternalKey,
            InternalKey,
            SkipMapMemTable<InternalKey>,
            SimpleWriteAheadLog,
        >::open(temp_dir.path())
        .unwrap();
        self.db = db;
        self._temp_dir = temp_dir;
    }

    fn fill_seq(&self) {
        let write_options = WriteOptions { sync: false };
        let start = std::time::Instant::now();

        for i in 0i128..NUM_KVS {
            self.db
                .set(
                    &write_options,
                    Vec::from(i.to_le_bytes()),
                    Vec::from([i as u8; VALUE_SIZE]),
                )
                .unwrap();
        }
        let end = std::time::Instant::now();
        let elapsed = (end - start).as_secs_f64();
        println!("fill_seq: {:?} MB/s", RAW_SIZE / elapsed);
    }

    fn fill_random(&mut self) {
        let elapsed = self.do_write(false, NUM_KVS);
        println!("fill_random: {:?} MB/s", RAW_SIZE / elapsed);
    }

    fn fill_random_sync(&mut self) {
        let num_kvs = NUM_KVS / 100;
        let elapsed = self.do_write(true, num_kvs);
        println!(
            "fill_random_sync: {:?} MB/s) ({} ops) ",
            RAW_SIZE / 100f64 / elapsed,
            num_kvs
        );
    }

    fn do_write(&mut self, sync: bool, num_kvs: i128) -> f64 {
        self.reopen_db();
        let mut random = rand::thread_rng().sample_iter(Uniform::new_inclusive(0, num_kvs));
        let write_options = WriteOptions { sync };
        let start = std::time::Instant::now();

        for _ in 0i128..num_kvs {
            let i = random.next().unwrap();
            self.db
                .set(
                    &write_options,
                    Vec::from(i.to_le_bytes()),
                    Vec::from([i as u8; VALUE_SIZE]),
                )
                .unwrap();
        }
        let end = std::time::Instant::now();
        (end - start).as_secs_f64()
    }

    fn read_seq(&self) {
        let mut not_found = 0;
        let start = std::time::Instant::now();
        for i in 0..NUM_KVS {
            if self.db.get(&Vec::from(i.to_le_bytes())).unwrap().is_none() {
                not_found += 1;
            }
        }

        let end = std::time::Instant::now();
        let elapsed = (end - start).as_secs_f64();
        println!(
            "read_seq: {:?} MB/s ({} of {} found)",
            RAW_SIZE / elapsed,
            NUM_KVS - not_found,
            NUM_KVS
        );
    }

    fn read_random(&self) {
        let mut random = rand::thread_rng().sample_iter(Uniform::new_inclusive(0, NUM_KVS));
        let mut not_found = 0;
        let start = std::time::Instant::now();
        for _ in 0..NUM_KVS {
            if self
                .db
                .get(&Vec::from(random.next().unwrap().to_le_bytes()))
                .unwrap()
                .is_none()
            {
                not_found += 1;
            }
        }

        let end = std::time::Instant::now();
        let elapsed = (end - start).as_secs_f64();
        println!(
            "read_random: {:?} MB/s ({} of {} found)",
            RAW_SIZE / elapsed,
            NUM_KVS - not_found,
            NUM_KVS
        );
    }
}

fn main() {
    print_environment();
    print_arguments();
    println!("-------------------------------------------------");
    let mut benchmark = BenchMark::new();
    benchmark.fill_seq();
    benchmark.fill_random_sync();
    benchmark.fill_random();
    benchmark.read_seq();
    benchmark.fill_random();
    benchmark.read_random();
}
