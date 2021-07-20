use kvlite::db::key_types::InternalKey;
use kvlite::db::no_transaction_db::NoTransactionDB;
use kvlite::db::options::WriteOptions;
use kvlite::db::DB;
use kvlite::memory::{MrMwSkipMapMemTable, MrSwSkipMapMemTable};
use kvlite::wal::simple_wal::SimpleWriteAheadLog;
use procfs::CpuInfo;
use rand::distributions::Uniform;
use rand::{Rng, RngCore};
use std::time::Duration;
use tempfile::TempDir;

const NUM_KVS: u128 = 1000000;
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

type DataBase = NoTransactionDB<
    InternalKey,
    InternalKey,
    MrMwSkipMapMemTable<InternalKey>,
    SimpleWriteAheadLog,
>;

struct BenchMark {
    // drop `db` before `_temp_dir`
    db: DataBase,
    _temp_dir: TempDir,
}

impl BenchMark {
    fn new() -> BenchMark {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = DataBase::open(temp_dir.path()).unwrap();
        BenchMark {
            _temp_dir: temp_dir,
            db,
        }
    }

    fn reopen_db(&mut self) {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = DataBase::open(temp_dir.path()).unwrap();
        self.db = db;
        self._temp_dir = temp_dir;
    }

    fn fill_seq(&self) {
        let write_options = WriteOptions { sync: false };

        let mut random = rand::thread_rng();
        let start = std::time::Instant::now();
        for i in 0u128..NUM_KVS {
            let mut value = Vec::from([0u8; VALUE_SIZE]);
            random.fill_bytes(&mut value);
            self.db
                .set(&write_options, Vec::from(i.to_be_bytes()), value)
                .unwrap();
        }
        let end = std::time::Instant::now();
        let elapsed = (end - start).as_secs_f64();
        let elapsed_micros = (end - start).as_micros();
        self.print_write(
            "fill_seq",
            elapsed_micros as f64 / NUM_KVS as f64,
            RAW_SIZE / elapsed,
        );
    }

    fn fill_random(&mut self) {
        let duration = self.do_write(false, NUM_KVS, true);
        let elapsed = duration.as_secs_f64();
        let elapsed_micros = duration.as_micros() as f64;
        self.print_write(
            "fill_random",
            elapsed_micros as f64 / NUM_KVS as f64,
            RAW_SIZE / elapsed,
        );
    }

    fn fill_random_sync(&mut self) {
        let num_kvs = NUM_KVS / 100;
        let duration = self.do_write(true, num_kvs, true);
        let elapsed = duration.as_secs_f64();
        let elapsed_micros = duration.as_micros() as f64;
        let file_size = fs_extra::dir::get_size(self.db.db_path()).unwrap();
        println!(
            "{:<20}: {:>10.3} micros/op {:>10.3} MB/s | file size: {}  ({} ops)",
            "fill_random_sync",
            elapsed_micros / num_kvs as f64,
            RAW_SIZE / 100f64 / elapsed,
            file_size,
            num_kvs
        );
    }

    fn overwrite(&mut self) {
        let duration = self.do_write(false, NUM_KVS, false);
        let elapsed = duration.as_secs_f64();
        let elapsed_micros = duration.as_micros() as f64;
        self.print_write(
            "overwrite",
            elapsed_micros as f64 / NUM_KVS as f64,
            RAW_SIZE / elapsed,
        );
    }

    fn do_write(&mut self, sync: bool, num_kvs: u128, reopen_db: bool) -> Duration {
        if reopen_db {
            self.reopen_db();
        }
        let random = rand::thread_rng();
        let mut random_iter = random.sample_iter(Uniform::new_inclusive(0, num_kvs));
        let mut random = rand::thread_rng();

        let write_options = WriteOptions { sync };
        let start = std::time::Instant::now();

        for _ in 0u128..num_kvs {
            let i = random_iter.next().unwrap();
            let mut value = Vec::from([0u8; VALUE_SIZE]);
            random.fill_bytes(&mut value);
            self.db
                .set(&write_options, Vec::from(i.to_be_bytes()), value)
                .unwrap();
        }
        let end = std::time::Instant::now();
        end - start
    }

    fn read_seq(&self) {
        let iterator = self.db.get_db_iterator();
        let start = std::time::Instant::now();
        let mut count: u128 = 0;
        for (k, _v) in iterator {
            debug_assert_eq!(Vec::from(count.to_be_bytes()), k);
            count += 1;
        }

        let end = std::time::Instant::now();
        let elapsed = (end - start).as_secs_f64();
        println!(
            "{:<20}: {:10.3} MB/s ({} of {} found)",
            "read_seq",
            RAW_SIZE / elapsed,
            count,
            NUM_KVS
        );
    }

    fn read_random(&self) {
        let mut random = rand::thread_rng().sample_iter(Uniform::new_inclusive(0, NUM_KVS));
        let mut not_found = 0;
        let start = std::time::Instant::now();
        for _ in 0u128..NUM_KVS {
            if self
                .db
                .get(&Vec::from(random.next().unwrap().to_be_bytes()))
                .unwrap()
                .is_none()
            {
                not_found += 1;
            }
        }

        let end = std::time::Instant::now();
        let elapsed = (end - start).as_secs_f64();
        println!(
            "{:<20}: {:10.3} reads per second ({} of {} found)",
            "read_random",
            NUM_KVS as f64 / elapsed,
            NUM_KVS - not_found,
            NUM_KVS
        );
    }

    fn print_write(&self, bench_name: &str, micros_per_op: f64, size_per_sec: f64) {
        let file_size = fs_extra::dir::get_size(self.db.db_path()).unwrap();
        println!(
            "{:<20}: {:>10.3} micros/op {:>10.3} MB/s | file size: {}",
            bench_name, micros_per_op, size_per_sec, file_size
        );
    }
}

fn main() {
    print_environment();
    print_arguments();
    if cfg!(feature = "use_jemalloc") {
        println!("Use jemalloc");
    } else {
        println!("Use system default memory allocator");
    }

    if cfg!(feature = "snappy_compression") {
        println!("Use snappy compression algorithm");
    } else {
        println!("No compression algorithm");
    }

    println!("-------------------------------------------------");
    let mut benchmark = BenchMark::new();
    benchmark.fill_seq();
    benchmark.read_seq();
    benchmark.fill_random_sync();
    benchmark.fill_random();
    benchmark.read_random();
    benchmark.overwrite();
}
