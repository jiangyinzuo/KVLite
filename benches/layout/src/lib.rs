#![feature(trusted_random_access)]
#![feature(adt_const_params)]
#![feature(generic_const_exprs)]

use tempfile::TempDir;

pub mod dsm;
pub mod nsm;
pub mod pax;

const MAX_LEVEL: usize = 3;

pub fn setup() -> (TempDir, String) {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap().to_string();
    for i in 1..=MAX_LEVEL {
        std::fs::create_dir_all(format!("{}/{}", db_path, i)).unwrap();
    }
    (temp_dir, db_path)
}
