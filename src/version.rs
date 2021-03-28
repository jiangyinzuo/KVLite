use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, RwLock};

pub struct Version {
    table_id: Arc<RwLock<(File, u128)>>,
}

impl Version {
    /// Restore version when opening db.
    pub fn restore(db_path: String, file_name: &str) -> Version {
        let mut version_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(version_path(&db_path, file_name))
            .expect("invalid version file");
        let mut bt = [0u8; 16];
        version_file.seek(SeekFrom::Start(0));
        version_file.read_exact(&mut bt);
        let id = u128::from_le_bytes(bt);
        Version {
            table_id: Arc::new(RwLock::new((version_file, id))),
        }
    }

    pub fn get_table_id(&self) -> u128 {
        let guard = self.table_id.read().unwrap();
        guard.1
    }

    pub fn increment_table_id(&self) {
        let mut guard = self.table_id.write().unwrap();
        guard.1 += 1;
        let bt = guard.1.to_le_bytes();
        guard.0.seek(SeekFrom::Start(0));
        guard.0.write_all(&bt).unwrap();
    }
}

fn version_path(db_path: &str, file_name: &str) -> String {
    format!("{}/{}.txt", db_path, file_name)
}

#[cfg(test)]
mod tests {

    use crate::version::Version;
    use tempfile::TempDir;

    #[test]
    fn test() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_str().unwrap().to_string();
        for i in 0..100 {
            let version = Version::restore(path.clone(), "foo");
            assert_eq!(i, version.get_table_id());
            version.increment_table_id();
            assert_eq!(i + 1, version.get_table_id());
        }
    }
}
