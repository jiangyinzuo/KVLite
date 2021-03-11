use std::sync::atomic::AtomicU64;

/// The collection of all the Versions produced
pub struct Versions {
    db_path: String,
    next_sstable_id: AtomicU64,
}

impl Versions {
    pub fn new(db_path: String) -> Versions {
        Versions {
            db_path,
            next_sstable_id: AtomicU64::new(0),
        }
    }

    /// Persistently write the immutable memory table to level0 sstable.
    pub fn write_level0_files(&self) {}
}
