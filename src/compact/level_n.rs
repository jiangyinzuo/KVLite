use crate::sstable::table_handle::TableReadHandle;
use std::num::NonZeroUsize;
use std::sync::Arc;

pub(crate) fn start_compact(compact_level: NonZeroUsize, handle_to_compact: Arc<TableReadHandle>) {
    let compactor = Compactor::new(compact_level, handle_to_compact);
    compactor.run();
}

struct Compactor {
    compact_level: NonZeroUsize,
    handle_to_compact: Arc<TableReadHandle>,
}

impl Compactor {
    fn new(compact_level: NonZeroUsize, handle_to_compact: Arc<TableReadHandle>) -> Compactor {
        debug_assert_eq!(handle_to_compact.level(), compact_level.get());
        Compactor {
            compact_level,
            handle_to_compact,
        }
    }

    fn run(&self) {}
}
