use crate::sstable::manager::TableManager;
use std::sync::Arc;
use tokio::runtime::Runtime;
use crossbeam_channel::{Sender, Receiver};
use crate::db::MAX_LEVEL;

/// Struct for compacting level 1 ~ level N sstables.
pub struct Compactor {
    table_manager: std::sync::Arc<TableManager>,
    rt: Arc<Runtime>,
    senders: Vec<Sender<()>>,
}

impl Compactor {
    pub fn new(table_manager: Arc<TableManager>, rt: Arc<Runtime>) -> Compactor {
        let senders = Vec::with_capacity(MAX_LEVEL);
        let mut compactor = Compactor { table_manager, rt, senders};
        for i in 1..=MAX_LEVEL {
            let (sender, receiver) = crossbeam_channel::unbounded();
            compactor.senders.push(sender);
            compactor.start_compacting_task(i, receiver);
        }
        
        compactor
    }

    fn start_compacting_task(&self, compact_level: usize, receiver: Receiver<()>) {
        self.rt.spawn(async move {
            info!("start compacting task for level {}.", compact_level);
            while let Ok(()) = receiver.recv() {
                
            }
        });
    }
}
