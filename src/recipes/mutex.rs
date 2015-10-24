use std::sync::{Arc};
use zookeeper::{ZkResult, ZooKeeper};
use consts::*;
use acls;
use std::time::Duration;
use paths;
use super::locks::*;

pub struct InterProcessMutex {
    internals: LockInternals,
}

impl InterProcessMutex {
    pub fn new(zk: Arc<ZooKeeper>, path: &str, lock_name: &str, max_leases: u32) -> InterProcessMutex {
        InterProcessMutex {
            internals: LockInternals::new(zk, path, lock_name, max_leases)
        }
    }
}

impl InterProcessLock for InterProcessMutex {
    fn acquire(&self, duration: Option<Duration>) -> LockResult<bool> {
        self.internals.acquire(duration)
    }
    
    fn is_acquired_in_this_process(&self) -> bool {
        self.internals.is_acquired_in_this_process()
    }
    
    fn release(&self) -> LockResult<()> {
        self.internals.release()
    }
    
    fn get_participant_nodes(&self) -> Vec<String> {
        //TODO
        vec![]
    }
}
