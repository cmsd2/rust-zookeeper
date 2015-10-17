use std::sync::{Arc};
use zookeeper::ZooKeeper;
use std::time::Duration;
use retry::*;
use super::locks::*;

pub struct InterProcessMutex<R>
    where R: RetryPolicy + Send + Clone + 'static
{
    internals: LockInternals<R>,
}

impl <R> InterProcessMutex<R>
    where R: RetryPolicy + Send + Clone + 'static
{
    pub fn new(zk: Arc<ZooKeeper>, path: &str, lock_name: &str, max_leases: u32, retry_policy: R) -> InterProcessMutex<R> {
        InterProcessMutex {
            internals: LockInternals::<R>::new(zk, path, lock_name, max_leases, retry_policy)
        }
    }
}

impl <R> InterProcessLock for InterProcessMutex<R>
    where R: RetryPolicy + Send + Clone + 'static
{
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
