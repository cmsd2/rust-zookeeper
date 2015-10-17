use std::sync::{Arc, Mutex};
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use zookeeper::{ZkResult, ZooKeeper};
use consts::*;
use acls;
use std::time::Duration;
use threads::*;
use paths;

pub struct LockInternals {
    lock_path: String,
    zk: Arc<ZooKeeper>,
    max_leases: u32,
    thread_lock: Arc<Mutex<LockData>>
}

impl LockInternals {

    pub fn new(zk: Arc<ZooKeeper>, path: &str, lock_name: &str, max_leases: u32) -> LockInternals {
        LockInternals {
            lock_path: Self::make_lock_path(path, lock_name),
            zk: zk.clone(),
            max_leases: max_leases,
            thread_lock: Arc::new(Mutex::new(LockData::new()))
        }
    }

    pub fn is_acquired_in_this_process(&self) -> bool {
        self.owner().is_some()
    }

    pub fn owner(&self) -> Option<ThreadId> {
        let data = self.thread_lock.lock().unwrap();

        data.owner()
    }

    pub fn release(&self) -> LockResult<()> {
        let thread_id = ThreadId::current_thread_id();
        let lock_path = self.get_lock_path();

        if self.decr_lock(thread_id) {
            try!(self.zk.delete(&lock_path, -1));
        }

        Ok(())
    }
             
    pub fn acquire(&self, duration: Option<Duration>) -> LockResult<bool> {
        let thread_id = ThreadId::current_thread_id();

        debug!("acquiring mutex for thread {:?}", thread_id);

        if self.incr_lock(thread_id) {
            return Ok(true)
        }

        let result = try!(self.create_node());

        if result {
            debug!("acquired mutex for thread {:?}", thread_id);
            
            let mut data = self.thread_lock.lock().unwrap();

            data.acquire(thread_id);
        }

        Ok(result)
    }

    fn create_node(&self) -> ZkResult<bool> {
        let lock_path = self.get_lock_path();
        let data = vec![];
        
        let result = self.zk.create(&lock_path, data, acls::OPEN_ACL_UNSAFE.clone(), CreateMode::Ephemeral);

        match result {
            Ok(path) => {
                debug!("created node {:?}", path);
                Ok(true)
            }
            Err(ZkError::NodeExists) => {
                Ok(false)
            }
            Err(err) => {
                Err(err)
            }
        }
    }

    /*
    * if already locked by current thread, then increment lock and return true
    * otherwise return false
    */
    fn incr_lock(&self, thread_id: ThreadId) -> bool {
        let mut data = self.thread_lock.lock().unwrap();

        data.incr(thread_id)
    }

    /*
    * decrement lock.
    * return true if unlocked, false otherwise.
    */
    fn decr_lock(&self, thread_id: ThreadId) -> bool {
        let mut data = self.thread_lock.lock().unwrap();

        data.decr(thread_id)
    }

    pub fn get_lock_path<'a>(&'a self) -> &'a str {
        &self.lock_path
    }
    
    pub fn make_lock_path(path: &str, name: &str) -> String {
        paths::make_path(path, name)
    }
}

pub struct LockData {
    thread_id: Option<ThreadId>,
    lock_count: u32,
}

impl LockData {
    pub fn new() -> LockData {
        LockData {
            thread_id: None,
            lock_count: 0,
        }
    }

    pub fn owner(&self) -> Option<ThreadId> {
        if self.lock_count != 0 {
            self.thread_id
        } else {
            None
        }
    }

    pub fn get_lock_count(&self) -> u32 {
        self.lock_count
    }

    pub fn incr(&mut self, thread_id: ThreadId) -> bool {
        if self.thread_id == Some(thread_id) {
            self.incr_and_get_lock_count();
            true
        } else {
            false
        }
    }

    pub fn decr(&mut self, thread_id: ThreadId) -> bool {
        if self.thread_id != Some(thread_id) || self.lock_count == 0 {
            panic!("can't release lock: not owned by thread");
        } else {
            self.decr_and_get_lock_count() == 0
        }
    }

    pub fn acquire(&mut self, thread_id: ThreadId) -> Option<ThreadId> {
        if self.thread_id == Some(thread_id) {
            let count = self.incr_and_get_lock_count();
            debug!("lock already owned by thread {:?} count now {:?}", thread_id, count);
            self.thread_id
        } else if self.lock_count == 0 {
            self.thread_id = Some(thread_id);
            self.incr_and_get_lock_count();
            debug!("lock freshly claimed by thread {:?}", thread_id);
            self.thread_id
        } else {
            debug!("lock owned by thread {:?} count {:?}", self.thread_id, self.lock_count);
            None
        }
    }

    pub fn release(&mut self, thread_id: ThreadId) -> Option<ThreadId> {
        if self.decr(thread_id) {
            None
        } else {
            self.thread_id
        }
    }

    fn incr_and_get_lock_count(&mut self) -> u32 {
        self.lock_count += 1;
        self.lock_count
    }

    fn decr_and_get_lock_count(&mut self) -> u32 {
        self.lock_count -= 1;
        self.lock_count
    }
}

pub struct InterProcessMutex {
    internals: LockInternals,
}

pub enum LockError {
    ZkLockError(ZkError)
}

impl From<ZkError> for LockError {
    fn from(err: ZkError) -> LockError {
        LockError::ZkLockError(err)
    }
}

pub type LockResult<T> = Result<T, LockError>;

pub trait InterProcessLock {
    fn acquire(&self, duration: Option<Duration>) -> LockResult<bool>;
    fn is_acquired_in_this_process(&self) -> bool;
    fn release(&self) -> LockResult<()>;
    fn get_participant_nodes(&self) -> Vec<String>;
//    fn make_revocable(listener...?);
}

impl InterProcessMutex {
    pub fn new(zk: Arc<ZooKeeper>, path: &str, lock_name: &str, max_leases: u32) -> InterProcessMutex {
        InterProcessMutex {
            internals: LockInternals::new(zk, path, lock_name, max_leases)
        }
    }

    pub fn get_lock_path<'a>(&'a self) -> &'a str {
        &self.internals.lock_path
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
