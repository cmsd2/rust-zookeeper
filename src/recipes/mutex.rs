use std::sync::{Arc, Mutex};
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use std::thread;
use std::cmp;
use zookeeper::{ZkResult, ZooKeeper};
use consts::*;
use acls;
use std::time::Duration;
use threads::*;
use paths;
use ::WatchedEvent;
use schedule_recv as timer;

/*
Locks in zookeeper use ephemeral sequential nodes.
Each time a process wants to acquire a lock it doesn't already own, it
creates a new node to indicate its desire to acquire the lock.
The node's directory will be the same for all users of the lock.
The name of the node that is created will be something like
"_c_12345678-1234-1234-12345678-lockname000001"
The name of the lock as far as users of the locks api are concerned is "lockname".
The numeric suffix is added by zookeeper to make each node with a given name unique.
The uuid prefix is added by this implementation to guard against sequential nodes
being created but notification of success being lost across a reconnect.
Everything before the final hyphen is opaque to zookeeper.
Once the node has been created, the lock implementation will query for all
the child nodes in the given path that have lockname in their name.
It sorts these by numeric sequence number in ascending order.
A mutex normally only permits one owner at a time, while semaphores permit
some arbitrary number of owners. This is called the maximum number of leases.
If there are a number of nodes equal or greater than the max leases appearing
before our node in the sorted list then we don't yet hold the lock.
To wait for the lock, we watch the node in position (our_pos - max_leases).
TODO is this sufficient? shouldn't we watch the range of elements (our_pos-max_leases..our_pos] ?
When that node is deleted, we check again whether we hold the lock or not by examining our
position in the sorted list of nodes.
[1,2] max_leases=1. our_id=2. watch node 1 (index 0)
[2,3,4,5,6] max_leases=2. our_id=5. watch node 3 (index 1)
If nodes 2 and 3 release in that order, then when we get notified of 3's deletion we'll
be the holder of the lock.
If node 3 releases first, then we won't. nodes 2 and 4 will still hold it, and we'll start
a new watch on node 2.
If node 4 releases after node 3, then at that point, we'll hold the lock along with node 2,
but we won't know it yet until node 2 releases.
*/

pub struct LockInternals {
    base_path: String,
    name: String,
    zk: Arc<ZooKeeper>,
    max_leases: u32,
    thread_lock: Arc<Mutex<LockData>>
}

impl LockInternals {

    pub fn new(zk: Arc<ZooKeeper>, path: &str, lock_name: &str, max_leases: u32) -> LockInternals {
        LockInternals {
            base_path: path.to_owned(),
            name: lock_name.to_owned(),
            zk: zk.clone(),
            max_leases: max_leases,
            thread_lock: Arc::new(Mutex::new(LockData::new()))
        }
    }

    /// checks if a thread in this process owns the lock
    /// by examining local state.
    pub fn is_acquired_in_this_process(&self) -> bool {
        self.owner().is_some()
    }

    /// returns the thread id holding the lock if the
    /// lock is held by a thread in this process.
    /// otherwise returns none.
    pub fn owner(&self) -> Option<ThreadId> {
        let data = self.thread_lock.lock().unwrap();

        data.owner()
    }

    /// decrement the depth of the lock owned by the current thread.
    /// if the count is now 0 then release the lock entirely by
    /// deleting the node in zookeeper.
    /// this wakes the thread (if any) that is watching our node for
    /// changes.
    pub fn release(&self) -> LockResult<()> {
        let thread_id = ThreadId::current_thread_id();
        let lock_path = self.get_lock_path();

        if self.decr_lock(thread_id) {
            try!(self.zk.delete(&lock_path, -1));
        }

        Ok(())
    }

    /// attempt to acquire the lock.
    /// if the thread already holds the lock, the depth will be incremented.
    /// otherwise, create a node in zookeeper to place this thred in the
    /// queue.
    /// then enter a blocking loop in which we wait with a watch for an
    /// earlier item to release the lock, and when it does, recheck to see
    /// if we have acquired it.
    /// acquisition of the lock is implied by our position in the fifo queue.
    pub fn acquire(&self, duration: Option<Duration>) -> LockResult<bool> {
        let thread_id = ThreadId::current_thread_id();

        debug!("acquiring mutex for thread {:?}", thread_id);

        if self.incr_lock(thread_id) {
            return Ok(true)
        }

        let result = try!(self.create_node_loop(duration));

        if result {
            debug!("acquired mutex for thread {:?}", thread_id);
            
            let mut data = self.thread_lock.lock().unwrap();

            data.acquire(thread_id);
        } else {
            debug!("failed to acquire mutex for thread {:?}", thread_id);
        }

        Ok(result)
    }

    /// sets up a timer which will fire after a certain time.
    /// returns the receiver end of the channel to which a message
    /// will be posted to signify the time is up.
    fn timer_oneshot(duration: Duration) -> Receiver<()> {
        let ms = ((duration.as_secs() * 1000) as u32) + duration.subsec_nanos() / 1000000;
        timer::oneshot_ms(ms)
    }

    /// if duration is some, return a receiver from the timer.
    /// if duration is none, return a sender and receiver that will do never fire.
    /// do not let the sender go out of scope until you're done with the receiver.
    fn maybe_timer_oneshot(duration: Option<Duration>) -> (Option<Sender<()>>, Receiver<()>) {
        match duration {
            Some(d) => (None, Self::timer_oneshot(d)),
            None => {
                let (never_tx, never) = mpsc::channel();
                (Some(never_tx), never)
            }
        }
    }

    fn create_node_loop(&self, duration: Option<Duration>) -> ZkResult<bool> {
        let lock_path = self.get_lock_path();

        let (tx, rx) = mpsc::channel();
        let mut done = false;
        let mut result = false;
        let (_timeout_tx, timeout) = Self::maybe_timer_oneshot(duration);


        while !done {
            let tx_clone = tx.clone();
            let exists_watcher = move |event: &WatchedEvent| {
                debug!("watched event: {:?}", event);
                tx_clone.send(true);
            };
            
            let maybe_stat = self.zk.exists_w(&lock_path, exists_watcher);

            match maybe_stat {
                Ok(stat) => {
                    // node exists
                    // wait for watcher
                    debug!("lock node exists {:?}", lock_path);

                    done = select!(
                        lock_result = rx.recv() => {
                            debug!("watcher for {:?} returned {:?}", lock_path, lock_result);
                            result = match lock_result {
                                Ok(exists) => exists,
                                Err(recv_err) => {
                                    debug!("error receiving from watcher channel: {:?}", recv_err);
                                    return Err(ZkError::APIError);
                                }
                            };
                            result
                        },
                        timeout_result = timeout.recv() => {
                            debug!("timeout for {:?}: {:?}", lock_path, timeout_result);
                            true
                        }
                    );
                }
                Err(ZkError::NoNode) => {
                    // node didn't exist when we checked, but could now...
                    debug!("lock node doesn't exist {:?}", lock_path);                    
                    result = try!(self.create_node());
                    done = result;
                }
                Err(e) => {
                    debug!("error watching lock node {:?}: {:?}", lock_path, e);
                    return Err(e);
                }
            }

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

    /// increments depth of the reentrant lock if owned by current thread.
    ///
    /// if already locked by current thread, then increment lock and return true
    /// otherwise return false
    fn incr_lock(&self, thread_id: ThreadId) -> bool {
        let mut data = self.thread_lock.lock().unwrap();

        data.incr(thread_id)
    }

    /// decrement depth of lock held by current thread.
    ///
    /// return true if unlocked, false otherwise.
    fn decr_lock(&self, thread_id: ThreadId) -> bool {
        let mut data = self.thread_lock.lock().unwrap();

        data.decr(thread_id)
    }

    pub fn get_lock_path<'a>(&'a self) -> String {
        Self::make_lock_path(&self.base_path, &self.name)
    }

    /// returns the path to the node to create when requesting
    /// the lock.
    pub fn make_lock_path(path: &str, name: &str) -> String {
        paths::make_path(path, name)
    }
}

pub struct LockData {
    thread_id: Option<ThreadId>,
    lock_count: u32,
    lock_path: Option<String>,
}

impl LockData {
    pub fn new() -> LockData {
        LockData {
            thread_id: None,
            lock_count: 0,
            lock_path: None,
        }
    }

    pub fn owner(&self) -> Option<ThreadId> {
        if self.lock_count != 0 {
            self.thread_id
        } else {
            assert!(self.thread_id.is_none());
            None
        }
    }

    /// the depth of the reentrant lock
    /// 0 implies the lock is not held by a thread
    /// in this process.
    pub fn get_lock_count(&self) -> u32 {
        self.lock_count
    }

    pub fn set_lock_path(&mut self, lock_path: Option<String>) {
        self.lock_path = lock_path;
    }

    /// the actual path of the node as created by zookeeper
    pub fn get_lock_path<'a>(&'a self) -> Option<&'a str> {
        self.lock_path.as_ref().map(|s| &s[..])
    }

    /// if the given thread holds the lock then
    /// increment the lock count and return true
    /// returns false otherwise.
    fn incr(&mut self, thread_id: ThreadId) -> bool {
        if self.thread_id == Some(thread_id) {
            self.incr_and_get_lock_count();
            true
        } else {
            false
        }
    }

    /// if the given thread holds the lock then
    /// decrement the lock count.
    /// returns true if the lock is now unlocked,
    /// false otherwise.
    fn decr(&mut self, thread_id: ThreadId) -> bool {
        if self.thread_id != Some(thread_id) || self.lock_count == 0 {
            panic!("can't release lock: not owned by thread");
        } else {
            self.decr_and_get_lock_count() == 0
        }
    }

    /// if the lock is not already held by a thread in this process
    /// then ensure the owner is the given thread and increment the lock count.
    /// returns some(thread_id) if the thread owns the lock
    /// returns false otherwise.
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

    /// decrements the depth of the lock if held by the given thread.
    /// returns some(thread_id) if the thread still holds the lock.
    /// returns none otherwise.
    pub fn release(&mut self, thread_id: ThreadId) -> Option<ThreadId> {
        if self.decr(thread_id) {
            self.thread_id = None;
        }
        self.thread_id
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
