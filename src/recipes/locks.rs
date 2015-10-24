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

#[derive(Debug,Clone,PartialEq)]
pub struct LockNode {
    pub node_name: String,
    pub uuid: String,
    pub name: String,
    pub sequence: String
}

impl LockNode {
    pub fn new(name: &str, node_name: &str) -> Option<LockNode> {
        match paths::split_protected_name_suffix(name, node_name) {
            Some((uuid, name, suffix)) => {
                Some(LockNode {
                    node_name: node_name.to_owned(),
                    uuid: uuid.to_owned(),
                    name: name.to_owned(),
                    sequence: suffix.to_owned(),
                })
            },
            None => None
        }
    }

    pub fn cmp_by_sequence(a: &Self, b: &Self) -> cmp::Ordering {
        a.sequence.cmp(&b.sequence)
    }
}

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

    fn get_sorted_lock_nodes(&self) -> ZkResult<Vec<LockNode>> {
        let children = try!(self.zk.get_children(&self.base_path, false));

        let mut protected_nodes: Vec<LockNode> = children.iter().flat_map(|node| {
            LockNode::new(&self.name, node)
        }).filter(|node| node.name == self.name)
            .collect();

        protected_nodes.sort_by(LockNode::cmp_by_sequence);
        
        Ok(protected_nodes)
    }

    fn find_lock_node(&self, lock_nodes: &[LockNode], node_name: &str) -> Option<usize> {
        lock_nodes.iter().position(|node| node.node_name == node_name)
    }
    
    fn have_lock(&self, pos: usize, max_leases: u32) -> bool {
        pos < (max_leases as usize)
    }

    fn create_node_loop(&self, duration: Option<Duration>) -> ZkResult<bool> {
        let lock_path = self.get_lock_path();

        let (_timeout_tx, timeout) = Self::maybe_timer_oneshot(duration);


        // create ephemeral sequential node
        // TODO retry until success or crash, according to retry policy
        let path = try!(self.create_node());

        loop {
            debug!("checking lock ownership");
            match try!(self.check_lock_or_wait(&path, &timeout)) {
                Some(acquired) => {
                    if acquired {
                        debug!("lock acquired");
                        return Ok(acquired);
                    } else {
                        debug!("lock not acquired. looping");
                    }
                },
                None => {
                    // timed out
                    debug!("timed out acquiring lock");
                    try!(self.zk.delete(&path, -1));
                    return Ok(false);
                }
            }
        }
    }

    pub fn check_lock_or_wait(&self, node_path: &str, timeout: &Receiver<()>) -> ZkResult<Option<bool>> {
        debug!("checking lock {}", node_path);
        let (base_path, node_name) = paths::split_path(&node_path);
            
        // download list of children
        // sort the list by the sequence number suffix
        let children = try!(self.get_sorted_lock_nodes());

        debug!("found lock queue {:?}", children);
        
        // check where we are in the list
        match self.find_lock_node(&children[..], node_name) {
            Some(pos) => {
                debug!("found node {} in lock queue at pos {}", node_name, pos);
                if self.have_lock(pos, self.max_leases) {
                    // if in the range [0..max_leases) then we
                    // already own the lock, so return Ok(true)
                    debug!("we own the lock!");
                    Ok(Some(true))
                } else {
                    // otherwise, get and watch the n-max_leases item
                    // where n is our position in the list.
                    // sleep/block until that node is removed.
                    // when we wake up again, loop
                    let watch_pos = pos - (self.max_leases as usize);
                    let watch_node_name = &children[watch_pos].node_name;

                    debug!("waiting on node {} at pos {}", watch_node_name, watch_pos);
                    let not_timedout = try!(self.wait_while_exists(watch_node_name, timeout));

                    if not_timedout {
                        debug!("got watch event for node");
                        Ok(Some(false))
                    } else {
                        debug!("timed out waiting for node");
                        Ok(None)
                    }
                }
            },
            None => {
                // our session probably got reset, deleting our
                // ephemeral node
                debug!("couldn't find our sequential lock node!");
                Ok(Some(false))
            }
        }
    }

    pub fn exists_w(&self, node_name: &str, sender: Sender<bool>) -> ZkResult<bool> {
        let node_path = paths::make_path(&self.base_path, node_name);
        
        let exists_watcher = move |event: &WatchedEvent| {
            debug!("watched event: {:?}", event);
            sender.send(event.event_type != WatchedEventType::NodeDeleted);
        };

        match self.zk.exists_w(&node_path, exists_watcher) {
            Ok(_stat) => Ok(true),
            Err(ZkError::NoNode) => Ok(false),
            Err(err) => Err(err)
        }
    }

    pub fn wait_while_exists(&self, node_name: &str, timeout_rx: &Receiver<()>) -> ZkResult<bool> {
        let (watch_tx, watch_rx) = mpsc::channel();
        
        if false == try!(self.exists_w(node_name, watch_tx.clone())) {
            debug!("node {} is already deleted", node_name);
            return Ok(false);
        }

        debug!("waiting for watch event on {}", node_name);
        
        loop {
            select!(
                exists_result = watch_rx.recv() => {
                    debug!("watcher for {:?} returned {:?}", node_name, exists_result);
                    match exists_result {
                        Ok(exists) => {
                            if exists {
                                debug!("node {} exists. resetting watch", node_name);
                                if false == try!(self.exists_w(node_name, watch_tx.clone())) {
                                    debug!("node {} is now deleted", node_name);
                                    return Ok(false);
                                }
                            } else {
                                return Ok(true);
                            }
                        },
                        Err(err) => {
                            warn!("error receiving from watch channel: {:?}", err);
                            return Err(ZkError::APIError);
                        }
                    }
                },
                timeout_result = timeout_rx.recv() => {
                    debug!("timeout for {:?}: {:?}", node_name, timeout_result);
                    return Ok(false);
                }
                );
        }
    }
    

    fn create_node(&self) -> ZkResult<String> {
        let lock_path = self.get_lock_path();
        let data = vec![];
        
        self.zk.create_p(&lock_path, data, acls::OPEN_ACL_UNSAFE.clone(), CreateMode::EphemeralSequential)
    }

    /// increments depth of the reentrant lock if owned by current thread.
    ///
    /// if already locked by current thread, then increment lock and return true
    /// otherwise return false
    fn incr_lock(&self, thread_id: ThreadId) -> bool {
        let mut data = self.thread_lock.lock().unwrap();

        data.acquire(thread_id) == Some(thread_id)
    }

    /// decrement depth of lock held by current thread.
    ///
    /// return true if unlocked, false otherwise.
    fn decr_lock(&self, thread_id: ThreadId) -> bool {
        let mut data = self.thread_lock.lock().unwrap();

        data.release(thread_id) == None
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
            debug!("lock finally released by thread {:?}", thread_id);
            self.thread_id = None;
        } else {
            debug!("lock still owned by thread {:?} count {:?}", thread_id, self.lock_count);
        }
        self.thread_id
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

    fn incr_and_get_lock_count(&mut self) -> u32 {
        self.lock_count += 1;
        self.lock_count
    }

    fn decr_and_get_lock_count(&mut self) -> u32 {
        self.lock_count -= 1;
        self.lock_count
    }
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
