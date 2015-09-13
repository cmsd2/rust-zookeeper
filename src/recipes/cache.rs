use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use std::collections::HashMap;
use consts::{WatchedEventType, ZkError, ZkState};
use proto::WatchedEvent;
use zookeeper::{ZkResult, ZooKeeper};
use zookeeper_ext::ZooKeeperExt;
use listeners::{ListenerSet, Subscription};

pub type ChildData = Arc<Vec<u8>>;
pub type Data = HashMap<String, ChildData>;

#[derive(Debug,Clone)]
pub enum PathChildrenCacheEvent {
    Initialized,
    ConnectionSuspended,
    ConnectionLost,
    ConnectionReconnected,
    ChildRemoved(String),
    ChildAdded(String, ChildData),
    ChildUpdated(String, ChildData),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RefreshMode {
    Standard,
    ForceGetDataAndStat,
}

#[derive(Debug)]
pub enum Operation {
    Shutdown,
    Refresh(RefreshMode),
    Event(PathChildrenCacheEvent),
    GetData(String /* path */),
}

pub struct PathChildrenCache {
    path: Arc<String>,
    zk: Arc<ZooKeeper>,
    data: Arc<Mutex<Data>>,
    worker_thread: Option<thread::JoinHandle<()>>,
    channel: Option<Sender<Operation>>,
    listener_subscription: Option<Subscription>,
    event_listeners: ListenerSet<PathChildrenCacheEvent>,
}

impl PathChildrenCache {

    fn get_children(zk: Arc<ZooKeeper>, path: &str, data: Arc<Mutex<Data>>, ops_chan: Sender<Operation>, mode: RefreshMode) -> ZkResult<()> {
        let ops_chan1 = ops_chan.clone();

        let watcher = move |event: &WatchedEvent| {
            match event.event_type {
                WatchedEventType::NodeChildrenChanged => {
                    let _path = event.path.as_ref().expect("Path absent");

                    // Subscribe to new changes recursively
                    match ops_chan1.send(Operation::Refresh(RefreshMode::Standard)) {
                        Err(err) => {
                            warn!("error sending Refresh operation to ops channel: {:?}", err);
                        },
                        _ => {}
                    };
                },
                _ => error!("Unexpected: {:?}", event)
            };
        };

        let children = try!(zk.get_children_w(&path, watcher));

        let mut data_locked = data.lock().unwrap();

        for child in children.iter() {
            let child_path = join_path(path, child);

            if mode == RefreshMode::ForceGetDataAndStat || !data_locked.contains_key(&child_path) {

                let child_data = try!(Self::get_data(zk.clone(), &child_path, data.clone(), ops_chan.clone()));

                let child_data = Arc::new(child_data);
                
                data_locked.insert(child_path.clone(), child_data.clone());

                try!(ops_chan.send(Operation::Event(PathChildrenCacheEvent::ChildAdded(child_path, child_data))).map_err(|err| {
                    info!("error sending ChildAdded event: {:?}", err);
                    ZkError::APIError
                }));
            }
        }

        debug!("New data: {:?}", *data_locked);

        Ok(())
    }

    fn get_data(zk: Arc<ZooKeeper>, path: &str, data: Arc<Mutex<Data>>, ops_chan: Sender<Operation>) -> ZkResult<Vec<u8>> {
        let path1 = path.to_owned();

        let data_watcher = move |event: &WatchedEvent| {
            let mut data_locked = data.lock().unwrap();
            match event.event_type {
                WatchedEventType::NodeDeleted => {
                    data_locked.remove(&path1);

                    match ops_chan.send(Operation::Event(PathChildrenCacheEvent::ChildRemoved(path1.clone()))) {
                        Err(err) => {
                            warn!("error sending ChildRemoved event: {:?}", err);
                        },
                        _ => {}
                    }
                },
                WatchedEventType::NodeDataChanged => {
                    // Subscribe to new changes recursively
                    match ops_chan.send(Operation::GetData(path1)) {
                        Err(err) => {
                            warn!("error sending GetData to op channel: {:?}", err);
                        },
                        _ => {}
                    }
                },
                _ => error!("Unexpected: {:?}", event)
            };

            debug!("New data: {:?}", *data_locked);
        };

        zk.get_data_w(path, data_watcher).map(|stuff| { stuff.0 })
    }

    fn update_data(zk: Arc<ZooKeeper>, path: &str, data: Arc<Mutex<Data>>, ops_chan_tx: Sender<Operation>) -> ZkResult<()> {
        let mut data_locked = data.lock().unwrap();
        
        let path = path.to_owned();
        
        let result = Self::get_data(zk.clone(), &path, data.clone(), ops_chan_tx.clone());
        
        match result {
            Ok(child_data) => {
                info!("got data {:?}", child_data);
                
                let child_data = Arc::new(child_data);
                        
                data_locked.insert(path.clone(), child_data.clone());
                
                ops_chan_tx.send(Operation::Event(PathChildrenCacheEvent::ChildUpdated(path, child_data))).map_err(|err| {
                    warn!("error sending ChildUpdated event: {:?}", err);
                    ZkError::APIError
                })
            },
            Err(err) => {
                warn!("error getting child data: {:?}", err);
                Err(ZkError::APIError)
            }
        }
    }

    pub fn new(zk: Arc<ZooKeeper>, path: &str) -> ZkResult<PathChildrenCache> {

        let data = Arc::new(Mutex::new(HashMap::new()));

        try!(zk.ensure_path(path));

        Ok(PathChildrenCache{
            path: Arc::new(path.to_string()),
            zk: zk,
            data: data,
            worker_thread: None,
            channel: None,
            listener_subscription: None,
            event_listeners: ListenerSet::new(),
        })
    }

    pub fn get_current_data(&self) -> Data {
        self.data.lock().unwrap().clone()
    }

    pub fn clear(&self) {
        self.data.lock().unwrap().clear()
    }

    fn handle_state_change(state: ZkState, ops_chan_tx: Sender<Operation>) -> bool {
        let mut done = false;

        info!("zk state change {:?}", state);
        match state {
            ZkState::Connected => {
                match ops_chan_tx.send(Operation::Refresh(RefreshMode::ForceGetDataAndStat)) {
                    Err(err) => {
                        warn!("error sending Refresh to op channel: {:?}", err);
                        done = true;
                    },
                    _ => {}
                }
            },
            _ => {
            }
        }
        
        done
    }

    fn handle_operation(op: Operation, zk: Arc<ZooKeeper>, path: Arc<String>, data: Arc<Mutex<Data>>, event_listeners: ListenerSet<PathChildrenCacheEvent>, ops_chan_tx: Sender<Operation>) -> bool {
        let mut done = false;
        
        debug!("handling op {:?}", op);
        match op {
            Operation::Shutdown => {
                info!("shutting down worker thread");
                done = true;
            },
            Operation::Refresh(mode) => {
                debug!("getting children");
                let result = Self::get_children(zk.clone(), &*path, data.clone(), ops_chan_tx.clone(), mode);
                info!("got children {:?}", result);
            },
            Operation::GetData(path) => {
                debug!("getting data");
                let result = Self::update_data(zk.clone(), &*path, data.clone(), ops_chan_tx.clone());
                match result {
                    Err(err) => {
                        warn!("error getting child data: {:?}", err);
                    },
                    _ => {}
                }
            },
            Operation::Event(event) => {
                info!("received event {:?}", event);
                event_listeners.notify(&event);
            },
        }
        
        done
    }

    pub fn start(&mut self) -> ZkResult<()> {
        let (ops_chan_tx, ops_chan_rx) = mpsc::channel();
        let (listener_chan_tx, listener_chan_rx) = mpsc::channel();
        
        let sub = self.zk.add_listener(listener_chan_tx.clone());
        self.listener_subscription = Some(sub);
        
        let zk = self.zk.clone();
        let path = self.path.clone();
        let data = self.data.clone();
        let event_listeners = self.event_listeners.clone();
        self.channel = Some(ops_chan_tx.clone());

        self.worker_thread = Some(thread::spawn(move || {
            let mut done = false;

            while !done {
                select! (
                    state_result = listener_chan_rx.recv() => {
                        match state_result {
                            Ok(state) => {
                                done = Self::handle_state_change(state, ops_chan_tx.clone());
                            },
                            Err(err) => {
                                info!("zk state chan err. shutting down. {:?}", err);
                                done = true;
                            }
                        }
                    },
                    op_result = ops_chan_rx.recv() => {
                        match op_result {
                            Ok(operation) => {
                                done = Self::handle_operation(operation, zk.clone(), path.clone(), data.clone(), event_listeners.clone(), ops_chan_tx.clone());
                            },
                            Err(err) => {
                                info!("error receiving from operations channel ({:?}). shutting down worker thread", err);
                                done = true;                            
                            }
                        }
                    });
            }
        }));
        
        self.offer_operation(Operation::Refresh(RefreshMode::ForceGetDataAndStat))
    }

    pub fn add_listener(&self, subscriber: Sender<PathChildrenCacheEvent>) -> Subscription {
        self.event_listeners.subscribe(subscriber)
    }

    pub fn remove_listener(&self, sub: Subscription) -> () {
        self.event_listeners.unsubscribe(sub)
    }

    fn offer_operation(&self, op: Operation) -> ZkResult<()> {
        match self.channel {
            Some(ref chan) => {
                chan.send(op).map_err(|err| {
                    warn!("error submitting op to channel: {:?}", err);
                    ZkError::APIError
                })
            },
            None => Err(ZkError::APIError)
        }
    }
      
}

pub fn join_path(dir: &str, child: &str) -> String {
    let dir_bytes = dir.as_bytes();
    let mut result = dir.to_string();
    if dir_bytes[dir_bytes.len() - 1] != ('/' as u8) {
        result.push_str("/");
    }
    result.push_str(child);
    result
}
