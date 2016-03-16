use super::zookeeper::{ZooKeeper, ZkResult, ZooKeeperClient};
use super::proto::{Acl, Stat, WatchedEvent};
use super::consts::{CreateMode, ZkError};
use super::retry::*;
use super::paths;
use uuid::Uuid;
use super::ZooKeeperExt;
use std::sync::mpsc::*;
use std::sync::Mutex;
use std::sync::Arc;

#[derive(Copy, Clone, Debug)]
pub enum CuratorError {
    ZkError(ZkError),
    Timeout,
}

impl From<ZkError> for CuratorError {
    fn from(err: ZkError) -> CuratorError {
        CuratorError::ZkError(err)
    }
}

pub type CuratorResult<T> = Result<T, CuratorError>;

pub trait Curator : ZooKeeperClient {
    fn build_create<R>(&self, retry: R) -> CreateBuilder<R> where R: RetryPolicy;

    fn build_exists<R, W>(&self, retry: R) -> ExistsBuilder<R, W> where R: RetryPolicy, W : Fn(WatchedEvent) + Send + 'static;
    
    fn build_delete<R>(&self, retry: R) -> DeleteBuilder<R> where R: RetryPolicy;

    fn build_get_acl<R>(&self, retry: R) -> GetAclBuilder<R> where R: RetryPolicy;

    fn build_get_children<R, W>(&self, retry: R) -> GetChildrenBuilder<R, W> where R: RetryPolicy, W: Fn(WatchedEvent) + Send + 'static;

    fn build_get_data<R, W>(&self, retry: R) -> GetDataBuilder<R, W> where R: RetryPolicy, W: Fn(WatchedEvent) + Send + 'static;

    fn build_set_acl<R>(&self, retry: R) -> SetAclBuilder<R> where R: RetryPolicy;

    fn build_set_data<R>(&self, retry: R) -> SetDataBuilder<R> where R: RetryPolicy;

    fn retry<P, F, T>(&self, retry_policy: &P, mut fun: F) -> CuratorResult<T>
        where P: RetryPolicy, F: FnMut() -> ZkResult<T>;
}


impl Curator for ZooKeeper {
    fn build_create<'a, R>(&'a self, retry_policy: R) -> CreateBuilder<'a, R>
        where R: RetryPolicy
    {
        CreateBuilder::new(&self, retry_policy)
    }

    fn build_exists<'a, R, W>(&'a self, retry_policy: R) -> ExistsBuilder<'a, R, W>
        where R: RetryPolicy, W: Fn(WatchedEvent) + Send + 'static
    {
        ExistsBuilder::new(&self, retry_policy)
    }

    fn build_delete<'a, R>(&'a self, retry_policy: R) -> DeleteBuilder<'a, R>
        where R: RetryPolicy
    {
        DeleteBuilder::new(&self, retry_policy)
    }

    fn build_get_acl<'a, R>(&'a self, retry_policy: R) -> GetAclBuilder<'a, R>
        where R: RetryPolicy
    {
        GetAclBuilder::new(&self, retry_policy)
    }

    fn build_get_children<'a, R, W>(&'a self, retry_policy: R) -> GetChildrenBuilder<'a, R, W>
        where R: RetryPolicy, W: Fn(WatchedEvent) + Send + 'static
    {
        GetChildrenBuilder::new(&self, retry_policy)
    }

    fn build_get_data<'a, R, W>(&'a self, retry_policy: R) -> GetDataBuilder<'a, R, W>
        where R: RetryPolicy, W: Fn(WatchedEvent) + Send + 'static
    {
        GetDataBuilder::new(&self, retry_policy)
    }

    fn build_set_acl<'a, R>(&'a self, retry_policy: R) -> SetAclBuilder<'a, R>
        where R: RetryPolicy
    {
        SetAclBuilder::new(&self, retry_policy)
    }

    fn build_set_data<'a, R>(&'a self, retry_policy: R) -> SetDataBuilder<'a, R>
        where R: RetryPolicy
    {
        SetDataBuilder::new(&self, retry_policy)
    }

    fn retry<P, F, T>(&self, retry_policy: &P, fun: F) -> CuratorResult<T>
        where P: RetryPolicy,
              F: FnMut() -> ZkResult<T>
    
    {
        match RetryLoop::call_with_retry(retry_policy, fun) {
            Ok(result) => {
                match result {
                    Some(value) => Ok(value),
                    None => Err(CuratorError::Timeout),
                }
            },
            Err(err) => Err(CuratorError::ZkError(err)),
        }
    }
}

pub struct CreateBuilder<'a, R> {
    zk: &'a ZooKeeper,
    acl: Vec<Acl>,
    mode: CreateMode,
    create_parents: bool,
    protection: bool,
    retry_policy: R,
}

impl <'a, R> CreateBuilder<'a, R> where R: RetryPolicy {
    pub fn new(zk: &'a ZooKeeper, retry: R) -> CreateBuilder<'a, R> {
        CreateBuilder {
            zk: zk,
            acl: vec![],
            mode: CreateMode::Persistent,
            create_parents: false,
            protection: false,
            retry_policy: retry,
        }
    }

    pub fn with_acl(mut self, acl: Vec<Acl>) -> CreateBuilder<'a, R> {
        self.acl = acl;
        self
    }

    pub fn with_mode(mut self, mode: CreateMode) -> CreateBuilder<'a, R> {
        self.mode = mode;
        self
    }

    pub fn with_create_parents(mut self, create_parents: bool) -> CreateBuilder<'a, R> {
        self.create_parents = create_parents;
        self
    }

    pub fn with_protection(mut self, protection: bool) -> CreateBuilder<'a, R> {
        self.protection = protection;
        self
    }

    fn calculate_destination(&self, path: &'a str) -> (&'a str, String, Option<String>) {
        let (base_path, node_name) = paths::split_path(path);

        if self.protection {
            let hyphenated_uuid = Uuid::new_v4().to_hyphenated_string();
            let protected_node_name = format!("_c_{}-{}", hyphenated_uuid, node_name);

            (base_path, protected_node_name, Some(hyphenated_uuid))
        } else {
            (base_path, node_name.to_owned(), None)
        }
    }

    pub fn for_path(self, path: &str, data: Vec<u8>) -> CuratorResult<String> {
        let mut attempt = 0;
        let mut created_path = None;

        let (base_path, node_name, protection_id) = self.calculate_destination(path);
        let p_id_ref = &protection_id;

        let path = paths::make_path(&base_path, &node_name);

        self.zk.retry(&self.retry_policy, || {
            if attempt != 0 && self.protection {
                created_path = try!(self.find_protected_node(&base_path, p_id_ref.as_ref().unwrap()));
            }

            if created_path.is_some() {
                Ok(created_path.clone().unwrap())
            } else {
                attempt += 1;
            
                let result = self.zk.create(&path, data.clone(), self.acl.clone(), self.mode);

                match result {
                    Ok(created_path_result) => Ok(created_path_result),
                    Err(ZkError::NoNode) => {
                        trace!("handling NoNode after create...");
                        if self.create_parents {
                            try!(self.zk.ensure_path(base_path));

                            self.zk.create(&path, data.clone(), self.acl.clone(), self.mode)
                        } else {
                            Err(ZkError::NoNode)
                        }
                    },
                    Err(err) => Err(err)
                }
            }
        })
    }

    fn find_protected_node(&self, path: &str, hyphenated_uuid: &str) -> ZkResult<Option<String>> {
        let (dir, _protected_name_unsequenced) = paths::split_path(path);
        debug!("recovering protected name from path {} for uuid {} in dir {}", path, hyphenated_uuid, dir);

        debug!("getting children in {:?}", dir);
        
        match self.zk.get_children(dir, false) {
            Ok(children) => {
                debug!("found children {:?}", children);
                for child in children {
                    debug!("maching child node {}", child);
                    match paths::split_protected_name(&child) {
                        Some((uuid, _sequenced_name)) => {
                            if uuid == hyphenated_uuid {
                                debug!("recovered protected name {}", child);
                                return Ok(Some(paths::make_path(path, &child)));
                            }
                        },
                        None => {
                        }
                    }
                }
            },
            Err(err) => {
                info!("couldn't get children for {:?}: {:?}", dir, err);
            }
        }

        Ok(None)
    }
}

pub struct ExistsBuilder<'a, R, W>
    where W: Fn(WatchedEvent) + Send + 'static
{
    zk: &'a ZooKeeper,
    retry_policy: R,
    watch: bool,
    watcher: Option<Sender<WatchedEvent>>,
    watcher_fn: Option<W>,
}

impl <'a, R, W> ExistsBuilder<'a, R, W> where R: RetryPolicy, W : Fn(WatchedEvent) + Send + 'static {
    pub fn new(zk: &'a ZooKeeper, retry: R) -> ExistsBuilder<'a, R, W> {
        ExistsBuilder {
            zk: zk,
            retry_policy: retry,
	    watch: false,
	    watcher: None,
            watcher_fn: None,
        }
    }

    pub fn with_watch(mut self, watch: bool) -> ExistsBuilder<'a, R, W> {
        self.watch = watch;
	self
    }

    pub fn with_watcher(mut self, watcher: Option<Sender<WatchedEvent>>) -> ExistsBuilder<'a, R, W> {
        self.watcher = watcher;
	self
    }

    pub fn with_watcher_fn(mut self, watcher_fn: Option<W>) -> ExistsBuilder<'a, R, W> {
        self.watcher_fn = watcher_fn;
        self
    }

    pub fn for_path(self, path: &str) -> CuratorResult<Option<Stat>> {
        if self.watcher.is_some() {
            let maybe_watcher = self.watcher.as_ref();
	
            self.zk.retry(&self.retry_policy, || {
                let ws: Sender<WatchedEvent> = maybe_watcher.unwrap().clone();

                self.zk.exists_w(path, move |ev: WatchedEvent| {

                    if let Err(err) = ws.send(ev.clone()) {
		        debug!("error sending watched event to channel: {:?}", err);
		    } else {
                        debug!("sent watched event to channel");
                    }
	        
                }).map(|x| Some(x))
            })
        } else if self.watcher_fn.is_some() {
            let watcher_mut = Arc::new(Mutex::new(self.watcher_fn.unwrap()));
            let zk = self.zk; 

            self.zk.retry(&self.retry_policy, move || {
                let w_c = watcher_mut.clone();
                zk.exists_w(path, move |ev: WatchedEvent| {
                    let w = w_c.lock().unwrap();

                    w(ev.clone());
                }).map(|x| Some(x))
            })
	} else {
	    self.zk.retry(&self.retry_policy, || {
	        self.zk.exists(path, self.watch)
	    })
	}
    }
}

pub struct DeleteBuilder<'a, R> {
    zk: &'a ZooKeeper,
    retry_policy: R,
    version: i32,
}

impl <'a, R> DeleteBuilder<'a, R> where R: RetryPolicy {
    pub fn new(zk: &'a ZooKeeper, retry: R) -> DeleteBuilder<'a, R> {
        DeleteBuilder {
            zk: zk,
            retry_policy: retry,
            version: 0,
        }
    }

    pub fn with_version(mut self, version: i32) -> DeleteBuilder<'a, R> {
        self.version = version;
        self
    }

    pub fn for_path(self, path: &str) -> CuratorResult<()> {
        self.zk.retry(&self.retry_policy, || {
            self.zk.delete(path, self.version)  
        })
    }
}

pub struct GetAclBuilder<'a, R> {
    zk: &'a ZooKeeper,
    retry_policy: R,
}

impl <'a, R> GetAclBuilder<'a, R> where R: RetryPolicy {
    pub fn new(zk: &'a ZooKeeper, retry: R) -> GetAclBuilder<'a, R> {
        GetAclBuilder {
            zk: zk,
            retry_policy: retry,
        }
    }

    pub fn for_path(self, path: &str) -> CuratorResult<(Vec<Acl>, Stat)> {
        self.zk.retry(&self.retry_policy, || {
            self.zk.get_acl(path)
        })
    }
}

pub struct GetChildrenBuilder<'a, R, W>
    where W: Fn(WatchedEvent) + Send + 'static
{
    zk: &'a ZooKeeper,
    retry_policy: R,
    watch: bool,
    watcher: Option<Sender<WatchedEvent>>,
    watcher_fn: Option<W>,
}

impl <'a, R, W> GetChildrenBuilder<'a, R, W> where R: RetryPolicy, W: Fn(WatchedEvent) + Send + 'static {
    pub fn new(zk: &'a ZooKeeper, retry: R) -> GetChildrenBuilder<'a, R, W> {
        GetChildrenBuilder {
            zk: zk,
            retry_policy: retry,
            watch: false,
            watcher: None,
            watcher_fn: None,
        }
    }

    pub fn with_watch(mut self, watch: bool) -> GetChildrenBuilder<'a, R, W> {
        self.watch = watch;
	self
    }

    pub fn with_watcher(mut self, watcher: Option<Sender<WatchedEvent>>) -> GetChildrenBuilder<'a, R, W> {
        self.watcher = watcher;
	self
    }

    pub fn with_watcher_fn(mut self, watcher_fn: Option<W>) -> GetChildrenBuilder<'a, R, W> {
        self.watcher_fn = watcher_fn;
        self
    }

    pub fn for_path(self, path: &str) -> CuratorResult<Vec<String>> {

        if self.watcher.is_some() {
            let maybe_watcher = self.watcher.as_ref();
	
            self.zk.retry(&self.retry_policy, || {
                let ws: Sender<WatchedEvent> = maybe_watcher.unwrap().clone();

                self.zk.get_children_w(path, move |ev: WatchedEvent| {

                    if let Err(err) = ws.send(ev.clone()) {
		        debug!("error sending watched event to channel: {:?}", err);
		    } else {
                        debug!("sent watched event to channel");
                    }
	        
                })
            })
        } else if self.watcher_fn.is_some() {
            let watcher_mut = Arc::new(Mutex::new(self.watcher_fn.unwrap()));
            let zk = self.zk; 

            self.zk.retry(&self.retry_policy, move || {
                let w_c = watcher_mut.clone();
                zk.get_children_w(path, move |ev: WatchedEvent| {
                    let w = w_c.lock().unwrap();

                    w(ev.clone());
                })
            })
	} else {
	    self.zk.retry(&self.retry_policy, || {
	        self.zk.get_children(path, self.watch)
	    })
	}
    }
}

pub struct GetDataBuilder<'a, R, W> {
    zk: &'a ZooKeeper,
    retry_policy: R,
    watch: bool,
    watcher: Option<Sender<WatchedEvent>>,
    watcher_fn: Option<W>
}

impl <'a, R, W> GetDataBuilder<'a, R, W> where R: RetryPolicy, W: Fn(WatchedEvent) + Send + 'static {
    pub fn new(zk: &'a ZooKeeper, retry: R) -> GetDataBuilder<'a, R, W> {
        GetDataBuilder {
            zk: zk,
            retry_policy: retry,
            watch: false,
            watcher: None,
            watcher_fn: None,
        }
    }

    pub fn with_watch(mut self, watch: bool) -> GetDataBuilder<'a, R, W> {
        self.watch = watch;
	self
    }

    pub fn with_watcher(mut self, watcher: Option<Sender<WatchedEvent>>) -> GetDataBuilder<'a, R, W> {
        self.watcher = watcher;
	self
    }

    pub fn with_watcher_fn(mut self, watcher_fn: Option<W>) -> GetDataBuilder<'a, R, W> {
        self.watcher_fn = watcher_fn;
	self
    }

    pub fn for_path(self, path: &str) -> CuratorResult<(Vec<u8>, Stat)> {

        if self.watcher.is_some() {
            let maybe_watcher = self.watcher.as_ref();
	
            self.zk.retry(&self.retry_policy, || {
                let ws: Sender<WatchedEvent> = maybe_watcher.unwrap().clone();

                self.zk.get_data_w(path, move |ev: WatchedEvent| {

                    if let Err(err) = ws.send(ev.clone()) {
		        debug!("error sending watched event to channel: {:?}", err);
		    } else {
                        debug!("sent watched event to channel");
                    }
	        
                })
            })
        } else if self.watcher_fn.is_some() {
            let watcher_mut = Arc::new(Mutex::new(self.watcher_fn.unwrap()));
            let zk = self.zk; 

            self.zk.retry(&self.retry_policy, move || {
                let w_c = watcher_mut.clone();
                zk.get_data_w(path, move |ev: WatchedEvent| {
                    let w = w_c.lock().unwrap();

                    w(ev.clone());
                })
            })
	} else {
	    self.zk.retry(&self.retry_policy, || {
	        self.zk.get_data(path, self.watch)
	    })
	}
    }
}

pub struct SetAclBuilder<'a, R> {
    zk: &'a ZooKeeper,
    retry_policy: R,
    version: i32,
    acl: Vec<Acl>,
}

impl <'a, R> SetAclBuilder<'a, R> where R: RetryPolicy {
    pub fn new(zk: &'a ZooKeeper, retry: R) -> SetAclBuilder<'a, R> {
        SetAclBuilder {
            zk: zk,
            retry_policy: retry,
            version: 0,
            acl: vec![],
        }
    }

    pub fn with_version(mut self, version: i32) -> SetAclBuilder<'a, R> {
        self.version = version;
        self
    }

    pub fn with_acl(mut self, acl: Vec<Acl>) -> SetAclBuilder<'a, R> {
        self.acl = acl;
        self
    }

    pub fn for_path(self, path: &str) -> CuratorResult<Stat> {
        self.zk.retry(&self.retry_policy, || {
            self.zk.set_acl(path, self.acl.clone(), self.version)
        })
    }
}

pub struct SetDataBuilder<'a, R> {
    zk: &'a ZooKeeper,
    retry_policy: R,
    version: i32,
    data: Vec<u8>,
}

impl <'a, R> SetDataBuilder<'a, R> where R: RetryPolicy {
    pub fn new(zk: &'a ZooKeeper, retry: R) -> SetDataBuilder<'a, R> {
        SetDataBuilder {
            zk: zk,
            retry_policy: retry,
            version: 0,
            data: vec![],
        }
    }

    pub fn with_version(mut self, version: i32) -> SetDataBuilder<'a, R> {
        self.version = version;
        self
    }

    pub fn with_data(mut self, data: Vec<u8>) -> SetDataBuilder<'a, R> {
        self.data = data;
        self
    }

    pub fn for_path(self, path: &str) -> CuratorResult<Stat> {
        self.zk.retry(&self.retry_policy, || {
            self.zk.set_data(path, self.data.clone(), self.version)  
        })
    }
}
