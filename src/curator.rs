use super::zookeeper::{ZooKeeper, ZkResult, ZooKeeperClient};
use super::proto::Acl;
use super::consts::{CreateMode, ZkError};
use super::retry::*;
use super::paths;
use uuid::Uuid;
use super::ZooKeeperExt;

pub trait Curator : ZooKeeperClient {
    fn build_create<R>(&self, retry: R) -> CreateBuilder<R> where R: RetryPolicy;
    
/*    fn build_delete() -> DeleteBuilder;

    fn build_exists() -> ExistsBuilder;

    fn build_get_acl() -> GetAclBuilder;

    fn build_get_children() -> GetChildrenBuilder;

    fn build_get_data() -> GetDataBuilder;

    fn build_set_acl() -> SetAclBuilder;

    fn build_set_data() -> SetDataBuilder;*/

    fn retry<P, F, T>(&self, retry_policy: &P, mut fun: F) -> ZkResult<Option<T>>
        where P: RetryPolicy, F: FnMut() -> ZkResult<T>;
}


impl Curator for ZooKeeper {
    fn build_create<'a, R>(&'a self, retry_policy: R) -> CreateBuilder<'a, R>
        where R: RetryPolicy
    {
        CreateBuilder::new(&self, retry_policy)
    }

    fn retry<P, F, T>(&self, retry_policy: &P, fun: F) -> ZkResult<Option<T>>
        where P: RetryPolicy,
              F: FnMut() -> ZkResult<T>
    
    {
        RetryLoop::call_with_retry(retry_policy, fun)
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

    pub fn for_path(self, path: &str, data: Vec<u8>) -> ZkResult<Option<String>> {
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
