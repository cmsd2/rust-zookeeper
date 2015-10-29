use super::zookeeper::{ZooKeeper, ZkResult, ZooKeeperClient};
use super::proto::Acl;
use super::consts::{CreateMode};
use super::retry::*;


pub trait Curator : ZooKeeperClient {
    fn build_create(&self) -> CreateBuilder;
    
/*    fn build_delete() -> DeleteBuilder;

    fn build_exists() -> ExistsBuilder;

    fn build_get_acl() -> GetAclBuilder;

    fn build_get_children() -> GetChildrenBuilder;

    fn build_get_data() -> GetDataBuilder;

    fn build_set_acl() -> SetAclBuilder;

    fn build_set_data() -> SetDataBuilder;*/
}

impl Curator for ZooKeeper {
    fn build_create<'a>(&'a self) -> CreateBuilder<'a> {
        CreateBuilder::new(&self)
    }
}

pub struct CreateBuilder<'a> {
    curator: &'a ZooKeeper,
    acl: Vec<Acl>,
    mode: CreateMode,
}

impl <'a> CreateBuilder<'a> {
    pub fn new(curator: &'a ZooKeeper) -> CreateBuilder<'a> {
        CreateBuilder {
            curator: curator,
            acl: vec![],
            mode: CreateMode::Persistent,
        }
    }

    pub fn with_acl(mut self, acl: Vec<Acl>) -> CreateBuilder<'a> {
        self.acl = acl;
        self
    }

    pub fn with_mode(mut self, mode: CreateMode) -> CreateBuilder<'a> {
        self.mode = mode;
        self
    }

    pub fn for_path(self, path: &str, data: Vec<u8>) -> ZkResult<String> {
        self.curator.create(path, data, self.acl, self.mode)
    }
}
