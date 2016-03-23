use acls::*;
use consts::{CreateMode, ZkApiError};
use zookeeper::{ZooKeeperClient};
use zkresult::*;
use std::iter::once;

pub trait ZooKeeperExt {
    fn ensure_path(&self, path: &str) -> ZkResult<()>;
}

impl<Z> ZooKeeperExt for Z where Z : ZooKeeperClient {
    fn ensure_path(&self, path: &str) -> ZkResult<()> {
        for (i, _) in path.chars().chain(once('/')).enumerate().skip(1).filter(|c| c.1 == '/') {
            match self.create(&path[..i],
                              vec![],
                              OPEN_ACL_UNSAFE.clone(),
                              CreateMode::Persistent) {
                Ok(_) | Err(ZkError::ApiError(ZkApiError::NodeExists)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}
