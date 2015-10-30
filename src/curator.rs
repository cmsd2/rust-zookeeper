use super::zookeeper::{ZooKeeper, ZkResult, ZooKeeperClient};
use super::proto::Acl;
use super::consts::{CreateMode};
use super::retry::*;
use std::time::Duration;
use time;

pub trait Curator : ZooKeeperClient {
    fn build_create<R>(&self, retry: R) -> CreateBuilder<R> where R: RetryPolicy;
    
/*    fn build_delete() -> DeleteBuilder;

    fn build_exists() -> ExistsBuilder;

    fn build_get_acl() -> GetAclBuilder;

    fn build_get_children() -> GetChildrenBuilder;

    fn build_get_data() -> GetDataBuilder;

    fn build_set_acl() -> SetAclBuilder;

    fn build_set_data() -> SetDataBuilder;*/

    fn retry<T, F>(&self, retry_policy: &RetryPolicy, mut fun: F) -> ZkResult<T>
        where F: FnMut() -> ZkResult<T>;
}

pub fn timespec_to_duration(ts: time::Timespec) -> Duration {
    Duration::new(ts.sec as u64, ts.nsec as u32)
}

/// calculates ts1 - ts2
/// returns a std::time::Duration
/// TODO handle underflow/overflow
pub fn timespec_sub(ts1: &time::Timespec, ts2: &time::Timespec) -> Duration {
    let secs = ts1.sec - ts2.sec;
    let nsec = ts1.nsec - ts2.nsec;
    Duration::new(secs as u64, nsec as u32)
}

impl Curator for ZooKeeper {
    fn build_create<'a, R>(&'a self, retry_policy: R) -> CreateBuilder<'a, R>
        where R: RetryPolicy
    {
        CreateBuilder::new(&self, retry_policy)
    }

    fn retry<T, F>(&self, retry_policy: &RetryPolicy, mut fun: F) -> ZkResult<T>
        where F: FnMut() -> ZkResult<T>
    {
        let mut retry_count = 0;
        let start_time = time::now_utc().to_timespec();
        
        loop {
            let result = fun();

            retry_count += 1;

            let retry_time = time::now_utc().to_timespec();
            
            let elapsed_time = timespec_sub(&retry_time, &start_time);
            
            match retry_policy.allow_retry(retry_count, elapsed_time) {
                RetryResult::RetryAfterSleep(sleep_time) => {
                    
                },
                RetryResult::Stop => {
                    return result;
                }
            }
        }
    }
}

pub struct CreateBuilder<'a, R> {
    zk: &'a ZooKeeper,
    acl: Vec<Acl>,
    mode: CreateMode,
    retry_policy: R,
}

/*impl <'a, R> FnOnce<()> for CreateBuilder<'a, R> {
    type Output = ZkResult<String>;
    
    fn call_once(&self, _arg: ()) -> ZkResult<String> {
        unimplemented!()
    }
}

impl <'a, R> FnMut<()> for CreateBuilder<'a, R> {
    fn call_mut(&mut self) -> ZkResult<String> {
        unimplemented!()
    }
}*/

impl <'a, R> CreateBuilder<'a, R> where R: RetryPolicy {
    pub fn new(zk: &'a ZooKeeper, retry: R) -> CreateBuilder<'a, R> {
        CreateBuilder {
            zk: zk,
            acl: vec![],
            mode: CreateMode::Persistent,
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

    pub fn for_path(self, path: &str, data: Vec<u8>) -> ZkResult<String> {
        self.zk.retry(&self.retry_policy, || {
            self.zk.create(path, data.clone(), self.acl.clone(), self.mode)
        })
    }
}
