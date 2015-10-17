use zookeeper::CreateMode::*;
use zookeeper::{WatchedEvent, ZooKeeper, ZooKeeperExt};
use zookeeper::acls::*;
use zookeeper::recipes::mutex::InterProcessMutex;

use ZkCluster;

use std::sync::Arc;
use std::time::Duration;
use env_logger;

    
#[test]
pub fn test_mutex() {
    let _ = env_logger::init();

    let cluster = ZkCluster::start(1);
    
    let zk = Arc::new(ZooKeeper::connect(&cluster.connect_string,
                                         Duration::from_secs(30),
                                         |event: &WatchedEvent| info!("{:?}", event)).unwrap());


    zk.ensure_path("/mutex").unwrap();
    
    let mutex = InterProcessMutex::new(zk.clone(), "/mutex", "a", 10);
    

}
