#![deny(unused_mut)]
extern crate zookeeper;
#[macro_use]
extern crate log;
extern crate env_logger;

use std::io;
use std::sync::Arc;
use std::time::Duration;
use std::thread;
use std::env;
use zookeeper::{Watcher, WatchedEvent, ZooKeeper, ZooKeeperClient};
use zookeeper::retry::RetryForever;
use zookeeper::recipes::mutex::InterProcessMutex;
use zookeeper::recipes::locks::InterProcessLock;

struct LoggingWatcher;
impl Watcher for LoggingWatcher {
    fn handle(&self, e: WatchedEvent) {
        info!("{:?}", e)
    }
}

fn zk_server_urls() -> String {
    let key = "ZOOKEEPER_SERVERS";
    match env::var(key) {
        Ok(val) => val,
        Err(_) => "localhost:2181".to_string(),
    }
}


fn mutex_example() {
    let zk_urls = zk_server_urls();
    let retry = RetryForever::new(Duration::from_millis(2000));
    
    println!("connecting to {}", zk_urls);

    let zk = ZooKeeper::connect(&*zk_urls, Duration::from_secs(5), LoggingWatcher).unwrap();

    let mut tmp = String::new();

    let auth = zk.add_auth("digest", vec![1, 2, 3, 4]);

    println!("authenticated -> {:?}", auth);

    let zk_arc = Arc::new(zk);

    let m = Arc::new(InterProcessMutex::new(zk_arc.clone(), "/", "test_mutex", 1));

    println!("acquiring mutex first time");
    let result_1 = m.acquire(None).ok();
    println!("result: {:?}", result_1);

    println!("acquiring mutex second time");
    let result_2 = m.acquire(None).ok();
    println!("result: {:?}", result_2);

    println!("releasing mutex first time");
    let result_3 = m.release().ok();
    println!("result: {:?}", result_3);

    let m_captured = m.clone();
    let m_acquire = thread::spawn(move || {
        let result = m_captured.acquire(None).ok();
        println!("should be able to acquire in separate thread after it becomes available: {:?}", result);
    });

    let m_captured_2 = m.clone();
    let m_acquire_2 = thread::spawn(move || {
        let result = m_captured_2.acquire(Some(Duration::from_millis(100))).ok();
        println!("should time out if not available: {:?}", result);
    });

    println!("mutex acquire failure should timeout...");
    m_acquire_2.join().unwrap();
    println!("mutex timeout ok");
    
    println!("press enter to close client");
    io::stdin().read_line(&mut tmp).unwrap();

    println!("releasing mutex final time");
    let result_4 = m.release();
    println!("result: {:?}", result_4);

    println!("mutex acquire should succeed after it becomes available");
    m_acquire.join().unwrap();
    println!("mutex acquire succeeded");
}

fn main() {
    env_logger::init().unwrap();
    mutex_example();
}
