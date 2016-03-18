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
use std::sync::mpsc;
use zookeeper::{CreateMode, Watcher, WatchedEvent, ZooKeeper, ZooKeeperClient};
use zookeeper::acls;
use zookeeper::recipes::cache::PathChildrenCache;
use zookeeper::retry::RetryForever;
use zookeeper::curator::Curator;
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


fn zk_example() {
    let zk_urls = zk_server_urls();
    let retry = RetryForever::new(Duration::from_millis(2000));
    
    println!("connecting to {}", zk_urls);

    let zk = ZooKeeper::connect(&*zk_urls, Duration::from_secs(5), LoggingWatcher).unwrap();

    let mut tmp = String::new();

    let auth = zk.add_auth("digest", vec![1, 2, 3, 4]);

    println!("authenticated -> {:?}", auth);

    let path = zk.create("/test",
                         vec![1, 2],
                         acls::OPEN_ACL_UNSAFE.clone(),
                         CreateMode::Ephemeral);

    println!("created -> {:?}", path);

    println!("press enter to perform create_p");
    io::stdin().read_line(&mut tmp).unwrap();
    
    let path_seq = zk.build_create(retry)
        .with_acl(acls::OPEN_ACL_UNSAFE.clone())
        .with_mode(CreateMode::EphemeralSequential)
        .for_path("/test_seq", vec![1,2]);

    println!("created -> {:?}", path_seq);

    println!("press enter to continue past create_p");
    io::stdin().read_line(&mut tmp).unwrap();
    
    let exists = zk.exists("/test", true);

    println!("exists -> {:?}", exists);
    
    let path2 = zk.build_create(retry)
        .with_acl(acls::OPEN_ACL_UNSAFE.clone())
        .with_mode(CreateMode::Ephemeral)
        .with_create_parents(true)
        .for_path("/test2", vec![1, 2]);

    println!("created -> {:?}", path2);

    let path3 = zk.build_create(retry)
        .with_acl(acls::OPEN_ACL_UNSAFE.clone())
        .with_mode(CreateMode::Ephemeral)
        .with_create_parents(true)
        .with_protection(true)
        .for_path("/test3", vec![1, 2]);

    println!("created -> {:?}", path3);
    
    let doesnt_exist = zk.exists("/blabla", true);

    println!("don't exists path -> {:?}", doesnt_exist);

    let get_acl = zk.get_acl("/test");

    println!("get_acl -> {:?}", get_acl);

    let set_acl = zk.set_acl("/test", acls::OPEN_ACL_UNSAFE.clone(), -1);

    println!("set_acl -> {:?}", set_acl);

    let children = zk.get_children("/", true);

    println!("children of / -> {:?}", children);

    let set_data = zk.set_data("/test", vec![6, 5, 4, 3], -1);

    println!("set_data -> {:?}", set_data);

    let get_data = zk.get_data("/test", true);

    println!("get_data -> {:?}", get_data);

    // let delete = zk.delete("/test", -1);

    // println!("deleted /test -> {:?}", delete);

    let watch_children = zk.get_children_w("/", |event: WatchedEvent| {
        println!("watched event {:?}", event);
    });

    println!("watch children -> {:?}", watch_children);

    let zk_arc = Arc::new(zk);

    let mut pcc = PathChildrenCache::new(zk_arc.clone(), "/", retry).unwrap();

    match pcc.start() {
        Err(err) => {
            println!("error starting cache: {:?}", err);
            return;
        }
        _ => {
            println!("cache started");
        }
    }

    let (ev_tx, ev_rx) = mpsc::channel();
    pcc.add_listener(move |e| ev_tx.send(e).unwrap());
    thread::spawn(move || {
        for ev in ev_rx {
            println!("received event {:?}", ev);
        }
    });

    let m = Arc::new(InterProcessMutex::new(zk_arc.clone(), "/", "test_mutex", 1, retry));

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

    println!("releasing mutex second time");
    let result_4 = m.release().ok();
    println!("released result: {:?}", result_4);
    m_acquire.join().unwrap();

    // The client can be shared between tasks
    let zk_arc_captured = zk_arc.clone();
    thread::spawn(move || {
        zk_arc_captured.close().unwrap();

        // And operations return error after closed
        match zk_arc_captured.exists("/test", false) {
            Err(err) => println!("Usage after closed should end up with error: {:?}", err),
            Ok(_) => panic!("Shouldn't happen"),
        }
    });

    println!("press enter to exit example");
    io::stdin().read_line(&mut tmp).unwrap();
}

fn main() {
    env_logger::init().unwrap();
    zk_example();
}
