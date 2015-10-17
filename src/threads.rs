
#[cfg(not(target_os = "windows"))]
mod unix {
    #[derive(PartialEq, Debug, Copy, Clone)]
    pub struct ThreadId {
        pthread_id: u64,
    }

    use libc;
    
    extern {
        fn pthread_self() -> libc::pthread_t;
    }

    impl ThreadId {
        pub fn current_thread_id() -> ThreadId {
            ThreadId {
                pthread_id: unsafe { pthread_self() },
            }
        }
    }
}

#[cfg(not(target_os = "windows"))]
pub use threads::unix::*;

