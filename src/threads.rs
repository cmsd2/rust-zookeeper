

#[cfg(not(target_os = "windows"))]
mod unix {
    use libc::pthread_self;

    #[derive(PartialEq, Debug, Copy, Clone)]
    pub struct ThreadId {
        thread_id: u64,
    }

    impl ThreadId {
        pub fn current_thread_id() -> ThreadId {
            ThreadId {
                thread_id: unsafe { pthread_self() },
            }
        }
    }
}

#[cfg(target_os = "windows")]
mod win {
    use kernel32::GetCurrentThreadId;

    #[derive(PartialEq, Debug, Copy, Clone)]
    pub struct ThreadId {
        thread_id: u64,
    }
    
    impl ThreadId {
        pub fn current_thread_id() -> ThreadId {
            ThreadId {
                thread_id: unsafe { GetCurrentThreadId() as u64},
            }
        }
    }
}

#[cfg(not(target_os = "windows"))]
pub use threads::unix::*;

#[cfg(target_os = "windows")]
pub use threads::win::*;
