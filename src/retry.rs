use std::time::Duration;
use std::cmp;
use rand;
use rand::Rng;

pub enum RetryResult {
    RetryAfterSleep(Duration),
    Stop,
}

pub trait RetryPolicy {
    fn allow_retry(&self, retry_count: u32, elapsed_time: Duration) -> RetryResult;
}

trait SleepingRetry {
    fn get_max_retries(&self) -> u32;
    fn get_sleep_time(&self, retry_count: u32, elapsed_time: Duration) -> Duration;
}

impl RetryPolicy for SleepingRetry {
    fn allow_retry(&self, retry_count: u32, elapsed_time: Duration) -> RetryResult {
        if retry_count < self.get_max_retries() {
            RetryResult::RetryAfterSleep(self.get_sleep_time(retry_count, elapsed_time))
        } else {
            RetryResult::Stop
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct RetryForever {
    retry_interval: Duration,
}

impl RetryForever {
    pub fn new(retry_interval: Duration) -> RetryForever {
        RetryForever {
            retry_interval: retry_interval,
        }
    }
}

impl RetryPolicy for RetryForever {
    fn allow_retry(&self, _retry_count: u32, _elapsed_time: Duration) -> RetryResult {
        RetryResult::RetryAfterSleep(self.retry_interval)
    }
}

#[derive(Copy, Clone, Debug)]
pub struct RetryNTimes {
    max_retries: u32,
    retry_interval: Duration,
}

impl RetryNTimes {
    pub fn new(max_retries: u32, retry_interval: Duration) -> RetryNTimes {
        RetryNTimes {
            max_retries: max_retries,
            retry_interval: retry_interval,
        }
    }
}

impl SleepingRetry for RetryNTimes {
    fn get_max_retries(&self) -> u32 {
        self.max_retries
    }

    fn get_sleep_time(&self, _retry_count: u32, _elapsed_time: Duration) -> Duration {
        self.retry_interval
    }
}

#[derive(Copy, Clone, Debug)]
pub struct RetryOneTime {
    retry_n_times: RetryNTimes,
}

impl RetryOneTime {
    pub fn new(retry_interval: Duration) -> RetryOneTime {
        RetryOneTime {
            retry_n_times: RetryNTimes::new(1, retry_interval),
        }
    }
}

impl SleepingRetry for RetryOneTime {
    fn get_max_retries(&self) -> u32 {
        1
    }

    fn get_sleep_time(&self, retry_count: u32, elapsed_time: Duration) -> Duration {
        self.retry_n_times.get_sleep_time(retry_count, elapsed_time)
    }
}

#[derive(Copy, Clone, Debug)]
pub struct RetryUntilElapsed {
    max_elapsed_time: Duration,
    retry_forever: RetryForever
}

impl RetryUntilElapsed {
    pub fn new(max_elapsed_time: Duration, retry_interval: Duration) -> RetryUntilElapsed {
        RetryUntilElapsed {
            max_elapsed_time: max_elapsed_time,
            retry_forever: RetryForever::new(retry_interval),
        }
    }
}

impl RetryPolicy for RetryUntilElapsed {
    fn allow_retry(&self, retry_count: u32, elapsed_time: Duration) -> RetryResult {
        if elapsed_time.lt(&self.max_elapsed_time) {
            self.retry_forever.allow_retry(retry_count, elapsed_time)
        } else {
            RetryResult::Stop
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct RetryExponentialBackoff {
    base_sleep_time: Duration,
    max_retries: u32,
    max_sleep: Duration,
}

impl RetryExponentialBackoff {
    pub fn new(base_sleep_time: Duration, max_retries: u32, max_sleep: Duration) -> RetryExponentialBackoff {
        RetryExponentialBackoff {
            base_sleep_time: base_sleep_time,
            max_retries: max_retries,
            max_sleep: max_sleep,
        }
    }
}

impl SleepingRetry for RetryExponentialBackoff {
    fn get_max_retries(&self) -> u32 {
        self.max_retries
    }

    fn get_sleep_time(&self, retry_count: u32, elapsed_time: Duration) -> Duration {
        let mut retry_interval = self.base_sleep_time * cmp::max(1, rand::thread_rng().gen_range(1, 1 << (retry_count + 1)));

        if retry_interval.gt(&self.max_sleep) {
            warn!("Sleep extension too large ({:?}). Pinning to {:?}", retry_interval, self.max_sleep);
            retry_interval = self.max_sleep;
        }

        retry_interval
    }
}

#[derive(Copy, Clone, Debug)]
pub struct RetryBoundedExponentialBackoff {
    max_sleep_time: Duration,
    exponential_backoff: RetryExponentialBackoff,
}

impl RetryBoundedExponentialBackoff {
    pub fn new(base_sleep_time: Duration, max_sleep_time: Duration, max_retries: u32, max_sleep: Duration) -> RetryBoundedExponentialBackoff {
        RetryBoundedExponentialBackoff {
            max_sleep_time: max_sleep_time,
            exponential_backoff: RetryExponentialBackoff::new(
                base_sleep_time,
                max_retries,
                max_sleep,
            )
        }
    }
}

impl SleepingRetry for RetryBoundedExponentialBackoff {
    fn get_max_retries(&self) -> u32 {
        self.exponential_backoff.get_max_retries()
    }

    fn get_sleep_time(&self, retry_count: u32, elapsed_time: Duration) -> Duration {
        let retry_interval = self.exponential_backoff.get_sleep_time(retry_count, elapsed_time);

        if retry_interval.gt(&self.max_sleep_time) {
            self.max_sleep_time
        } else {
            retry_interval
        }
    }
}

