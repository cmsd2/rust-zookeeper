use std::time::Duration;
use std::cmp;
use rand;
use rand::Rng;
use schedule_recv as timer;
use time;
use time_ext::*;
use consts::*;
use zkresult::*;

pub enum RetryResult {
    RetryAfterSleep(Duration),
    Stop,
}

pub trait RetryPolicy {
    fn allow_retry(&self, retry_count: u32, elapsed_time: Duration) -> RetryResult;
}

pub trait SleepingRetry {
    fn get_max_retries(&self) -> Option<u32>;
    fn get_sleep_time(&self, retry_count: u32, elapsed_time: Duration) -> Duration;
}

impl<T> RetryPolicy for T
where T: SleepingRetry 
{
    fn allow_retry(&self, retry_count: u32, elapsed_time: Duration) -> RetryResult {
        let max_retries = self.get_max_retries();
        
        if max_retries == None || retry_count < max_retries.unwrap() {
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
    fn get_max_retries(&self) -> Option<u32> {
        Some(self.max_retries)
    }

    fn get_sleep_time(&self, _retry_count: u32, _elapsed_time: Duration) -> Duration {
        self.retry_interval
    }
}

#[derive(Copy, Clone, Debug)]
pub struct NoRetry;

impl RetryPolicy for NoRetry {
    fn allow_retry(&self, _retry_count: u32, _elapsed_time: Duration) -> RetryResult {
        RetryResult::Stop
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
    fn get_max_retries(&self) -> Option<u32> {
        Some(1)
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
    max_retries: Option<u32>,
    max_sleep: Duration,
}

impl RetryExponentialBackoff {
    pub fn new(base_sleep_time: Duration, max_retries: Option<u32>, max_sleep: Duration) -> RetryExponentialBackoff {
        RetryExponentialBackoff {
            base_sleep_time: base_sleep_time,
            max_retries: max_retries,
            max_sleep: max_sleep,
        }
    }
}

impl SleepingRetry for RetryExponentialBackoff {
    fn get_max_retries(&self) -> Option<u32> {
        self.max_retries
    }

    fn get_sleep_time(&self, retry_count: u32, _elapsed_time: Duration) -> Duration {
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
    max_retry_time: Duration,
    exponential_backoff: RetryExponentialBackoff,
}

impl RetryBoundedExponentialBackoff {
    pub fn new(base_sleep_time: Duration, max_retry_time: Duration, max_retries: Option<u32>, max_sleep: Duration) -> RetryBoundedExponentialBackoff {
        RetryBoundedExponentialBackoff {
            max_retry_time: max_retry_time,
            exponential_backoff: RetryExponentialBackoff::new(
                base_sleep_time,
                max_retries,
                max_sleep,
            )
        }
    }

    fn get_max_retries(&self) -> Option<u32> {
        self.exponential_backoff.get_max_retries()
    }

    fn get_sleep_time(&self, retry_count: u32, elapsed_time: Duration) -> Duration {
        self.exponential_backoff.get_sleep_time(retry_count, elapsed_time)
    }
}

impl RetryPolicy for RetryBoundedExponentialBackoff {
    fn allow_retry(&self, retry_count: u32, elapsed_time: Duration) -> RetryResult {
        let max_retries = self.get_max_retries();
        
        if (max_retries == None || retry_count < max_retries.unwrap()) && elapsed_time.lt(&self.max_retry_time) {
            RetryResult::RetryAfterSleep(self.get_sleep_time(retry_count, elapsed_time))
        } else {
            RetryResult::Stop
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct RetryLoop {
    is_done: bool,
    retry_count: u32,
    start_time: time::Timespec,
}

impl RetryLoop {
    pub fn new() -> RetryLoop {
        RetryLoop {
            is_done: false,
            retry_count: 0,
            start_time: time::now_utc().to_timespec(),
        }
    }

    pub fn elapsed_time(&self) -> Duration {
        let now = time::now_utc().to_timespec();
            
        timespec_sub(&now, &self.start_time)
    }

    pub fn should_continue(&self) -> bool {
        !self.is_done
    }

    pub fn complete(&mut self) {
        self.is_done = true;
    }

    /*
    the following zk errors are eligible for retry:
    connection loss
    operation timeout
    session moved
    session expired
    */
    pub fn is_retry_err(err: &ZkError) -> bool {
        match *err {
            ZkError::ApiError(ZkApiError::ConnectionLoss) => true,
            ZkError::ApiError(ZkApiError::OperationTimeout) => true,
            ZkError::ApiError(ZkApiError::SessionExpired) => true,
            ZkError::Interrupted => true,
            ZkError::Timeout => true,
            /* session moved error too? */
            _ => false,
        }
    }

    pub fn sleep(&self, sleep_time: Duration) {
        let millis = duration_to_ms(&sleep_time);
        
        let timer = timer::oneshot_ms(millis as u32);

        timer.recv().unwrap();
    }

    pub fn failure<P, R>(&mut self, retry_policy: &P, err: ZkError) -> ZkResult<()>
        where P: RetryPolicy
    {
        debug!("retry loop handling error {:?}", err);
        
        if Self::is_retry_err(&err) {
            let elapsed_time = self.elapsed_time();
            self.retry_count += 1;

            match retry_policy.allow_retry(self.retry_count, elapsed_time) {
                RetryResult::RetryAfterSleep(sleep_time) => {
                    self.sleep(sleep_time);
                    Ok(())
                },
                RetryResult::Stop => {
                    Err(err)
                }
            }    
        } else {
            Err(err)
        }
    }
    
    /*
    non retryable exceptions are propagated.
    retry is only attempted if the retry policy allows
    so is subject to time limit and/or retries limit
    if the policy does not allow, then also propagate error immediately

    if error was not propagated then go around for another try
    */
    pub fn call_with_retry<P, F, R>(retry_policy: &P, mut fun: F) -> ZkResult<Option<R>>
        where P: RetryPolicy,
              F: FnMut() -> ZkResult<R>
    {
        let mut retry_loop = RetryLoop::new();
        let mut result = None;
        
        while retry_loop.should_continue() {
            //todo block until connected or timeout

            let fun_result = fun();
            
            match fun_result {
                Ok(r) => {
                    retry_loop.complete();
                    result = Some(r);
                },
                Err(err) => {
                    try!(retry_loop.failure::<P,R>(retry_policy, err));
                }
            }
        }

        Ok(result)
    }
}
