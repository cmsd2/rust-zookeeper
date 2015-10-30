use time;
use std::time::Duration;

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

pub fn duration_to_ms(d: &Duration) -> u64 {
    d.as_secs() * 1000 + (d.subsec_nanos() / 1000000) as u64
}
