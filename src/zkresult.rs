use std::result;

use consts::*;

#[derive(Clone, Debug)]
pub enum ZkError {
    ApiError(ZkApiError),
    Timeout,
    Interrupted,
    UnknownError,
    ChannelError,
}

pub type ZkResult<T> = result::Result<T, ZkError>;
