use std::cell::UnsafeCell;

use futures::future::BoxFuture;
use io_uring::IoUring;

use crate::buf::FixedSizeBufRing;

struct Executor {
    io_uring: IoUring,
    buf_ring: FixedSizeBufRing,
}

struct Task<'a> {
    future: UnsafeCell<Option<BoxFuture<'a, ()>>>,
}
