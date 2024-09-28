use std::{
    cell::UnsafeCell,
    ffi::c_void,
    future::Future,
    os::fd::RawFd,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

use futures::future::BoxFuture;
use futures_lite::future;
use io_uring::{opcode, squeue, types, IoUring};
use slab::Slab;

use crate::{buf::FixedSizeBufRing, fd::Fd};

struct Executor {
    io_uring: IoUring,
    buf_ring: FixedSizeBufRing,
}

struct Task<'a> {
    future: UnsafeCell<Option<BoxFuture<'a, ()>>>,
}

struct OpenFileFuture<'a> {
    ring: &'a mut IoUring,
    done: Option<Result<RawFd, nix::Error>>,
}

impl<'a> OpenFileFuture<'a> {
    fn new(ring: &'a mut IoUring) -> Self {
        Self { ring, done: None }
    }
}

impl<'a> Future for OpenFileFuture<'a> {
    type Output = Result<RawFd, nix::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let self_mut = self.get_mut();

        if let Some(result) = self_mut.done {
            return Poll::Ready(result);
        }

        let flags = libc::O_RDONLY;
        let open_e = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), "testfile.txt".as_ptr())
            .flags(flags)
            .build()
            .user_data(0x01);

        // Attempt to push the open operation to the submission queue
        unsafe {
            self_mut
                .ring
                .submission()
                .push(&open_e)
                .expect("Failed to submit open operation");
        }

        // Register the waker
        let waker = cx.waker().clone();
        waker.wake_by_ref();

        Poll::Pending
    }
}

impl Executor {
    fn run<F>(&mut self, fut: F)
    where
        F: Future<Output = ()>,
    {
        let raw_waker = create_raw_waker();
        let waker = unsafe { Waker::from_raw(raw_waker) };
        let mut cx = Context::from_waker(&waker);

        loop {
            self.io_uring.submit_and_wait(1).expect("Submission failed");
            for cqe in &mut self.io_uring.completion() {
                let user_data = cqe.user_data() as *mut c_void;
                if !user_data.is_null() {
                    unsafe {
                        let callback: fn() = std::mem::transmute(user_data);
                        callback();
                    }
                }
            }
        }
    }
}

fn create_raw_waker() -> RawWaker {
    fn clone(data: *const ()) -> RawWaker {
        create_raw_waker()
    }
    fn wake(data: *const ()) {}
    fn wake_by_ref(data: *const ()) {}
    fn drop_wake(data: *const ()) {}

    const VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

    RawWaker::new(std::ptr::null(), &VTABLE)
}

thread_local! {
    static RING: io_uring::IoUring = io_uring::IoUring::new(8).unwrap();
}

async fn read_completions() {
    RING.with(|ring| {
        ring.submit_and_wait(1).unwrap();

        let mut cq = unsafe { ring.completion_shared() };
        let cqe = cq.next().unwrap();
        let data = cqe.user_data() as *mut (std::task::Waker, *mut Option<i32>);
        let data = unsafe { Box::from_raw(data) };
        let (waker, result) = *data;
        unsafe { *result = Some(cqe.result()) }
        unsafe { Rc::from_raw(result) };
        waker.wake();
    });
}

pub struct UringFuture {
    entry: Option<squeue::Entry>,
    result: Rc<Option<i32>>,
}

impl UringFuture {
    pub fn new(entry: squeue::Entry) -> Self {
        Self {
            entry: Some(entry),
            result: Rc::new(None),
        }
    }
}

pub fn block_on<T: 'static>(future: impl Future<Output = T> + 'static) -> T {
    future::block_on(async { future.await })
}
