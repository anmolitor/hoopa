use std::{
    fmt::{Debug, Display},
    fs::File,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
};

use io_uring::types;

fn loopy() {
    let mut ring = io_uring::IoUring::new(2).unwrap();
    let mut sq = ring.submission();
    loop {
        sq.sync();
        test(&mut sq);
    }
}

fn test(sq: &mut io_uring::SubmissionQueue) {}

#[derive(Debug)]
pub struct Fd(OwnedFd);

impl FromRawFd for Fd {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Fd(OwnedFd::from_raw_fd(fd))
    }
}

impl AsRawFd for Fd {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

impl From<&Fd> for types::Fd {
    fn from(value: &Fd) -> Self {
        types::Fd(value.0.as_raw_fd())
    }
}

impl From<Fd> for types::Fd {
    fn from(value: Fd) -> Self {
        types::Fd(value.0.as_raw_fd())
    }
}

impl From<OwnedFd> for Fd {
    fn from(value: OwnedFd) -> Self {
        Fd(value)
    }
}

impl From<File> for Fd {
    fn from(value: File) -> Self {
        Fd(OwnedFd::from(value))
    }
}

impl Display for Fd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
