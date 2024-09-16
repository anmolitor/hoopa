use std::{
    fs::File, os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd}, rc::Rc
};

use io_uring::types;

#[derive(Debug, Clone)]
pub struct Fd(Rc<OwnedFd>);

impl FromRawFd for Fd {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Fd(Rc::from(OwnedFd::from_raw_fd(fd)))
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
        Fd(Rc::from(value))
    }
}

impl From<File> for Fd {
    fn from(value: File) -> Self {
        Fd(Rc::from(OwnedFd::from(value)))
    }
}
