use std::{
    ffi::CStr,
    fmt::Display,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
};

use io_uring::{opcode, types};

use crate::uninit::OptZeroed;

use super::{entry::CheckedEntry, OpCode, PipeOut};

#[derive(Debug)]
pub struct File {
    fd: OwnedFd,
    pub size: u32,
}

impl File {
    pub fn new(fd: OwnedFd, size: u32) -> Self {
        File { fd, size }
    }

    pub fn splice_to(&self, pipe_out: &PipeOut) -> CheckedEntry {
        let file_fd = types::Fd(self.fd.as_raw_fd());
        let pipe_fd = pipe_out.fd();
        let entry = opcode::Splice::new(file_fd, 0, pipe_fd, -1, self.size).build();
        CheckedEntry::new(entry, OpCode::Splice)
    }
}

pub fn statx(path: &CStr, statx_buf: &mut OptZeroed<libc::statx>) -> CheckedEntry {
    let entry = opcode::Statx::new(
        types::Fd(libc::AT_FDCWD),
        path.as_ptr(),
        statx_buf.get_mut() as *mut _,
    )
    .flags(libc::O_RDONLY)
    .mask(libc::STATX_SIZE | libc::STATX_TYPE)
    .build();
    CheckedEntry::new(entry, OpCode::Statx)
}

pub fn open_at(path: &CStr) -> CheckedEntry {
    let entry = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path.as_ptr())
        .flags(libc::O_RDONLY)
        .build();
    CheckedEntry::new(entry, OpCode::OpenAt)
}

impl FromRawFd for File {
    unsafe fn from_raw_fd(fd: std::os::unix::prelude::RawFd) -> Self {
        File {
            fd: OwnedFd::from_raw_fd(fd),
            size: u32::MAX,
        }
    }
}

impl Display for File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fd.as_raw_fd().fmt(f)
    }
}

impl Drop for File {
    fn drop(&mut self) {
        tracing::debug!(file = %self, "Dropping file");
    }
}
