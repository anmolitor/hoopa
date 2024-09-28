use std::{
    fmt::Display,
    os::fd::{AsRawFd, OwnedFd},
};

use io_uring::types;

#[derive(Debug)]
pub struct PipeIn(OwnedFd);

#[derive(Debug)]
pub struct PipeOut(OwnedFd);

pub fn pipe() -> Result<(PipeOut, PipeIn), nix::Error> {
    let (read, write) = nix::unistd::pipe()?;
    Ok((PipeOut(read), PipeIn(write)))
}

impl PipeIn {
    pub(super) fn fd(&self) -> types::Fd {
        types::Fd(self.0.as_raw_fd())
    }
}

impl PipeOut {
    pub(super) fn fd(&self) -> types::Fd {
        types::Fd(self.0.as_raw_fd())
    }
}

impl Display for PipeIn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_raw_fd().fmt(f)
    }
}

impl Display for PipeOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_raw_fd().fmt(f)
    }
}

impl Drop for PipeIn {
    fn drop(&mut self) {
        tracing::debug!(pipe = %self, "Dropping input pipe");
    }
}

impl Drop for PipeOut {
    fn drop(&mut self) {
        tracing::debug!(pipe = %self, "Dropping output pipe");
    }
}
