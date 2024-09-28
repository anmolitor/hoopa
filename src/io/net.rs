use std::{
    fmt::Display,
    net::SocketAddr,
    os::fd::{AsRawFd as _, FromRawFd, OwnedFd, RawFd},
};

use io_uring::{opcode, types};
use socket2::{Domain, Protocol, Type};

use crate::buf::FixedSizeBufRing;

use super::{entry::CheckedEntry, OpCode, PipeIn};

#[derive(Debug)]
pub struct TcpStream(OwnedFd);

#[derive(Debug)]
pub struct Socket(socket2::Socket);

impl TcpStream {
    pub fn leak(self) -> RawFd {
        let raw_fd = self.0.as_raw_fd();
        std::mem::forget(self);
        raw_fd
    }

    pub fn write(&self, str: &[u8]) -> CheckedEntry {
        let fd = types::Fd(self.0.as_raw_fd());
        let entry = opcode::Send::new(fd, str.as_ptr(), str.len() as _).build();
        CheckedEntry::new(entry, OpCode::Send)
    }

    pub fn recv_multi(&self, buf_ring: &FixedSizeBufRing) -> CheckedEntry {
        let fd = types::Fd(self.0.as_raw_fd());
        let entry = opcode::RecvMulti::new(fd, buf_ring.bgid()).build();
        CheckedEntry::new(entry, OpCode::RecvMulti)
    }

    pub fn splice_from(&self, pipe_in: &PipeIn, size: u32) -> CheckedEntry {
        let fd = types::Fd(self.0.as_raw_fd());
        let pipe_fd = pipe_in.fd();
        let entry = opcode::Splice::new(pipe_fd, -1, fd, -1, size).build();
        CheckedEntry::new(entry, OpCode::Splice)
    }
}

impl Socket {
    pub fn new(socket_addr: SocketAddr) -> std::io::Result<Self> {
        let socket = socket2::Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;

        // Set the SO_REUSEPORT option
        socket.set_reuse_port(true)?;

        // Bind the socket to the address
        socket.bind(&socket_addr.into())?;

        // Start listening with a backlog of 128
        socket.listen(128)?;

        Ok(Self(socket))
    }

    pub fn accept_multi(&self) -> CheckedEntry {
        let fd = types::Fd(self.0.as_raw_fd());
        let entry = opcode::AcceptMulti::new(fd).build();
        CheckedEntry::new(entry, OpCode::AcceptMulti)
    }
}

impl Display for TcpStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_raw_fd().fmt(f)
    }
}

impl FromRawFd for TcpStream {
    unsafe fn from_raw_fd(fd: std::os::unix::prelude::RawFd) -> Self {
        TcpStream(OwnedFd::from_raw_fd(fd))
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        tracing::debug!(stream = %self, "Dropping tcp stream");
    }
}
