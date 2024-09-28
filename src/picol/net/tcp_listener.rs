use std::{
    io,
    net::ToSocketAddrs,
    os::fd::{AsRawFd, FromRawFd},
};

use futures::Stream;

use crate::picol::io::{UringFuture, UringStream};

use super::tcp_stream::TcpStream;

pub struct TcpListener {
    socket: socket2::Socket,
}

impl TcpListener {
    pub fn bind<T: ToSocketAddrs>(addr: T) -> io::Result<TcpListener> {
        let addr = addr.to_socket_addrs()?.next().unwrap();
        let socket = socket2::Socket::new(
            socket2::Domain::for_address(addr),
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )?;
        socket.set_reuse_address(true)?;
        socket.bind(&addr.into())?;
        socket.listen(1024)?;
        Ok(TcpListener { socket })
    }

    pub async fn accept(&self) -> io::Result<TcpStream> {
        let fd = io_uring::types::Fd(self.socket.as_raw_fd());
        let entry = io_uring::opcode::Accept::new(fd, std::ptr::null_mut(), std::ptr::null_mut());
        let entry = entry.build();
        let result = UringFuture::new(entry).await?;

        let socket = unsafe { socket2::Socket::from_raw_fd(result) };

        Ok(TcpStream::new(socket))
    }

    pub fn accept_multi(&self) -> impl Stream<Item = io::Result<TcpStream>> {
        let fd = io_uring::types::Fd(self.socket.as_raw_fd());
        let entry = io_uring::opcode::AcceptMulti::new(fd);
        let entry = entry.build();
        let stream = UringStream::new(entry);

        use futures::StreamExt as _;
        stream.map(|result| {
            let socket = unsafe { socket2::Socket::from_raw_fd(result?) };
            Ok(TcpStream::new(socket))
        })
    }
}
