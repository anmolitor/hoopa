use std::{
    net::TcpListener,
    os::fd::{AsRawFd, FromRawFd, RawFd},
    thread,
    time::Duration,
};

use crate::{
    buf::{self, FixedSizeBufRing},
    completion_result::{Completion, RecvMultiResult},
    fd::Fd,
    slab::Slab,
    uring_id::{ConnectionId, Op as _, OpCode, UringId},
};
use io_uring::{opcode, types, IoUring};
use nix::sys::epoll::{EpollCreateFlags, EpollEvent, EpollFlags, EpollTimeout};
use tracing::field;

const MAX_CONCURRENT_REQUESTS_PER_THREAD: u16 = 512;
const IO_URING_SIZE: u32 = 256;

#[derive(Debug)]
struct RequestState {
    connection_fd: Fd,
}

pub fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    //let listener = TcpListener::bind(("127.0.0.1", 3456))?;
    let listener = TcpListener::bind(("0.0.0.0", 3456))?;
    listener.set_nonblocking(true)?;
    let socket_fd = listener.as_raw_fd();

    let cpus = num_cpus::get_physical();

    let mut handles = Vec::with_capacity(cpus - 1);
    let mut rings = Vec::with_capacity(cpus - 1);
    for cpu in 1..cpus.try_into().expect("> 65535 CPUs. Impressive.") {
        let worker_ring = setup_worker_io_uring()?;
        rings.push((cpu, worker_ring.as_raw_fd()));
        let join_handle = std::thread::spawn(move || {
            worker(worker_ring, cpu).expect("Worker failed");
        });
        handles.push(join_handle);
    }

    tracing::info!(message = "Accepting requests.", threads = cpus, addr = %listener.local_addr()?);

    let result = accept_worker(socket_fd, &rings, 0);
    for handle in handles {
        handle.join().expect("Join");
    }
    result?;
    Ok(())
}

fn setup_worker_io_uring() -> std::io::Result<IoUring> {
    IoUring::builder()
        .setup_coop_taskrun()
        //.setup_sqpoll(1024 * 1024)
        .build(256)
}

fn accept_worker(socket_fd: i32, chans: &[(u16, RawFd)], cpu: usize) -> anyhow::Result<()> {
    let span = tracing::info_span!("", cpu = %cpu);
    let _enter = span.enter();

    tracing::debug!(message = "Started main worker");

    let mut ring: IoUring = IoUring::builder()
        .setup_coop_taskrun()
        //.setup_sqpoll(1024 * 1024)
        .setup_single_issuer()
        .build(256)?;
    let (submitter, mut sq, mut cq) = ring.split();
    tracing::debug!(message = "Setup io_uring");

    let accept_multi_entry = opcode::AcceptMulti::new(types::Fd(socket_fd)).build();
    unsafe {
        sq.push(&accept_multi_entry)?;
    }

    let mut chan_iter = chans.iter().cycle();

    loop {
        sq.sync();
        let submit_result = submitter.submit_and_wait(1);
        match submit_result {
            Ok(_) => (),
            Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => (),
            Err(err) => return Err(err.into()),
        }

        cq.sync();
        for completion in &mut cq {
            let user_data = completion.user_data();
            let result = completion.result();
            let completion_span = tracing::debug_span!("completion", user_data, result);
            let _enter = completion_span.enter();
            if user_data > 0 && result < 0 {
                tracing::error!("Failed to send connection");
                continue;
            }
            if user_data > 0 && result > 0 {
                tracing::debug!("Connection sent");
                continue;
            }
            if result == 0 {
                tracing::trace!("No connections to accept");
                continue;
            }
            if result < 0 {
                tracing::error!(message = "Accept returned unexpected error");
                continue;
            }

            let connection_fd: RawFd = completion.result();
            let (receiver_id, ring_fd) = chan_iter.next().expect("Empty slice of senders");
            let message_entry =
                opcode::MsgRingData::new(types::Fd(*ring_fd), connection_fd, u64::MAX, None)
                    .build()
                    .user_data(*receiver_id as _);
            unsafe { sq.push(&message_entry)? };
            tracing::debug!(message = "Accepted connection", receiver = receiver_id);
        }
    }
}

fn worker(mut ring: IoUring, cpu: u16) -> anyhow::Result<()> {
    let span = tracing::info_span!("", cpu = %cpu);
    let _enter = span.enter();

    tracing::debug!(message = "Started worker");

    let (submitter, mut sq, mut cq) = ring.split();
    tracing::debug!(message = "Setup io_uring");
    let mut buf_ring = buf::Builder::new(cpu).build()?;
    buf_ring.register(&submitter)?;
    tracing::debug!(message = "Registered shared buffers");
    let mut state: Slab<ConnectionId, RequestState> = Slab::new(MAX_CONCURRENT_REQUESTS_PER_THREAD);
    loop {
        sq.sync();
        submitter.submit_and_wait(1)?;

        cq.sync();
        tracing::debug!(message = "Working on completions", amount = cq.len());
        for completion in &mut cq {
            let result = completion.result();
            if completion.user_data() == u64::MAX {
                on_accept_connection(&completion, &mut state, &mut buf_ring, &mut sq)?;
                continue;
            }

            let uring_id = UringId::from_cqe(&completion);
            let (connection_id, stream_id, op_code) = uring_id.split();
            let completion_span = tracing::error_span!("completion", s_id = field::Empty, c_id = %connection_id, result, op = %op_code);
            if let Some(stream_id) = stream_id {
                completion_span.record("s_id", stream_id.to_string());
            }
            let _enter = completion_span.enter();

            let Some((request_state, request_state_entry)) = state.entry(connection_id) else {
                continue;
            };

            match Completion::from_entry(&buf_ring, &completion) {
                Ok(Completion::AcceptMulti { connection_fd }) => {
                    panic!(
                        "Workers should not accept connections, only the main thread.
                However, the connection {connection_fd:?} was accepted on thread {cpu}.
                    "
                    );
                }
                Err((OpCode::AcceptMulti, error)) => {
                    panic!(
                        "Workers should not accept connections, only the main thread.
However, thread {cpu} attempted to, but got an error {error}.
                    "
                    );
                }
                Ok(Completion::AsyncCancel) => {
                    tracing::debug!("Cancellation successful");
                }
                Err((OpCode::AsyncCancel, error)) => {
                    tracing::error!(message = "Unexpected error code", %error);
                }
                Err((OpCode::RecvMulti, nix::Error::ENOBUFS)) => {
                    tracing::warn!(message = "Ran out of provided buffers. Attempting retry.");
                    // TODO actually implement retry via Timeout
                    continue;
                }
                Err((OpCode::RecvMulti, nix::Error::ENOENT)) => {
                    tracing::warn!(message = "The client probably closed the connection.");
                    unsafe { sq.push(&uring_id.cancel())? };
                    continue;
                }
                Err((OpCode::RecvMulti, nix::Error::EBADF)) => {
                    tracing::warn!(message = "The client probably closed the connection.");
                    unsafe { sq.push(&uring_id.cancel())? };
                    continue;
                }
                Err((OpCode::RecvMulti, error)) => {
                    tracing::error!(message = "Unexpected error code", %error);
                }
                Ok(Completion::RecvMulti(RecvMultiResult::Cancelled)) => {
                    tracing::debug!("No more data to receive, cancelling RecvMulti");
                    unsafe { sq.push(&uring_id.cancel())? };
                }
                Ok(Completion::RecvMulti(RecvMultiResult::UnknownBuffer)) => {
                    tracing::warn!(
                        message =
                            "Received data from socket, but the kernel sent an unknown buffer id",
                        flags = completion.flags(),
                    );
                    continue;
                }
                Ok(Completion::RecvMulti(RecvMultiResult::Data { length, buf })) => {
                    tracing::debug!("Recv complete");
                    let mut headers = [httparse::EMPTY_HEADER; 16];
                    let mut req = httparse::Request::new(&mut headers);
                    let res = req.parse(buf.as_slice())?;
                    if res.is_partial() {
                        anyhow::bail!("Request headers too large (> 4096 bytes)");
                    }
                    tracing::debug!(message = "Successfully parsed request", path = req.path);

                    let write_entry = opcode::Send::new(
                        types::Fd::from(&request_state.connection_fd),
                        RESPONSE_200_HTTP11.as_ptr(),
                        RESPONSE_200_HTTP11.len() as _,
                    )
                    .build_with_user_data(uring_id);

                    request_state_entry.set(RequestState {
                        connection_fd: request_state.connection_fd,
                    });
                    unsafe { sq.push(&write_entry)? };
                    tracing::debug!("Writing to socket");
                }
                Ok(Completion::Send { length }) => {
                    tracing::debug!(message = "Sent bytes over socket", length);
                    request_state_entry.set(RequestState {
                        connection_fd: request_state.connection_fd,
                    });
                }
                Err((OpCode::Send, error)) => {
                    tracing::error!(message = "Unexpected error code", %error);
                }
                Ok(Completion::OpenAt { fd }) => {}
                Err((OpCode::OpenAt, error)) => {
                    tracing::error!(message = "Unexpected error code", %error);
                }
                Ok(Completion::Statx) => {}
                Err((OpCode::Statx, error)) => {
                    tracing::error!(message = "Unexpected error code", %error);
                }
                Ok(Completion::Splice) => {}
                Err((OpCode::Splice, error)) => {
                    tracing::error!(message = "Unexpected error code", %error);
                }
            }
        }
    }
}

fn on_accept_connection(
    completion: &io_uring::cqueue::Entry,
    state: &mut Slab<ConnectionId, RequestState>,
    buf_ring: &mut FixedSizeBufRing,
    sq: &mut io_uring::SubmissionQueue<'_>,
) -> Result<(), anyhow::Error> {
    let connection_fd = unsafe { Fd::from_raw_fd(completion.result()) };
    let (connection_id, request_state_entry) = state.reserve()?;
    let uring_id = UringId::from_connection_id(connection_id);

    let recv_multi_entry = opcode::RecvMulti::new(types::Fd::from(&connection_fd), buf_ring.bgid())
        .build_with_user_data(uring_id);

    request_state_entry.set(RequestState { connection_fd });
    unsafe { sq.push(&recv_multi_entry)? };
    tracing::debug!(message = "Accepted connection", socket = completion.result(), c_id = %connection_id);
    Ok(())
}

const RESPONSE_200_HTTP11: &str =
    "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 48\r\n\r\n<html><body><h1>Hello, World!</h1></body></html>";
const RESPONSE_101_UPGRADE_HTTP2: &str =
    "HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c\r\nConnection: Upgrade\r\n\r\n";
pub const PREFACE_HTTP2: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

#[cfg(test)]
mod tests {
    use crate::buf;

    use super::*;
    use std::{
        io::Write,
        net::{TcpListener, TcpStream},
        os::fd::AsRawFd,
    };

    #[test]
    fn serve() -> anyhow::Result<()> {
        let listener = TcpListener::bind(("127.0.0.1", 3457))?;
        let socket_fd = listener.as_raw_fd();
        let mut ring: IoUring = IoUring::builder()
            //.setup_coop_taskrun()
            //.setup_sqpoll(1024 * 1024)
            //.setup_single_issuer()
            .build(64)?;
        println!("setup worker ring");
        let mut buf_ring = buf::Builder::new(777).build()?;
        buf_ring.register(&ring.submitter())?;

        let accept_multi_entry = opcode::AcceptMulti::new(types::Fd(socket_fd)).build();
        unsafe { ring.submission().push(&accept_multi_entry)? };

        let mut stream = TcpStream::connect(listener.local_addr()?)?;

        ring.submit_and_wait(1)?;
        let cqe = ring.completion().next().unwrap();

        let recv_multi_entry = opcode::RecvMulti::new(types::Fd(cqe.result()), 777).build();
        unsafe {
            ring.submission().push(&recv_multi_entry)?;
        }
        stream.write_all(b"hello world")?;

        ring.submit_and_wait(1)?;
        let cqe = ring.completion().next().unwrap();
        println!("{cqe:?}");
        let buf = buf_ring.get_buf(cqe.result() as u32, cqe.flags());
        println!("buf: {:?}", buf.as_slice());
        drop(buf);

        stream.write_all(b"hello")?;

        ring.submit_and_wait(1)?;
        let cqe = ring.completion().next().unwrap();
        println!("{cqe:?}");
        let buf = buf_ring.get_buf(cqe.result() as u32, cqe.flags());
        println!("buf: {:?}", buf.as_slice());
        Ok(())
    }
}
