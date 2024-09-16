use std::{
    net::TcpListener,
    os::fd::{AsRawFd, FromRawFd, RawFd},
    u64,
};

use crate::{
    buf::{FixedBuffers, StorageId},
    fd::Fd,
    slab::{Slab, SlabId},
    uring_id::UringId,
};
use io_uring::{opcode, types, IoUring};
use kanal::{Receiver, Sender};

const MAX_CONCURRENT_REQUESTS_PER_THREAD: u16 = 512;
const IO_URING_SIZE: u32 = 64;

#[derive(Debug)]
enum RequestState {
    Read {
        connection_fd: Fd,
        storage_id: StorageId,
    },
    Write {
        connection_fd: Fd,
    },
}

pub fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let listener = TcpListener::bind(("127.0.0.1", 3456))?;
    listener.set_nonblocking(true)?;
    let socket_fd = listener.as_raw_fd();

    let cpus = num_cpus::get_physical();

    let mut handles = Vec::with_capacity(cpus - 1);
    let mut rings = Vec::with_capacity(cpus - 1);
    for cpu in 1..cpus {
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

fn accept_worker(socket_fd: i32, chans: &[(usize, RawFd)], cpu: usize) -> anyhow::Result<()> {
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
            let completion_span =
                tracing::debug_span!("completion", user_data, result = completion.result());
            let _enter = completion_span.enter();
            if user_data > 0 {
                tracing::debug!("Connection sent");
                continue;
            }
                
            let connection_fd: RawFd = completion.result();
            let (receiver_id, ring_fd) = chan_iter.next().expect("Empty slice of senders");
            let message_entry =
                opcode::MsgRingData::new(types::Fd(*ring_fd), connection_fd, u64::MAX, None)
                    .build().user_data(*receiver_id as _);
            unsafe { sq.push(&message_entry)? };
            tracing::debug!(message = "Accepted connection", receiver = receiver_id);
        }
    }
}

fn worker(mut ring: IoUring, cpu: usize) -> anyhow::Result<()> {
    let span = tracing::info_span!("", cpu = %cpu);
    let _enter = span.enter();

    tracing::debug!(message = "Started worker");

    let (submitter, mut sq, mut cq) = ring.split();
    tracing::debug!(message = "Setup io_uring");
    let mut buffers: FixedBuffers<4096> = FixedBuffers::new(MAX_CONCURRENT_REQUESTS_PER_THREAD);
    buffers.register_to(&submitter)?;
    tracing::debug!(message = "Registered shared buffers");
    let mut state: Slab<RequestState> = Slab::new(MAX_CONCURRENT_REQUESTS_PER_THREAD);

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
            let completion_span =
                tracing::debug_span!("completion", user_data, result = completion.result());
            let _enter = completion_span.enter();

            if user_data == u64::MAX {
                let connection_fd = unsafe { Fd::from_raw_fd(completion.result()) };
                let (read_entry, storage_id) = buffers.read(&connection_fd)?;
                let slab_id = state.insert(RequestState::Read {
                    connection_fd,
                    storage_id,
                })?;
                let user_id: u32 = slab_id.into();
                let id = UringId::from(user_id);
                unsafe { sq.push(&read_entry.user_data(id.user_data()))? };
                tracing::debug!("Reading from socket");

                continue;
            }

            let id = UringId(user_data);

            let user_id = id.user_id();
            let slab_id = SlabId::from(user_id as u16);
            let Some((request_state, request_state_entry)) = state.entry(slab_id) else {
                continue;
            };

            match request_state {
                RequestState::Read {
                    connection_fd,
                    storage_id,
                } => {
                    tracing::debug!("Read complete");
                    let write_entry = opcode::Write::new(
                        types::Fd::from(&connection_fd),
                        RESPONSE_200_HTTP11.as_ptr(),
                        RESPONSE_200_HTTP11.len() as _,
                    )
                    .build()
                    .user_data(user_data);
                    request_state_entry.set(RequestState::Write { connection_fd });
                    buffers.give_back(storage_id);
                    unsafe { sq.push(&write_entry)? };
                    tracing::debug!("Writing to socket");
                }
                RequestState::Write { connection_fd } => {
                    tracing::debug!("Write complete");
                    let close_entry = opcode::Close::new(types::Fd::from(connection_fd)).build();
                    request_state_entry.remove();
                    unsafe { sq.push(&close_entry)? };
                    tracing::debug!("Closing connection");
                }
            }
        }
    }
}

const RESPONSE_200_HTTP11: &str = "HTTP/1.1 200 OK\r\n\r\nHello World";

#[cfg(test)]
mod tests {
    use super::*;
    use std::{net::TcpListener, os::fd::AsRawFd};

    #[test]
    fn serve() -> anyhow::Result<()> {
        let listener = TcpListener::bind(("127.0.0.1", 3456))?;
        let socket_fd = listener.as_raw_fd();
        let mut ring: IoUring = IoUring::builder()
            //.setup_coop_taskrun()
            //.setup_sqpoll(1024 * 1024)
            //.setup_single_issuer()
            .build(64)?;
        println!("setup worker ring");
        // let (mut submitter, mut sq, mut cq) = ring.split();

        let accept_multi_entry = opcode::AcceptMulti::new(types::Fd(socket_fd)).build();
        unsafe { ring.submission().push(&accept_multi_entry)? };
        ring.submit_and_wait(1)?;
        println!("{:?}", ring.completion().next());
        Ok(())
    }
}
