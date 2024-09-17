use std::{
    net::TcpListener,
    os::fd::{AsRawFd, FromRawFd, RawFd},
};

use crate::{
    buf::{FixedBuffers, StorageId},
    fd::Fd,
    http2::{Frame, FrameType, HeadersFlags, Setting, SettingPairs, Settings},
    slab::Slab,
    uring_id::{ConnectionId, UringId},
};
use enumflags2::BitFlags;
use io_uring::{opcode, squeue::Flags, types, IoUring};
use tracing::field;

const MAX_CONCURRENT_REQUESTS_PER_THREAD: u16 = 512;
const IO_URING_SIZE: u32 = 256;

#[derive(Debug)]
enum RequestState {
    Read {
        connection_fd: Fd,
        storage_id: StorageId,
    },
    Write {
        connection_fd: Fd,
    },
    ReadHttp2Settings {
        connection_fd: Fd,
        storage_id: StorageId,
        request_storage_id: StorageId,
    },
    ReadHttp2 {
        connection_fd: Fd,
        storage_id: StorageId,
        request_storage_id: StorageId,
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
                    .build()
                    .user_data(*receiver_id as _);
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
    let mut state: Slab<ConnectionId, RequestState> = Slab::new(MAX_CONCURRENT_REQUESTS_PER_THREAD);

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

            if user_data == u64::MAX {
                on_accept_connection(&completion, &mut state, &mut buffers, &mut sq)?;
                continue;
            }

            let uring_id = UringId::from_cqe(&completion);
            let (connection_id, stream_id, op_code) = uring_id.split();
            let completion_span = tracing::debug_span!("completion", s_id = field::Empty, c_id = %connection_id, result = completion.result(), op = %op_code);
            if let Some(stream_id) = stream_id {
                completion_span.record("s_id", stream_id.to_string());
            }
            let _enter = completion_span.enter();

            let Some((request_state, request_state_entry)) = state.entry(connection_id) else {
                continue;
            };

            match request_state {
                RequestState::Read {
                    connection_fd,
                    storage_id: request_storage_id,
                } => {
                    tracing::debug!("Read complete");
                    let buf = &buffers[request_storage_id];
                    let mut headers = [httparse::EMPTY_HEADER; 16];
                    let mut req = httparse::Request::new(&mut headers);
                    let res = req.parse(buf)?;
                    if res.is_partial() {
                        anyhow::bail!("Request headers too large (> 4096 bytes)");
                    }
                    println!("{req:?}");
                    tracing::debug!(message = "Successfully parsed request", path = req.path);

                    if req.headers.contains(&httparse::Header {
                        name: "Upgrade",
                        value: b"h2c",
                    }) {
                        tracing::debug!("Upgrading to HTTP/2");

                        let (read_entry, storage_id) = buffers.read(&connection_fd, uring_id)?;
                        let write_entry = opcode::Send::new(
                            types::Fd::from(&connection_fd),
                            RESPONSE_101_UPGRADE_HTTP2.as_ptr(),
                            RESPONSE_101_UPGRADE_HTTP2.len() as _,
                        )
                        .build()
                        .flags(Flags::IO_LINK);
                        drop(request_state_entry);
                        let slab_id = state.insert(RequestState::ReadHttp2Settings {
                            connection_fd,
                            request_storage_id,
                            storage_id,
                        })?;

                        unsafe { sq.push(&write_entry.user_data(0))? };
                        unsafe { sq.push(&read_entry)? };
                        continue;
                    }

                    let write_entry = opcode::Write::new(
                        types::Fd::from(&connection_fd),
                        RESPONSE_200_HTTP11.as_ptr(),
                        RESPONSE_200_HTTP11.len() as _,
                    )
                    .build()
                    .user_data(user_data);

                    request_state_entry.set(RequestState::Write { connection_fd });
                    buffers.give_back(request_storage_id);
                    unsafe { sq.push(&write_entry)? };
                    tracing::debug!("Writing to socket");
                }
                RequestState::Write { connection_fd } => {
                    tracing::debug!("Write complete");
                    tracing::debug!("Closing connection");
                    request_state_entry.set(RequestState::Write { connection_fd });
                }
                RequestState::ReadHttp2Settings {
                    connection_fd,
                    storage_id,
                    request_storage_id,
                } => {
                    tracing::debug!("HTTP/2 settings");
                    let buf = &mut buffers[storage_id];
                    let Some(buf_without_prefix) = buf.strip_prefix(PREFACE_HTTP2) else {
                        anyhow::bail!("Client did not send HTTP/2 preface.");
                    };
                    let (rest, frame) =
                        Frame::parse(buf_without_prefix).expect("Frame parsing failed");

                    let mut client_settings = Settings::default();
                    Settings::parse(&rest[0..frame.len as usize], |code, val| {
                        client_settings.apply(code, val)
                    })
                    .expect("Frame parsing failed");

                    let setting_pairs = [
                        (Setting::EnablePush, 0),
                        (Setting::HeaderTableSize, client_settings.header_table_size),
                        (
                            Setting::InitialWindowSize,
                            client_settings.initial_window_size,
                        ),
                        (
                            Setting::MaxConcurrentStreams,
                            client_settings.max_concurrent_streams.unwrap_or(u32::MAX),
                        ),
                        (Setting::MaxFrameSize, client_settings.max_frame_size),
                        (
                            Setting::MaxHeaderListSize,
                            client_settings.max_header_list_size,
                        ),
                    ];

                    let server_settings = SettingPairs(&setting_pairs);
                    let server_frame =
                        Frame::new(FrameType::Settings(Default::default()), frame.stream_id);
                    let settings_len = server_settings.calc_size();
                    let frame_len = server_frame.with_len(settings_len as _).write_into(buf)?;

                    server_settings.write(&mut buf[frame_len.into()..])?;

                    let write_entry = buffers
                        .write(
                            &connection_fd,
                            storage_id,
                            frame_len as u32 + settings_len as u32,
                        )
                        .flags(Flags::IO_LINK)
                        .user_data(0);
                    // TODO come up with a system to release storage_ids
                    let (read_entry, storage_id) = buffers.read(&connection_fd, uring_id)?;

                    let mut header_flags = BitFlags::default();
                    header_flags.insert(HeadersFlags::EndHeaders);
                    let headers_frame =
                        FrameType::Headers(header_flags).into_frame(frame.stream_id);
                    //hpack::Encoder::new().encode_into([], &mut buf[]);

                    unsafe { sq.push(&write_entry)? };
                    unsafe { sq.push(&read_entry)? };
                    request_state_entry.set(RequestState::ReadHttp2 {
                        connection_fd,
                        storage_id,
                        request_storage_id,
                    });
                }
                RequestState::ReadHttp2 {
                    connection_fd,
                    storage_id,
                    request_storage_id,
                } => {
                    let buf = &mut buffers[storage_id];
                    let (rest, frame) = Frame::parse(buf).expect("Frame parsing failed");
                    println!("Frame {frame:?}");
                }
            }
        }
    }
}

fn on_accept_connection(
    completion: &io_uring::cqueue::Entry,
    state: &mut Slab<ConnectionId, RequestState>,
    buffers: &mut FixedBuffers<4096>,
    sq: &mut io_uring::SubmissionQueue<'_>,
) -> Result<(), anyhow::Error> {
    let connection_fd = unsafe { Fd::from_raw_fd(completion.result()) };
    let (connection_id, request_state_entry) = state.reserve()?;
    let uring_id = UringId::from_connection_id(connection_id);
    let (read_entry, storage_id) = buffers.read(&connection_fd, uring_id)?;
    request_state_entry.set(RequestState::Read {
        connection_fd,
        storage_id,
    });
    unsafe { sq.push(&read_entry)? };
    tracing::debug!(message = "Accepted connection", socket = completion.result(), c_id = %connection_id);
    Ok(())
}

const RESPONSE_200_HTTP11: &str = "HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\nHello World";
const RESPONSE_101_UPGRADE_HTTP2: &str =
    "HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c\r\nConnection: Upgrade\r\n\r\n";
pub const PREFACE_HTTP2: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

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
