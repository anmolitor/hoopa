use std::{
    ffi::{c_char, CStr, CString},
    fmt::Display,
    io::Write,
    net::SocketAddr,
    os::fd::{AsRawFd, RawFd},
    rc::Rc,
    sync::Arc,
    thread::available_parallelism,
    time::Duration,
};

use crate::{
    buf::{self, FixedSizeBufRing, GBuf},
    bump::Bump,
    completion_result::{Completion, RecvMultiData, RecvMultiError},
    fd::Fd,
    io::{self, File, PipeIn, PipeOut, ScopedSubmissionQueue, Socket, TcpStream},
    slab::{self, Slab},
    uninit::OptZeroed,
    uring_id::{AcceptedUringId, ConnectionId, HasOpCode as _, UringId},
};
use gxhash::GxBuildHasher;
use io_uring::{cqueue, opcode, types, IoUring, SubmissionQueue};
use moka::{policy::EvictionPolicy, sync::Cache};
use tracing::field;

const MAX_CONCURRENT_REQUESTS_PER_THREAD: u16 = 512;
const IO_URING_SIZE: u32 = 256;

#[derive(Debug)]
struct RequestState {
    tcp_stream: Option<TcpStream>,
    statx: OptZeroed<libc::statx>,
    pipe_fds: (PipeOut, PipeIn),
    file: Option<File>,
    buf: Bump<1024>,
    path_ref: Option<*const u8>,
}

impl Default for RequestState {
    fn default() -> Self {
        Self {
            tcp_stream: None,
            statx: unsafe { OptZeroed::zeroed() },
            pipe_fds: io::pipe().expect("Failed to create pipe"),
            file: None,
            buf: Bump::new(),
            path_ref: None,
        }
    }
}

impl slab::Reset for RequestState {
    fn reset(&mut self) {
        self.tcp_stream = None;
        self.statx.reset();
        self.buf.reset();
        self.file = None;
    }
}

type FileCache = Cache<CString, Arc<File>, GxBuildHasher>;

type State = Slab<ConnectionId, RequestState>;

pub fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cpus: usize = available_parallelism()?.into();

    let file_cache: FileCache = Cache::builder()
        .eviction_policy(EvictionPolicy::tiny_lfu())
        .time_to_live(Duration::from_secs(60))
        .initial_capacity(100)
        .max_capacity(100)
        .build_with_hasher(GxBuildHasher::default());

    let mut handles = Vec::with_capacity(cpus - 1);
    let mut rings = Vec::with_capacity(cpus - 1);
    for cpu in 1..cpus.try_into().expect("> 65535 CPUs. Impressive.") {
        let worker_ring = setup_worker_io_uring()?;
        rings.push((cpu, worker_ring.as_raw_fd()));
        let file_cache = file_cache.clone();
        let join_handle = std::thread::spawn(move || {
            worker(worker_ring, file_cache, cpu).expect("Worker failed");
        });
        handles.push(join_handle);
    }

    let main_ring = setup_worker_io_uring()?;
    let result = worker(main_ring, file_cache, 0);
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

fn worker(mut ring: IoUring, file_cache: FileCache, cpu: u16) -> anyhow::Result<()> {
    let address = "127.0.0.1:3456";
    let socket_addr: SocketAddr = address.parse().expect("Invalid address");

    let socket = Socket::new(socket_addr)?;
    let span = tracing::info_span!("", cpu = %cpu);
    let _enter = span.enter();

    tracing::debug!(message = "Started worker");

    let (submitter, mut sq, mut cq) = ring.split();
    tracing::debug!(message = "Setup io_uring");
    let buf_ring = buf::Builder::new(cpu).build()?;
    buf_ring.register(&submitter)?;

    tracing::debug!(message = "Registered shared buffers");
    let mut state: Slab<ConnectionId, RequestState> = Slab::new(MAX_CONCURRENT_REQUESTS_PER_THREAD);

    let accept_multi_entry = socket.accept_multi();

    // Connection ID is not needed here
    ScopedSubmissionQueue::new(&mut sq, UringId::from_connection_id(ConnectionId::from(0)))
        .add(accept_multi_entry);

    tracing::info!(message = "Accepting requests", addr = address);
    loop {
        sq.sync();
        submitter.submit_and_wait(1)?;

        cq.sync();
        tracing::debug!(message = "Working on completions", amount = cq.len());
        for completion in &mut cq {
            handle_completion(completion, &mut state, &buf_ring, &mut sq, &file_cache);
        }
    }
}

fn handle_completion<'ring, 'ssq>(
    entry: cqueue::Entry,
    state: &mut State,
    buf_ring: &FixedSizeBufRing,
    sq: &'ssq mut SubmissionQueue<'ring>,
    file_cache: &'ring FileCache,
) where
    'ring: 'ssq,
{
    let result = entry.result();

    let uring_id = UringId::from_cqe(&entry);

    match uring_id {
        UringId::Accepted(accepted_uring_id) => {
            let connection_id = accepted_uring_id.connection_id();
            let op_code = accepted_uring_id.op_code();
            let stream_id = accepted_uring_id.stream_id();

            let completion_span = tracing::error_span!("completion", s_id = field::Empty, c_id = %connection_id, result, op = %op_code);

            if let Some(stream_id) = stream_id {
                completion_span.record("s_id", stream_id.to_string());
            }
            let _enter = completion_span.enter();

            let Some(completion) = Completion::from_entry(buf_ring, accepted_uring_id, &entry)
            else {
                return;
            };
            let sq: ScopedSubmissionQueue<'ring, 'ssq, AcceptedUringId> =
                ScopedSubmissionQueue::new(sq, accepted_uring_id);

            handle_accepted_completion(completion, state, buf_ring, sq, file_cache);
        }
        UringId::InlineTcpStream(inline_tcp_stream_uring_id) => {
            tracing::info!(message = "Go away sent, closing connection.", op = %inline_tcp_stream_uring_id.op_code());
            inline_tcp_stream_uring_id.close();
        }
        UringId::Cancelled(cancelled_uring_id) => {}
    }
}

fn handle_accepted_completion(
    completion: Completion,
    state: &mut State,
    buf_ring: &FixedSizeBufRing,
    sq: ScopedSubmissionQueue<'_, '_, AcceptedUringId>,
    file_cache: &FileCache,
) {
    match completion {
        Completion::AcceptMulti(result) => {
            on_accept_connection(result, state, buf_ring, sq);
        }
        Completion::RecvMulti(result) => {
            on_receive_data(result, state, sq, file_cache);
        }
        Completion::Send(result) => on_send_data(result),
        Completion::OpenAt(result) => on_open_at(result, state, sq, file_cache),
        Completion::Statx(result) => on_statx(result, state, sq, file_cache),
        Completion::Splice(Ok(())) => {
            tracing::debug!(message = "Splice successful");
        }
        Completion::Splice(Err(nix::Error::ECANCELED)) => {
            tracing::error!(message = "Cancelled splice");
        }
        Completion::Splice(Err(error)) => {
            tracing::error!(message = "Unexpected error code", %error);
            state.reset(sq.uring_id().connection_id());
        }
    }
}

fn on_accept_connection<T>(
    tcp_stream: Result<TcpStream, nix::Error>,
    state: &mut Slab<ConnectionId, RequestState>,
    buf_ring: &FixedSizeBufRing,
    sq: ScopedSubmissionQueue<'_, '_, T>,
) {
    let tcp_stream = match tcp_stream {
        Err(nix::Error::ECANCELED) => {
            tracing::info!("Cancelled connection acceptance.");
            return;
        }
        Err(error) => {
            tracing::warn!(message = "Connection could not be established.", %error);
            // No cleanup necessary since no resources have been associated with the connection yet.
            return;
        }
        Ok(fd) => fd,
    };

    let (connection_id, request_state) = match state.reserve() {
        Err(slab::Error::StorageFull) => {
            // Handle error by sending 503 response and then closing the connection.
            let entry = tcp_stream.write(RESPONSE_503_HTTP11.as_bytes());
            // tcp_stream fd needs to be leaked because we are out of storage.
            // Cleanup has to happen in the corresponding completion.
            let uring_id = UringId::from_tcp_stream_fd(tcp_stream.leak());

            let mut sq = sq.map(|_| uring_id);
            sq.add(entry);
            return;
        }
        Ok(reserved) => reserved,
    };

    let uring_id = UringId::from_connection_id(connection_id);

    let entry = tcp_stream.recv_multi(buf_ring);

    tracing::debug!(message = "Accepted connection", socket = %tcp_stream, c_id = %connection_id);
    request_state.tcp_stream = Some(tcp_stream);
    sq.map(|_| uring_id).add(entry);
}

fn on_receive_data(
    data: Result<RecvMultiData, RecvMultiError>,
    state: &mut Slab<ConnectionId, RequestState>,
    mut sq: ScopedSubmissionQueue<'_, '_, AcceptedUringId>,
    file_cache: &FileCache,
) {
    let connection_id = sq.uring_id().connection_id();
    let RecvMultiData { length, buf } = match data {
        Err(RecvMultiError::Kernel(nix::Error::ENOBUFS)) => {
            tracing::warn!(message = "Ran out of provided buffers. Attempting retry.");
            // TODO actually implement retry via Timeout
            return;
        }
        Err(RecvMultiError::Kernel(nix::Error::EBADF))
        | Err(RecvMultiError::Kernel(nix::Error::ENOENT)) => {
            tracing::warn!(message = "The client probably closed the connection.");

            sq.cancel_self();
            return;
        }
        Err(RecvMultiError::Kernel(error)) => {
            tracing::error!(message = "Unexpected error code", %error);
            // Since we don't really know what caused the error,
            // we just end the connection early.
            state.reset(sq.uring_id().connection_id());
            return;
        }
        Err(RecvMultiError::Cancelled) => {
            tracing::debug!("No more data to receive, cancelling RecvMulti");
            state.reset(sq.uring_id().connection_id());
            // TODO figure out why this cancellation is not necessary
            return;
        }
        Err(RecvMultiError::UnknownBuffer { flags }) => {
            tracing::warn!(
                message = "Received data from socket, but the kernel sent an unknown buffer id",
                flags
            );
            return;
        }
        Ok(data) => data,
    };

    let request_state = state.get_mut(connection_id);

    let Some(tcp_stream) = &mut request_state.tcp_stream else {
        tracing::error!("No active tcp stream, but received data");
        return;
    };

    tracing::debug!("Recv complete");
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut req = httparse::Request::new(&mut headers);
    let res = match req.parse(buf.as_slice()) {
        Ok(res) => res,
        Err(error) => {
            tracing::warn!(message = "Invalid request", %error);
            sq.add(tcp_stream.write(RESPONSE_400_HTTP11.as_bytes()));
            return;
        }
    };
    if res.is_partial() {
        // TODO figure out a way, potentially using pin, to store Request<'a> in RequestState struct
        tracing::warn!(message = "Request headers too large (> 4096 bytes)");
        sq.add(tcp_stream.write(RESPONSE_400_HTTP11.as_bytes()));
        return;
    }
    let path = match parse_path(req.path) {
        Ok(ok) => ok,
        Err(error) => {
            tracing::warn!(message = "Failed to parse path", %error);
            sq.add(tcp_stream.write(RESPONSE_413_HTTP11.as_bytes()));
            return;
        }
    };
    tracing::debug!(message = "Successfully parsed request", path = path);

    request_state.buf.reset();

    let path = unsafe { extend_str_with_null_byte(path, &request_state.buf) };

    if let Some(file) = file_cache.get(path) {
        send_file(
            &mut sq,
            &file,
            &request_state.buf,
            tcp_stream,
            &request_state.pipe_fds,
        )
    }

    request_state.path_ref = Some(path.as_ptr());

    sq.add_parallel([io::open_at(path), io::statx(path, &mut request_state.statx)]);
}

fn on_send_data(result: Result<u32, nix::Error>) {
    match result {
        Ok(length) => {
            tracing::debug!(message = "Sent bytes over socket", length);
        }
        Err(error) => {
            tracing::error!(message = "Unexpected error code", %error);
        }
    }
}

fn on_open_at(
    result: Result<File, nix::Error>,
    state: &mut Slab<ConnectionId, RequestState>,
    mut sq: ScopedSubmissionQueue<'_, '_, AcceptedUringId>,
    file_cache: &FileCache,
) {
    let connection_id = sq.uring_id().connection_id();
    let request_state = state.get_mut(connection_id);
    let Some(tcp_stream) = &request_state.tcp_stream else {
        tracing::error!("No active tcp stream, but opened file");
        return;
    };
    let mut file = match result {
        Ok(file) => file,
        Err(nix::Error::ENOENT) => {
            sq.add(tcp_stream.write(RESPONSE_404_HTTP11.as_bytes()));
            return;
        }
        Err(error) => {
            tracing::error!(message = "Unexpected error code", %error);
            sq.add(tcp_stream.write(RESPONSE_500_HTTP11.as_bytes()));
            return;
        }
    };

    let Some(statx) = request_state.statx.get() else {
        tracing::debug!("Opened file, waiting for statx");
        request_state.file = Some(file);
        return;
    };
    file.size = statx.stx_size as u32;

    if let Some(path_ref) = request_state.path_ref {
        let path = unsafe { CStr::from_ptr(path_ref) };
        let file = Arc::new(file);
        file_cache.insert(path.to_owned(), file.clone());
        tracing::debug!(?path, %file, "Inserting file descriptor into cache");

        send_file(
            &mut sq,
            &file,
            &request_state.buf,
            tcp_stream,
            &request_state.pipe_fds,
        );
    } else {
        tracing::warn!(%file, "Failed to cache file descriptor");
        send_file(
            &mut sq,
            &file,
            &request_state.buf,
            tcp_stream,
            &request_state.pipe_fds,
        );
    };
}

fn on_statx(
    result: Result<(), nix::Error>,
    state: &mut Slab<ConnectionId, RequestState>,
    mut sq: ScopedSubmissionQueue<'_, '_, AcceptedUringId>,
    file_cache: &FileCache,
) {
    let connection_id = sq.uring_id().connection_id();
    let request_state = state.get_mut(connection_id);
    let Some(tcp_stream) = &request_state.tcp_stream else {
        tracing::error!("No active tcp stream, but got file stats");
        return;
    };
    match result {
        Ok(_) => {}
        Err(nix::Error::ENOENT) => {
            sq.add(tcp_stream.write(RESPONSE_404_HTTP11.as_bytes()));
            return;
        }
        Err(error) => {
            tracing::error!(message = "Unexpected error code", %error);
            sq.add(tcp_stream.write(RESPONSE_500_HTTP11.as_bytes()));
            return;
        }
    };
    let statx = unsafe { request_state.statx.set_done() };
    let Some(file) = request_state.file.take() else {
        tracing::debug!(
            message = "Got statx, waiting for opening file",
            size = statx.stx_size,
            mode = statx.stx_mode
        );
        return;
    };

    if let Some(path_ref) = request_state.path_ref {
        let path = unsafe { CStr::from_ptr(path_ref) };
        let file = Arc::new(file);
        file_cache.insert(path.to_owned(), file.clone());

        send_file(
            &mut sq,
            &file,
            &request_state.buf,
            tcp_stream,
            &request_state.pipe_fds,
        );
    } else {
        send_file(
            &mut sq,
            &file,
            &request_state.buf,
            tcp_stream,
            &request_state.pipe_fds,
        );
    };
}

fn is_file(statx: &libc::statx) -> bool {
    statx.stx_mode as u32 & libc::S_IFMT != libc::S_IFREG
}

fn send_file(
    sq: &mut ScopedSubmissionQueue<AcceptedUringId>,
    file: &File,
    bump: &Bump<1024>,
    tcp_stream: &TcpStream,
    (pipe_out, pipe_in): &(PipeOut, PipeIn),
) {
    let response = match build_response_200_http_11(bump, file.size) {
        Ok(bytes) => bytes,
        Err(_) => RESPONSE_500_HTTP11.as_bytes(),
    };
    sq.add_sequential([
        tcp_stream.write(response),
        file.splice_to(pipe_out),
        tcp_stream.splice_from(pipe_in, file.size),
    ]);
}

#[derive(Debug)]
enum Error<'a> {
    PathDidNotStartWithSlash(&'a str),
    PathContainedDotDot(&'a str),
}

impl<'a> Display for Error<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::PathDidNotStartWithSlash(path) => {
                f.write_fmt(format_args!("Path '{path}' did not start with '/'"))
            }
            Error::PathContainedDotDot(path) => {
                f.write_fmt(format_args!("Path '{path}' contained '..'"))
            }
        }
    }
}

fn parse_path(path: Option<&str>) -> Result<&str, Error> {
    let Some(path) = path else {
        return Ok("index.html");
    };
    let Some(path) = path.strip_prefix("/") else {
        return Err(Error::PathDidNotStartWithSlash(path));
    };
    if path.contains("..") {
        return Err(Error::PathContainedDotDot(path));
    }
    Ok(path)
}

unsafe fn extend_str_with_null_byte<'a, const N: usize>(
    original: &str,
    bump: &'a Bump<N>,
) -> &'a CStr {
    let slice = bump.alloc(original.len() + 1);
    slice[0..original.len()].copy_from_slice(original.as_bytes());
    slice[original.len()] = 0;
    CStr::from_bytes_with_nul_unchecked(slice)
}

fn build_response_200_http_11<const N: usize>(
    bump: &Bump<N>,
    content_length: u32,
) -> std::io::Result<&[u8]> {
    bump.write_fmt_to_ref(format_args!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {content_length}\r\n\r\n"
    ))
}

const RESPONSE_400_HTTP11: &str =
    "HTTP/1.1 400 OK\r\nContent-Type: text/html\r\nContent-Length: 11\r\n\r\nBad request";

const RESPONSE_404_HTTP11: &str =
    "HTTP/1.1 404 OK\r\nContent-Type: text/html\r\nContent-Length: 9\r\n\r\nNot found";

const RESPONSE_413_HTTP11: &str =
    "HTTP/1.1 413 OK\r\nContent-Type: text/html\r\nContent-Length: 17\r\n\r\nContent too large";

const RESPONSE_500_HTTP11: &str =
    "HTTP/1.1 503 OK\r\nContent-Type: text/html\r\nContent-Length: 21\r\n\r\nInternal server error";

const RESPONSE_503_HTTP11: &str =
    "HTTP/1.1 503 OK\r\nContent-Type: text/html\r\nContent-Length: 19\r\n\r\nService unavailable";

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
    fn extend() {
        let readme = c"README.md";
        let mut buf: OptZeroed<libc::statx> = unsafe { OptZeroed::zeroed() };
        unsafe {
            libc::statx(
                libc::AT_FDCWD,
                readme.as_ptr(),
                0,
                libc::STATX_ALL,
                buf.get_mut(),
            )
        };
        unsafe { buf.set_done() };
    }

    #[test]
    fn serve() -> anyhow::Result<()> {
        let listener = TcpListener::bind(("127.0.0.1", 3457))?;
        let socket_fd = listener.as_raw_fd();
        let mut ring: IoUring = IoUring::builder()
            //.setup_coop_taskrun()
            //.setup_sqpoll(1024 * 1024)
            //.setup_single_issuer()
            .build(64)?;
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
