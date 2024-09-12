use std::collections::VecDeque;
use std::fs::File;
use std::io::Read;
use std::net::TcpListener;
use std::os::fd::{AsFd, FromRawFd, OwnedFd};
use std::os::unix::io::{AsRawFd, RawFd};
use std::rc::Rc;
use std::{io, ptr};

use io_uring::squeue::Flags;
use io_uring::{opcode, squeue, types, IoUring, SubmissionQueue};
use slab::Slab;

#[derive(Debug)]
enum Token {
    Accept,
    Poll { fd: RawFd },
    Conn(ConnectionScopedToken),
}

#[derive(Debug)]
enum ConnectionScopedToken {
    Read {
        fd: Fd,
        buf_index: usize,
    },
    WriteFileHttpPrefix {
        fd: Fd,
        file_handle: Fd,
        pipe_read: Fd,
        pipe_write: Fd,
    },
    WriteFileToPipe {
        fd: Fd,
        file_handle: Fd,
        pipe_read: Fd,
        pipe_write: Fd,
    },
    WritePipeToSocket {
        fd: Fd,
        pipe_read: Fd,
    },
    CloseConnection,
}

pub struct AcceptCount {
    entry: squeue::Entry,
    count: usize,
}

impl AcceptCount {
    fn new(fd: RawFd, token: usize, count: usize) -> AcceptCount {
        AcceptCount {
            entry: opcode::Accept::new(types::Fd(fd), ptr::null_mut(), ptr::null_mut())
                .build()
                .user_data(token as _),
            count,
        }
    }

    pub fn push_to(&mut self, sq: &mut SubmissionQueue<'_>) {
        while self.count > 0 {
            unsafe {
                match sq.push(&self.entry) {
                    Ok(_) => self.count -= 1,
                    Err(_) => break,
                }
            }
        }

        sq.sync();
    }
}

pub fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", 3456))?;
    let listener_fd = listener.as_raw_fd();
    let cpus = num_cpus::get_physical();
    println!("listen {} with {} cores", listener.local_addr()?, cpus);

    let mut handles = Vec::with_capacity(cpus - 1);
    for cpu in 1..cpus {
        let join_handle = std::thread::spawn(move || {
            worker(listener_fd, cpu).expect("Worker failed");
        });
        handles.push(join_handle);
    }

    let result = worker(listener_fd, 0);
    for handle in handles {
        handle.join().expect("Join");
    }
    result?;
    Ok(())
}

fn submit(
    entry: squeue::Entry,
    queue: &mut squeue::SubmissionQueue,
    backlog: &mut VecDeque<squeue::Entry>,
    token_index: usize,
) {
    let entry = entry.user_data(token_index as _);
    unsafe {
        if queue.push(&entry).is_err() {
            backlog.push_back(entry);
        }
    }
}

#[derive(Debug)]
enum Error<'a> {
    ReadFromSocketFailed(Fd),
    InvalidHttpRequest(httparse::Error),
    PathDidNotStartWithSlash,
    PathContainedDotDot(&'a str),
    FileNotFound(&'a str, io::Error),
    FailedToGetFileMetadata(&'a str, io::Error),
    NotAFile(&'a str),
    FailedToCreatePipe(nix::Error),
}

fn event_loop<'a>(
    token: &'a mut ConnectionScopedToken,
    ret: i32,
    queue: &mut squeue::SubmissionQueue,
    backlog: &mut VecDeque<squeue::Entry>,
    token_index: usize,
    allocator: &'a mut Slab<Box<[u8]>>,
) -> Result<(), Error<'a>> {
    replace_with::replace_with_or_abort_and_return(token, |token| match token {
        ConnectionScopedToken::Read { fd, buf_index } => {
            if ret == 0 {
                return (
                    Err(Error::ReadFromSocketFailed(fd)),
                    ConnectionScopedToken::CloseConnection,
                );
            }
            let len = ret as usize;
            let mut headers = [httparse::EMPTY_HEADER; 16];
            let mut req = httparse::Request::new(&mut headers);
            let res = match req.parse(&allocator[buf_index][0..len]) {
                Err(err) => {
                    return (
                        Err(Error::InvalidHttpRequest(err)),
                        ConnectionScopedToken::CloseConnection,
                    );
                }
                Ok(ok) => ok,
            };
            let path = match parse_path(req.path) {
                Ok(ok) => ok,
                Err(err) => {
                    return (Err(err), ConnectionScopedToken::CloseConnection);
                }
            };
            let file = match std::fs::File::open(path) {
                Err(err) => {
                    return (
                        Err(Error::FileNotFound(path, err)),
                        ConnectionScopedToken::CloseConnection,
                    );
                }
                Ok(ok) => ok,
            };
            let metadata = match file.metadata() {
                Err(err) => {
                    return (
                        Err(Error::FailedToGetFileMetadata(path, err)),
                        ConnectionScopedToken::CloseConnection,
                    );
                }
                Ok(ok) => ok,
            };
            if !metadata.is_file() {
                return (
                    Err(Error::NotAFile(path)),
                    ConnectionScopedToken::CloseConnection,
                );
            }

            let (pipe_read, pipe_write) = match nix::unistd::pipe() {
                Err(err) => {
                    return (
                        Err(Error::FailedToCreatePipe(err)),
                        ConnectionScopedToken::CloseConnection,
                    );
                }
                Ok((r, w)) => (Fd(r), (Fd(w))),
            };

            let file_handle = Fd::from(file);

            let write_e =
                opcode::Send::new(types::Fd::from(&fd), RESPONSE.as_ptr(), RESPONSE.len() as _)
                    .build()
                    .flags(Flags::IO_LINK);

            let splice_file_to_pipe = opcode::Splice::new(
                types::Fd::from(&file_handle),
                0,
                types::Fd::from(&pipe_write),
                -1,
                metadata.len() as _,
            )
            .build()
            .flags(Flags::IO_LINK);

            let splice_pipe_to_socket = opcode::Splice::new(
                types::Fd::from(&pipe_read),
                -1,
                types::Fd::from(&fd),
                -1,
                metadata.len() as _,
            )
            .build();

            submit(write_e, queue, backlog, token_index);
            submit(splice_file_to_pipe, queue, backlog, token_index);
            submit(splice_pipe_to_socket, queue, backlog, token_index);

            (
                Ok(()),
                ConnectionScopedToken::WriteFileHttpPrefix {
                    fd,
                    file_handle,
                    pipe_read,
                    pipe_write,
                },
            )
        }
        ConnectionScopedToken::WriteFileHttpPrefix {
            fd,
            file_handle,
            pipe_read,
            pipe_write,
        } => (
            Ok(()),
            ConnectionScopedToken::WriteFileToPipe {
                fd,
                file_handle,
                pipe_read,
                pipe_write,
            },
        ),
        ConnectionScopedToken::WriteFileToPipe {
            fd,
            file_handle: _,
            pipe_read,
            pipe_write: _,
        } => (
            Ok(()),
            ConnectionScopedToken::WritePipeToSocket { fd, pipe_read },
        ),
        ConnectionScopedToken::WritePipeToSocket {
            fd: _,
            pipe_read: _,
        } => (Ok(()), ConnectionScopedToken::CloseConnection),
        ConnectionScopedToken::CloseConnection => (Ok(()), ConnectionScopedToken::CloseConnection),
    })
}

const RESPONSE: &str = "HTTP/1.1 200 OK\r\n\r\n";

fn worker(listener_fd: i32, thread_id: usize) -> anyhow::Result<()> {
    let mut ring = IoUring::new(256)?;

    let mut allocator: Slab<Box<[u8]>> = Slab::with_capacity(64);

    let mut backlog = VecDeque::new();
    let mut bufpool = Vec::with_capacity(64);
    let mut token_alloc = Slab::with_capacity(64);

    let (submitter, mut sq, mut cq) = ring.split();

    let mut accept = AcceptCount::new(listener_fd, token_alloc.insert(Token::Accept), 3);

    accept.push_to(&mut sq);

    loop {
        match submitter.submit_and_wait(1) {
            Ok(_) => (),
            Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => (),
            Err(err) => return Err(err.into()),
        }
        cq.sync();

        // clean backlog
        loop {
            if sq.is_full() {
                match submitter.submit() {
                    Ok(_) => (),
                    Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => break,
                    Err(err) => return Err(err.into()),
                }
            }
            sq.sync();

            match backlog.pop_front() {
                Some(sqe) => unsafe {
                    let _ = sq.push(&sqe);
                },
                None => break,
            }
        }

        accept.push_to(&mut sq);

        for cqe in &mut cq {
            let ret = cqe.result();
            let token_index = cqe.user_data() as usize;

            if ret < 0 {
                eprintln!(
                    "token {:?} error: {:?}",
                    token_alloc.get(token_index),
                    io::Error::from_raw_os_error(-ret)
                );
                continue;
            }

            let token = &mut token_alloc[token_index];
            println!("Token: {token:?}, ret {ret}, token_index {token_index}");
            match token {
                Token::Accept => {
                    accept.count += 1;

                    let fd = ret;
                    let token_index = token_alloc.insert(Token::Poll { fd });

                    let poll_e = opcode::PollAdd::new(types::Fd(fd), libc::POLLIN as _).build();

                    submit(poll_e, &mut sq, &mut backlog, token_index);
                }
                Token::Poll { fd } => {
                    let (buf_index, buf) = match bufpool.pop() {
                        Some(buf_index) => (buf_index, &mut allocator[buf_index]),
                        None => {
                            let buf_entry = allocator.vacant_entry();
                            let buf_index = buf_entry.key();
                            (buf_index, buf_entry.insert(Box::new([0u8; 2048])))
                        }
                    };

                    let fd = unsafe { Fd::from_raw_fd(*fd) };
                    let read_e =
                        opcode::Recv::new(types::Fd::from(&fd), buf.as_mut_ptr(), buf.len() as _)
                            .build();
                    *token = Token::Conn(ConnectionScopedToken::Read { fd, buf_index });

                    submit(read_e, &mut sq, &mut backlog, token_index);
                }
                Token::Conn(conn) => {
                    event_loop(
                        conn,
                        ret,
                        &mut sq,
                        &mut backlog,
                        token_index,
                        &mut allocator,
                    )
                    .unwrap();
                }
            }
        }
    }
}

fn parse_path<'a>(path: Option<&'a str>) -> Result<&'a str, Error<'a>> {
    let Some(path) = path else {
        return Ok("index.html");
    };
    let Some(path) = path.strip_prefix("/") else {
        return Err(Error::PathDidNotStartWithSlash);
    };
    if path.contains("..") {
        return Err(Error::PathContainedDotDot(path));
    }
    return Ok(path);
}

#[derive(Debug)]
struct Fd(OwnedFd);

impl AsRawFd for Fd {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

impl FromRawFd for Fd {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Fd(OwnedFd::from_raw_fd(fd))
    }
}

impl From<&Fd> for types::Fd {
    fn from(value: &Fd) -> Self {
        types::Fd(value.0.as_raw_fd())
    }
}

impl From<File> for Fd {
    fn from(value: File) -> Self {
        Fd(value.into())
    }
}

#[test]
fn test() {
    println!(
        "{:?}",
        "GET /test HTTP/1.1
Host: localhost:3456
User-Agent: curl/8.4.0
Accept: */*"
            .as_bytes()
    )
}

#[test]
fn splice() -> anyhow::Result<()> {
    let mut ring = IoUring::new(8)?;
    let file_target = std::fs::File::open("test.txt")?;

    let (read, write) = nix::unistd::pipe()?;
    let file = std::fs::File::open(
        "/workspaces/series_game_from_scratch/4_io_uring_echo_server/Cargo.toml",
    )?;
    let splice_1 = opcode::Splice::new(
        types::Fd(file.as_raw_fd()),
        0,
        types::Fd(write.as_raw_fd()),
        -1,
        u32::MAX,
    )
    .build()
    .flags(Flags::IO_LINK);
    unsafe { ring.submission().push_multiple(&[splice_1])? };
    ring.submit_and_wait(1)?;
    let splice_2 = opcode::Splice::new(
        types::Fd(read.as_raw_fd()),
        -1,
        types::Fd(file_target.as_raw_fd()),
        -1,
        u32::MAX,
    )
    .build();
    println!("test");

    unsafe { ring.submission().push_multiple(&[splice_2])? };
    ring.submit_and_wait(1)?;
    ring.completion().sync();
    Ok(())
}
