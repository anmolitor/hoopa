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

use crate::fd::Fd;
use crate::http2::{Frame, FrameType, Setting, SettingPairs, StreamId};

#[derive(Debug, Clone)]
enum Token {
    Accept,
    Poll {
        fd: Fd,
        http2: bool,
    },
    Read {
        fd: Fd,
        buf_index: usize,
        http2: bool,
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
    NoHttp2Preface(&'a [u8]),
}

fn event_loop<'a>(
    ret: i32,
    queue: &mut squeue::SubmissionQueue,
    backlog: &mut VecDeque<squeue::Entry>,
    token_index: usize,
    token_allocator: &'a mut Slab<Token>,
    allocator: &'a mut Slab<Box<[u8]>>,
    accept: &mut AcceptCount,
) -> Result<(), Error<'a>> {
    let token = &mut (token_allocator[token_index]);
    println!("Token {token:?}, ret {ret}");
    match token.clone() {
        Token::Accept => {
            accept.count += 1;

            let fd = unsafe { Fd::from_raw_fd(ret) };

            let poll_e = opcode::PollAdd::new(types::Fd::from(&fd), libc::POLLIN as _).build();
            let token_index = token_allocator.insert(Token::Poll { fd, http2: false });

            submit(poll_e, queue, backlog, token_index);

            Ok(())
        }
        Token::Poll { fd, http2 } => {
            let entry = allocator.vacant_entry();
            let buf_index = entry.key();
            let buf = entry.insert(Box::new([0u8; 2048]));

            let read_e =
                opcode::Recv::new(types::Fd::from(&fd), buf.as_mut_ptr(), buf.len() as _).build();

            submit(read_e, queue, backlog, token_index);
            *token = Token::Read {
                fd,
                buf_index,
                http2,
            };
            Ok(())
        }
        Token::Read {
            fd,
            buf_index,
            http2,
        } => {
            if ret == 0 {
                return Err(Error::ReadFromSocketFailed(fd));
            }
            let len = ret as usize;
            let buf = &mut allocator[buf_index][0..len];
            println!("Buf: {}", String::from_utf8_lossy(buf));

            if http2 {
                let Some(buf) = buf.strip_prefix(PREFACE_HTTP2) else {
                    return Err(Error::NoHttp2Preface(buf));
                };
                // let setting_payload = {
                //     SettingPairs(&[
                //         (Setting::EnablePush, 0),
                //         (Setting::HeaderTableSize, s.header_table_size),
                //         (Setting::InitialWindowSize, s.initial_window_size),
                //         (
                //             Setting::MaxConcurrentStreams,
                //             s.max_concurrent_streams.unwrap_or(u32::MAX),
                //         ),
                //         (Setting::MaxFrameSize, s.max_frame_size),
                //         (Setting::MaxHeaderListSize, s.max_header_list_size),
                //     ])
                //     .into_piece(&mut self.out_scratch)
                //     .map_err(ServeError::DownstreamWrite)?
                // };
                let frame = Frame::new(
                    FrameType::Settings(Default::default()),
                    StreamId::CONNECTION,
                );
                println!("Buf without preface {buf:?}");
            }

            let mut headers = [httparse::EMPTY_HEADER; 16];
            let mut req = httparse::Request::new(&mut headers);
            let res = match req.parse(buf) {
                Err(err) => {
                    return Err(Error::InvalidHttpRequest(err));
                }
                Ok(ok) => ok,
            };
            if req.headers.contains(&httparse::Header {
                name: "Upgrade",
                value: b"h2c",
            }) {
                println!("Upgrading to http2");
                let write_e = opcode::Send::new(
                    types::Fd::from(&fd),
                    RESPONSE_101_UPGRADE_HTTP2.as_ptr(),
                    RESPONSE_101_UPGRADE_HTTP2.len() as _,
                )
                .build();

                let poll_e = opcode::PollAdd::new(types::Fd::from(&fd), libc::POLLIN as _).build();
                *token = Token::Poll { fd, http2: true };

                submit(write_e, queue, backlog, token_index);
                submit(poll_e, queue, backlog, token_index);

                return Ok(());
            }
            let path = match parse_path(req.path) {
                Ok(ok) => ok,
                Err(err) => {
                    return Err(err);
                }
            };
            let file = match std::fs::File::open(path) {
                Err(err) => {
                    return Err(Error::FileNotFound(path, err));
                }
                Ok(ok) => ok,
            };
            let metadata = match file.metadata() {
                Err(err) => {
                    return Err(Error::FailedToGetFileMetadata(path, err));
                }
                Ok(ok) => ok,
            };
            if !metadata.is_file() {
                return Err(Error::NotAFile(path));
            }

            let (pipe_read, pipe_write) = match nix::unistd::pipe() {
                Err(err) => {
                    return Err(Error::FailedToCreatePipe(err));
                }
                Ok((r, w)) => (Fd::from(r), (Fd::from(w))),
            };

            let file_handle = Fd::from(file);

            let write_e = opcode::Send::new(
                types::Fd::from(&fd),
                RESPONSE_200_HTTP11.as_ptr(),
                RESPONSE_200_HTTP11.len() as _,
            )
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

            *token = Token::WriteFileHttpPrefix {
                fd,
                file_handle,
                pipe_read,
                pipe_write,
            };
            Ok(())
        }
        Token::WriteFileHttpPrefix {
            fd,
            file_handle,
            pipe_read,
            pipe_write,
        } => {
            *token = Token::WriteFileToPipe {
                fd,
                file_handle,
                pipe_read,
                pipe_write,
            };
            Ok(())
        }
        Token::WriteFileToPipe {
            fd,
            file_handle: _,
            pipe_read,
            pipe_write: _,
        } => {
            *token = Token::WritePipeToSocket { fd, pipe_read };
            Ok(())
        }
        Token::WritePipeToSocket {
            fd: _,
            pipe_read: _,
        } => Ok(()),
        Token::CloseConnection => Ok(()),
    }
}

const RESPONSE_200_HTTP11: &str = "HTTP/1.1 200 OK\r\n\r\n";
const RESPONSE_101_UPGRADE_HTTP2: &str =
    "HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c\r\nConnection: Upgrade\r\n\r\n";
pub const PREFACE_HTTP2: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

fn worker(listener_fd: i32, thread_id: usize) -> anyhow::Result<()> {
    let mut ring = IoUring::new(256)?;

    let mut allocator: Slab<Box<[u8]>> = Slab::with_capacity(64);

    let mut backlog = VecDeque::new();
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

            event_loop(
                ret,
                &mut sq,
                &mut backlog,
                token_index,
                &mut token_alloc,
                &mut allocator,
                &mut accept,
            );
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
    Ok(path)
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
