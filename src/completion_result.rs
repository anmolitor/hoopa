use std::os::fd::FromRawFd;

use io_uring::cqueue;

use crate::{
    buf::{FixedSizeBufRing, GBuf},
    io::{File, OpCode, TcpStream},
    uring_id::{AcceptedUringId, HasOpCode as _},
};

pub type CompletionResult = Result<Completion, nix::Error>;

pub enum Completion {
    AcceptMulti(Result<TcpStream, nix::Error>),
    RecvMulti(Result<RecvMultiData, RecvMultiError>),
    Send(Result<u32, nix::Error>),
    OpenAt(Result<File, nix::Error>),
    Statx(Result<(), nix::Error>),
    Splice(Result<(), nix::Error>),
}

#[derive(Debug)]
pub struct RecvMultiData {
    pub length: u32,
    pub buf: GBuf,
}

#[derive(Debug)]
pub enum RecvMultiError {
    Kernel(nix::Error),
    Cancelled,
    UnknownBuffer { flags: u32 },
}

impl Completion {
    pub fn from_entry(
        buf_ring: &FixedSizeBufRing,
        uring_id: AcceptedUringId,
        entry: &cqueue::Entry,
    ) -> Option<Completion> {
        let op_code = uring_id.op_code();
        let result = entry.result();

        match op_code {
            OpCode::AcceptMulti => {
                let result =
                    catch_nix_error(result).map(|_| unsafe { TcpStream::from_raw_fd(result) });
                Some(Completion::AcceptMulti(result))
            }
            OpCode::RecvMulti => {
                let result = catch_nix_error(result)
                    .map_err(RecvMultiError::Kernel)
                    .and_then(|length| {
                        if result == 0 {
                            return Err(RecvMultiError::Cancelled);
                        }
                        let Some(buf) = buf_ring.get_buf(length, entry.flags()) else {
                            return Err(RecvMultiError::UnknownBuffer {
                                flags: entry.flags(),
                            });
                        };
                        Ok(RecvMultiData { length, buf })
                    });
                Some(Completion::RecvMulti(result))
            }
            OpCode::Send => Some(Completion::Send(catch_nix_error(result))),
            OpCode::AsyncCancel => {
                tracing::error!(message = "AsyncCancel should be its own UringId kind, but it got completed with kind 'Accepted'.");
                None
            }
            OpCode::OpenAt => {
                let result = catch_nix_error(result).map(|_| unsafe { File::from_raw_fd(result) });
                Some(Completion::OpenAt(result))
            }
            OpCode::Statx => {
                let result = catch_nix_error(result).map(|_| ());
                Some(Completion::Statx(result))
            }
            OpCode::Splice => {
                let result = catch_nix_error(result).map(|_| ());
                Some(Completion::Splice(result))
            }
            OpCode::Nop => {
                tracing::error!(message = "Nop should only be used as a default opcode to later be replaced, but it got completed with kind 'Accepted'.");
                None
            }
        }
    }
}

fn catch_nix_error(result: i32) -> Result<u32, nix::Error> {
    if result < 0 {
        return Err(nix::errno::Errno::from_raw(-result));
    }
    Ok(result as u32)
}
