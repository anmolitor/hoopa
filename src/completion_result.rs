use std::os::fd::FromRawFd;

use io_uring::cqueue;

use crate::{
    buf::{FixedSizeBufRing, GBuf},
    fd::Fd,
    uring_id::{OpCode, UringId},
};

pub type CompletionResult = Result<Completion, nix::Error>;

pub enum Completion {
    AcceptMulti { connection_fd: Fd },
    RecvMulti(RecvMultiResult),
    Send { length: u32 },
    AsyncCancel,
}

pub enum RecvMultiResult {
    Data { length: u32, buf: GBuf },
    UnknownBuffer,
    Cancelled,
}

impl Completion {
    pub fn from_entry(
        buf_ring: &FixedSizeBufRing,
        entry: &cqueue::Entry,
    ) -> Result<Completion, (OpCode, nix::Error)> {
        let uring_id = UringId::from_cqe(entry);
        let op_code = uring_id.op_code();
        let result = entry.result();
        if result < 0 {
            return Err((op_code, nix::errno::Errno::from_raw(-result)));
        }
        match op_code {
            OpCode::AcceptMulti => Ok(Completion::AcceptMulti {
                connection_fd: unsafe { Fd::from_raw_fd(result) },
            }),
            OpCode::RecvMulti => {
                if result == 0 {
                    return Ok(Completion::RecvMulti(RecvMultiResult::Cancelled));
                }
                let Some(buf) = buf_ring.get_buf(result as u32, entry.flags()) else {
                    return Ok(Completion::RecvMulti(RecvMultiResult::UnknownBuffer));
                };
                Ok(Completion::RecvMulti(RecvMultiResult::Data {
                    length: result as u32,
                    buf,
                }))
            }
            OpCode::Send => Ok(Completion::Send {
                length: result as u32,
            }),
            OpCode::AsyncCancel => Ok(Completion::AsyncCancel),
        }
    }
}
