use std::{
    fmt::Display,
    os::fd::{FromRawFd, OwnedFd, RawFd},
};

use io_uring::{cqueue, squeue};
use num_enum::TryFromPrimitive as _;

use crate::io::OpCode;

/// This is the ID we use to associate `completion`s with `submission`s.
/// It is a simple `u64`, but we use the bytes in the following way:
///
/// < 56 bits > < 8 bits >
///   ^^^^^^^     ^^^^^^
///    rest        kind
///
/// The "kind" lower bits are the most important since we can use them to interpret the "rest" differently.
///
/// For kind == 0:
///
/// < 16 bits > < 1 bit >  < 31 bits >  < 8 bits > < 0 0 0 0 0 0 0 0 >
///   ^^^^^^^     ^^^^^      ^^^^^^^      ^^^^^^     ^^^^^^^^^^^^^^^
///   conn id    is http2   stream id     opcode           kind
///
/// This should be the "kind".used in almost all scenarios
///
/// For kind == 1:
///
/// < 16 bits > < 32 bits > < 8 bits > < 0 0 0 0 0 0 0 1 >
///   ^^^^^^^     ^^^^^^^     ^^^^^^     ^^^^^^^^^^^^^^^
///  Placeholder  SocketFD    opcode           kind
///
/// For kind == 2:
///
/// < 16 bits > < 1 bit >  < 31 bits >      < 8 bits >      < 0 0 0 0 0 0 1 0 >
///   ^^^^^^^     ^^^^^      ^^^^^^^          ^^^^^^          ^^^^^^^^^^^^^^^
///   conn id    is http2   stream id    cancelled opcode          kind
///
/// This is useful when there are memory issues and we cannot store the SocketFD anywhere.
/// The idea is that we hold onto the SocketFD implicitely (instead of using Drop as usually)
/// and cleanup when the completion comes in.
///
/// This is one of the reasons why io_uring is good: we can concurrently make Kernel calls.
/// For example, for opening a file and getting metadata about the file,
/// we submit two entries to the submission queue. These two entries can then be completed in any order
/// by the Kernel and the results will be placed in the completion queue.
/// Since a completion entry just consists of i32 result (which depends on the call made)
/// and the same u64 we can set on the submission entry, the u64 is the only chance we get to
/// know which is which. In the example, the OpenAt entry will get the same conn id bits, http2 bit, stream id bits and counter as the Statx entry, but the opcode will be different since they are two different kernel operations.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum UringId {
    Accepted(AcceptedUringId),
    InlineTcpStream(InlineTcpStreamUringId),
    Cancelled(CancelledUringId),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct AcceptedUringId {
    connection_id: ConnectionId,
    stream_id: Option<StreamId>,
    op_code: OpCode,
}

pub trait HasOpCode {
    fn op_code(&self) -> OpCode;
    fn set_op_code(self, op_code: OpCode) -> Self;
}

pub trait SetUserData {
    fn set_user_data(self, entry: squeue::Entry) -> squeue::Entry;
}

impl HasOpCode for AcceptedUringId {
    fn op_code(&self) -> OpCode {
        self.op_code
    }

    fn set_op_code(mut self, op_code: OpCode) -> Self {
        self.op_code = op_code;
        self
    }
}

impl SetUserData for AcceptedUringId {
    fn set_user_data(self, entry: squeue::Entry) -> squeue::Entry {
        entry.user_data(self.to_user_data())
    }
}

impl AcceptedUringId {
    pub fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub fn stream_id(&self) -> Option<StreamId> {
        self.stream_id
    }

    pub fn to_user_data(self) -> u64 {
        let AcceptedUringId {
            connection_id,
            stream_id,
            op_code,
        } = self;
        let kind = UringId::KIND_ACCEPTED as u64;
        let stream_id_val = stream_id.map_or(0, |id| id.0 as u64);
        let is_http2 = if stream_id.is_some() { 1u64 } else { 0u64 };

        ((connection_id.0 as u64) << 48)
            | (is_http2 << 47)
            | (stream_id_val << 16)
            | ((op_code as u64) << 8)
            | kind
    }

    pub fn to_cancel_id(self, cancelled_op_code: OpCode) -> UringId {
        UringId::Cancelled(CancelledUringId {
            connection_id: self.connection_id,
            stream_id: self.stream_id,
            cancelled_op_code,
        })
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct InlineTcpStreamUringId {
    raw_fd: RawFd,
    op_code: OpCode,
}

impl InlineTcpStreamUringId {
    pub fn op_code(&self) -> OpCode {
        self.op_code
    }

    pub fn close(self) {
        // Drop will cleanup automatically
        unsafe { OwnedFd::from_raw_fd(self.raw_fd) };
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct CancelledUringId {
    connection_id: ConnectionId,
    stream_id: Option<StreamId>,
    cancelled_op_code: OpCode,
}

impl CancelledUringId {
    pub fn connection_id(&self) -> ConnectionId {
        self.connection_id
    }

    pub fn cancelled_op_code(&self) -> OpCode {
        self.cancelled_op_code
    }

    pub fn stream_id(&self) -> Option<StreamId> {
        self.stream_id
    }
}

/// Each accepted connection gets a id which is assigned via a Slab allocator
/// Inside the slot, the connection file descriptor is stored and
/// the current state of the connection.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ConnectionId(u16);

impl From<u16> for ConnectionId {
    fn from(value: u16) -> Self {
        ConnectionId(value)
    }
}

impl From<ConnectionId> for u16 {
    fn from(value: ConnectionId) -> Self {
        value.0
    }
}

/// When upgrading to HTTP/2, there can be multiple requests in flight
/// at the same time. To be able to differentiate responses, the client sets a 31-bit StreamId
/// on each frame, which we then send back with the corresponding header/data frames.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct StreamId(u32);

pub struct UringIdOutOfRange<T>(T);

impl HasOpCode for UringId {
    fn op_code(&self) -> OpCode {
        match self {
            UringId::Accepted(accepted_uring_id) => accepted_uring_id.op_code,
            UringId::InlineTcpStream(inline_tcp_stream_uring_id) => {
                inline_tcp_stream_uring_id.op_code
            }
            UringId::Cancelled(cancelled_uring_id) => cancelled_uring_id.cancelled_op_code,
        }
    }

    fn set_op_code(self, op_code: OpCode) -> Self {
        match self {
            UringId::Accepted(mut accepted_uring_id) => {
                accepted_uring_id.op_code = op_code;
                UringId::Accepted(accepted_uring_id)
            }
            UringId::InlineTcpStream(mut inline_tcp_stream_uring_id) => {
                inline_tcp_stream_uring_id.op_code = op_code;
                UringId::InlineTcpStream(inline_tcp_stream_uring_id)
            }
            UringId::Cancelled(mut cancelled_uring_id) => {
                cancelled_uring_id.cancelled_op_code = op_code;
                UringId::Cancelled(cancelled_uring_id)
            }
        }
    }
}

impl SetUserData for UringId {
    fn set_user_data(self, entry: squeue::Entry) -> squeue::Entry {
        entry.user_data(self.to_user_data())
    }
}

impl UringId {
    const KIND_ACCEPTED: u8 = 0;
    const KIND_INLINE_TCP_STREAM: u8 = 1;
    const KIND_CANCELLED: u8 = 2;

    pub fn from_connection_id(connection_id: ConnectionId) -> UringId {
        UringId::Accepted(AcceptedUringId {
            connection_id,
            stream_id: None,
            op_code: OpCode::Nop,
        })
    }

    pub fn from_tcp_stream_fd(raw_fd: RawFd) -> UringId {
        UringId::InlineTcpStream(InlineTcpStreamUringId {
            raw_fd,
            op_code: OpCode::Nop,
        })
    }

    pub fn from_cqe(entry: &cqueue::Entry) -> Self {
        Self::from_user_data(entry.user_data())
    }

    fn from_user_data(user_data: u64) -> Self {
        let op_code = ((user_data >> 8) & 0xFF) as u8;
        let op_code = OpCode::try_from_primitive(op_code).expect("Invalid op code");

        let kind = (user_data & 0xff) as u8;
        match kind {
            Self::KIND_ACCEPTED => {
                let connection_id = ConnectionId((user_data >> 48) as u16);
                let is_http2 = ((user_data >> 47) & 0x1) != 0;
                let stream_id = if is_http2 {
                    Some(StreamId(((user_data >> 16) & 0x7FFFFFFF) as u32))
                } else {
                    None
                };

                UringId::Accepted(AcceptedUringId {
                    connection_id,
                    stream_id,
                    op_code,
                })
            }
            Self::KIND_INLINE_TCP_STREAM => {
                let mask = 0xffff_ffff << 16;
                let raw_fd = ((user_data & mask) >> 16) as i32;

                UringId::InlineTcpStream(InlineTcpStreamUringId { raw_fd, op_code })
            }
            Self::KIND_CANCELLED => {
                let connection_id = ConnectionId((user_data >> 48) as u16);
                let is_http2 = ((user_data >> 47) & 0x1) != 0;
                let stream_id = if is_http2 {
                    Some(StreamId(((user_data >> 16) & 0x7FFFFFFF) as u32))
                } else {
                    None
                };

                UringId::Cancelled(CancelledUringId {
                    connection_id,
                    stream_id,
                    cancelled_op_code: op_code,
                })
            }
            _ => panic!("Invalid UringId kind {}", kind),
        }
    }

    pub fn to_user_data(self) -> u64 {
        match self {
            UringId::Accepted(accepted) => accepted.to_user_data(),
            UringId::InlineTcpStream(InlineTcpStreamUringId { raw_fd, op_code }) => {
                let kind = Self::KIND_INLINE_TCP_STREAM as u64;
                ((raw_fd as u64) << 32) | ((op_code as u64) << 8) | kind
            }
            UringId::Cancelled(CancelledUringId {
                connection_id,
                stream_id,
                cancelled_op_code,
            }) => {
                let kind = Self::KIND_CANCELLED as u64;
                let stream_id_val = stream_id.map_or(0, |id| id.0 as u64);
                let is_http2 = if stream_id.is_some() { 1u64 } else { 0u64 };

                ((connection_id.0 as u64) << 48)
                    | (is_http2 << 47)
                    | (stream_id_val << 16)
                    | ((cancelled_op_code as u64) << 8)
                    | kind
            }
        }
    }
}

impl Display for ConnectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        u16::fmt(&self.0, f)
    }
}

impl Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        u32::fmt(&self.0, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng as _;

    #[test]
    fn roundtrip() {
        // Initialize the random number generator
        let mut rng = rand::thread_rng();

        for _ in 0..1000 {
            // Generate random bits for the upper 56 bits
            let upper_bits: u64 = rng.gen::<u64>() & 0xFF_FF_FF_FF_FF_FF_FF_00;
            // Generate a valid kind (0, 1 or 2)
            let kind: u8 = rng.gen_range(0..=2);
            let user_data = upper_bits | kind as u64;
            let uring_id = UringId::from_user_data(user_data);
            assert_eq!(uring_id.to_user_data(), user_data);
        }
    }
}
