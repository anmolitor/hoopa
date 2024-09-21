use std::fmt::Display;

use io_uring::{cqueue, opcode, squeue};
use num_enum::{IntoPrimitive, TryFromPrimitive};

/// This is the ID we use to associate `completion`s with `submission`s.
/// It is a simple `u64`, but we use the bytes in the following way:
///
/// < 16 bits > < 1 bit >  < 31 bits >  < 8 bits > < 8 bits >
///   ^^^^^^^     ^^^^^      ^^^^^^^      ^^^^^^     ^^^^^^
///   conn id    is http2   stream id     opcode     counter
///
/// This is one of the reasons why io_uring is good: we can concurrently make Kernel calls.
/// For example, for opening a file and getting metadata about the file,
/// we submit two entries to the submission queue. These two entries can then be completed in any order
/// by the Kernel and the results will be placed in the completion queue.
/// Since a completion entry just consists of i32 result (which depends on the call made)
/// and the same u64 we can set on the submission entry, the u64 is the only chance we get to
/// know which is which. In the example, the OpenAt entry will get the same conn id bits, http2 bit, stream id bits and counter as the Statx entry, but the opcode will be different since they are two different kernel operations.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct UringId(u64);

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

#[derive(Copy, Clone, Debug, Eq, PartialEq, TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
pub enum OpCode {
    AcceptMulti = opcode::AcceptMulti::CODE,
    RecvMulti = opcode::RecvMulti::CODE,
    Send = opcode::Send::CODE,
    AsyncCancel = opcode::AsyncCancel::CODE,
    OpenAt = opcode::OpenAt::CODE,
    Statx = opcode::Statx::CODE,
    Splice = opcode::Splice::CODE,
}

pub struct UringIdOutOfRange<T>(T);

impl UringId {
    pub fn from_cqe(entry: &cqueue::Entry) -> Self {
        UringId(entry.user_data())
    }

    pub fn cancel(self) -> squeue::Entry {
        opcode::AsyncCancel::new(self.0).build_with_user_data(self)
    }
    pub fn from_connection_id(connection_id: ConnectionId) -> Self {
        let id = u64::from(connection_id.0) << 48;
        UringId(id)
    }

    pub fn set_stream_id(mut self, stream_id: StreamId) -> Self {
        // Clear the current stream_id
        self.0 &= !(0x7FFFFFFF << 16);
        // Set the new stream_id
        self.0 |= (0x1 << 47) | ((u64::from(stream_id.0) & 0x7FFFFFFF) << 16);
        self
    }

    pub fn set_op_code(mut self, op_code: OpCode) -> Self {
        // Clear the current op_code
        self.0 &= !(0xFF << 8);
        // Set the new op_code
        self.0 |= (u64::from(op_code as u8) & 0xFF) << 8;
        self
    }

    pub fn increment_counter(mut self) -> Self {
        // Increment the last 8 bits (counter)
        self.0 += 1;
        self
    }

    pub fn split(&self) -> (ConnectionId, Option<StreamId>, OpCode) {
        (self.connection_id(), self.stream_id(), self.op_code())
    }

    pub fn connection_id(&self) -> ConnectionId {
        ConnectionId((self.0 >> 48) as u16)
    }

    pub fn is_http2(&self) -> bool {
        ((self.0 >> 47) & 0x1) != 0
    }

    pub fn stream_id(&self) -> Option<StreamId> {
        if self.is_http2() {
            Some(StreamId(((self.0 >> 16) & 0x7FFFFFFF) as u32))
        } else {
            None
        }
    }

    pub fn op_code(&self) -> OpCode {
        let op_code = ((self.0 >> 8) & 0xFF) as u8;
        OpCode::try_from_primitive(op_code).expect("Invalid op code")
    }

    pub fn counter(&self) -> u8 {
        (self.0 & 0xFF) as u8
    }
}

pub trait Op {
    fn code() -> OpCode;
    fn build(self) -> squeue::Entry;
    fn build_with_user_data(self, uring_id: UringId) -> squeue::Entry
    where
        Self: Sized,
    {
        self.build().user_data(uring_id.set_op_code(Self::code()).0)
    }
}

// impl Op for opcode::ReadFixed {
//     fn code() -> OpCode {
//         OpCode::ReadFixed
//     }

//     fn build(self) -> squeue::Entry {
//         self.build()
//     }
// }

impl Op for opcode::AcceptMulti {
    fn code() -> OpCode {
        OpCode::AcceptMulti
    }

    fn build(self) -> squeue::Entry {
        self.build()
    }
}

impl Op for opcode::Send {
    fn code() -> OpCode {
        OpCode::Send
    }

    fn build(self) -> squeue::Entry {
        self.build()
    }
}

impl Op for opcode::RecvMulti {
    fn code() -> OpCode {
        OpCode::RecvMulti
    }

    fn build(self) -> squeue::Entry {
        self.build()
    }
}

impl Op for opcode::AsyncCancel {
    fn code() -> OpCode {
        OpCode::AsyncCancel
    }

    fn build(self) -> squeue::Entry {
        self.build()
    }
}

impl Op for opcode::Splice {
    fn code() -> OpCode {
        OpCode::Splice
    }

    fn build(self) -> squeue::Entry {
        self.build()
    }
}

impl Op for opcode::Statx {
    fn code() -> OpCode {
        OpCode::Statx
    }

    fn build(self) -> squeue::Entry {
        self.build()
    }
}

impl Op for opcode::OpenAt {
    fn code() -> OpCode {
        OpCode::OpenAt
    }

    fn build(self) -> squeue::Entry {
        self.build()
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

impl Display for OpCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str_repr = match self {
            OpCode::AcceptMulti => "AcceptMulti",
            OpCode::AsyncCancel => "AsyncCancel",
            OpCode::RecvMulti => "RecvMulti",
            OpCode::Send => "Send",
            OpCode::OpenAt => "OpenAt",
            OpCode::Statx => "Statx",
            OpCode::Splice => "Splice",
        };
        f.write_str(str_repr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_connection_id() {
        let conn_id = ConnectionId(0x1234);
        let uring_id = UringId::from_connection_id(conn_id);
        assert_eq!(uring_id.connection_id(), conn_id);
    }

    #[test]
    fn test_set_stream_id() {
        let conn_id = ConnectionId(0x1234);
        let stream_id = StreamId(0x1ABCDE);
        let uring_id = UringId::from_connection_id(conn_id).set_stream_id(stream_id);
        assert!(uring_id.is_http2());
        assert_eq!(uring_id.stream_id(), Some(stream_id));
    }

    #[test]
    fn test_set_op_code() {
        let conn_id = ConnectionId(0x1234);
        let op_code = OpCode::RecvMulti;
        let uring_id = UringId::from_connection_id(conn_id).set_op_code(op_code);
        assert_eq!(uring_id.op_code(), op_code);
    }

    #[test]
    fn test_increment_counter() {
        let conn_id = ConnectionId(0x1234);
        let uring_id = UringId::from_connection_id(conn_id).increment_counter();
        assert_eq!(uring_id.counter(), 1);
    }

    #[test]
    fn test_combined_operations() {
        let conn_id = ConnectionId(0x1234);
        let stream_id = StreamId(0x1ABCDE);
        let op_code = OpCode::RecvMulti;

        let uring_id = UringId::from_connection_id(conn_id)
            .set_stream_id(stream_id)
            .set_op_code(op_code)
            .increment_counter()
            .increment_counter();

        assert_eq!(uring_id.connection_id(), conn_id);
        assert!(uring_id.is_http2());
        assert_eq!(uring_id.stream_id(), Some(stream_id));
        assert_eq!(uring_id.op_code(), op_code);
        assert_eq!(uring_id.counter(), 2);
    }
}
