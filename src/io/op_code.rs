use std::fmt::Display;

use io_uring::opcode;
use num_enum::{IntoPrimitive, TryFromPrimitive};

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
    Nop = opcode::Nop::CODE,
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
            OpCode::Nop => "Nop",
        };
        f.write_str(str_repr)
    }
}
