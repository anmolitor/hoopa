use io_uring::squeue;

use super::op_code::OpCode;

pub struct CheckedEntry(squeue::Entry, OpCode);

impl CheckedEntry {
    pub fn new(entry: squeue::Entry, op_code: OpCode) -> Self {
        Self(entry, op_code)
    }

    pub fn into_tuple(self) -> (squeue::Entry, OpCode) {
        (self.0, self.1)
    }
}
