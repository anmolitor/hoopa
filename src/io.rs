use entry::CheckedEntry;
use io_uring::{
    opcode,
    squeue::{self, Flags},
    SubmissionQueue,
};

use crate::uring_id::{AcceptedUringId, HasOpCode, SetUserData};

mod entry;
mod fs;
mod net;
mod op_code;
mod pipe;

pub use fs::{open_at, statx, File};
pub use net::{Socket, TcpStream};
pub use op_code::OpCode;
pub use pipe::{pipe, PipeIn, PipeOut};

pub struct ScopedSubmissionQueue<'sq, 'ssq, T> {
    sq: &'ssq mut SubmissionQueue<'sq>,
    uring_id: T,
}

impl<'sq, 'ssq, T> ScopedSubmissionQueue<'sq, 'ssq, T> {
    pub fn map<F, U>(self, f: F) -> ScopedSubmissionQueue<'sq, 'ssq, U>
    where
        F: FnOnce(T) -> U,
    {
        ScopedSubmissionQueue {
            sq: self.sq,
            uring_id: f(self.uring_id),
        }
    }
}

impl<'sq, 'ssq> ScopedSubmissionQueue<'sq, 'ssq, AcceptedUringId> {
    pub fn cancel(&mut self, op_code: OpCode) {
        let uring_id = self.uring_id.to_cancel_id(op_code);
        let uring_id_to_cancel = self.uring_id.set_op_code(op_code);
        let entry = opcode::AsyncCancel::new(uring_id_to_cancel.to_user_data()).build();
        let entry = uring_id.set_user_data(entry);
        unsafe { self.sq.push(&entry) };
    }

    pub fn cancel_self(&mut self) {
        self.cancel(self.uring_id.op_code());
    }
}

impl<'sq, 'ssq, T> ScopedSubmissionQueue<'sq, 'ssq, T>
where
    T: HasOpCode + SetUserData + Copy,
{
    pub fn new(sq: &'ssq mut SubmissionQueue<'sq>, uring_id: T) -> Self
    where
        'sq: 'ssq,
    {
        Self { sq, uring_id }
    }

    pub fn uring_id(&self) -> T {
        self.uring_id
    }

    pub fn add(&mut self, checked_entry: CheckedEntry) {
        let entry = self.finalize_entry(checked_entry);
        unsafe { self.sq.push(&entry) };
    }

    pub fn add_parallel<const N: usize>(&mut self, entries: [CheckedEntry; N]) {
        for entry in entries {
            self.add(entry);
        }
    }

    pub fn add_sequential<const N: usize>(&mut self, entries: [CheckedEntry; N]) {
        for (index, checked_entry) in entries.into_iter().enumerate() {
            let mut entry = self.finalize_entry(checked_entry);
            if index < N - 1 {
                entry = entry.flags(Flags::IO_LINK);
            }
            unsafe { self.sq.push(&entry) };
        }
    }

    fn finalize_entry(&self, checked_entry: CheckedEntry) -> squeue::Entry {
        let (entry, op_code) = checked_entry.into_tuple();
        self.uring_id.set_op_code(op_code).set_user_data(entry)
    }
}
