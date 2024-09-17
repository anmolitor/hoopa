use std::fmt::Display;

use io_uring::{opcode, types, IoUring};

use crate::{
    fd::Fd,
    uring_id::{Op as _, UringId},
};

pub struct FixedBuffers<const N: usize> {
    storage: Vec<[u8; N]>,
    next: u16,
    free_list: Vec<u16>,
}

impl<const N: usize> std::ops::Index<StorageId> for FixedBuffers<N> {
    type Output = [u8; N];

    fn index(&self, index: StorageId) -> &Self::Output {
        &self.storage[index.0 as usize]
    }
}

impl<const N: usize> std::ops::IndexMut<StorageId> for FixedBuffers<N> {
    fn index_mut(&mut self, index: StorageId) -> &mut Self::Output {
        &mut self.storage[index.0 as usize]
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    StorageFull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StorageId(u16);

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::StorageFull => f.write_str("Storage full!"),
        }
    }
}

impl std::error::Error for Error {}

impl<const N: usize> FixedBuffers<N> {
    pub fn new(num_buffers: u16) -> Self {
        if num_buffers == 0 {
            panic!("num_buffers should be at least one.");
        }
        let storage = vec![[0u8; N]; num_buffers.into()];

        Self {
            storage,
            next: num_buffers,
            free_list: Default::default(),
        }
    }

    pub fn register_to(&mut self, submitter: &io_uring::Submitter) -> std::io::Result<()> {
        let buffers: Box<[libc::iovec]> = self
            .storage
            .iter_mut()
            .map(|slice| libc::iovec {
                iov_base: slice.as_mut_ptr() as *mut libc::c_void,
                iov_len: N,
            })
            .collect();
        unsafe { submitter.register_buffers(&buffers) }
    }

    pub fn give_back(&mut self, id: StorageId) {
        self.free_list.push(id.0);
    }

    fn next(&mut self) -> Result<u16, Error> {
        match self.free_list.pop() {
            Some(free) => Ok(free),
            None => {
                if self.next == 0 {
                    return Err(Error::StorageFull);
                }
                self.next -= 1;
                Ok(self.next)
            }
        }
    }

    pub fn read(
        &mut self,
        fd: &Fd,
        uring_id: UringId,
    ) -> Result<(io_uring::squeue::Entry, StorageId), Error> {
        let buf_index = self.next()?;
        let storage_id = StorageId(buf_index);
        let read_entry = self.read_into(fd, storage_id, uring_id);
        Ok((read_entry, storage_id))
    }

    fn read_into(
        &mut self,
        fd: &Fd,
        storage_id: StorageId,
        uring_id: UringId,
    ) -> io_uring::squeue::Entry {
        io_uring::opcode::ReadFixed::new(
            types::Fd::from(fd),
            self[storage_id].as_mut_ptr(),
            N as _,
            storage_id.0,
        )
        .build_with_user_data(uring_id)
    }

    pub fn write(
        &self,
        connection_fd: &Fd,
        storage_id: StorageId,
        len: u32,
    ) -> io_uring::squeue::Entry {
        let buf = &self[storage_id];
        io_uring::opcode::Write::new(types::Fd::from(connection_fd), buf.as_ptr(), len).build()
    }
}
