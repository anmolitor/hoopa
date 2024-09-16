use std::fmt::Display;

use io_uring::{types, IoUring};

use crate::fd::Fd;

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

    pub fn read(&mut self, fd: &Fd) -> Result<(io_uring::squeue::Entry, StorageId), Error> {
        let buf_index = self.next()?;
        let read_entry = io_uring::opcode::ReadFixed::new(
            types::Fd::from(fd),
            self.storage[buf_index as usize].as_mut_ptr(),
            N as _,
            buf_index,
        )
        .build();
        Ok((read_entry, StorageId(buf_index)))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::CString,
        fs::File,
        io::Read,
        os::fd::{AsRawFd, FromRawFd},
        time::Instant,
    };

    use crate::uring_id::UringId;

    use super::*;

    #[test]
    fn read_test() -> anyhow::Result<()> {
        let user_data = 123456u32;
        let uring_id = UringId::from(user_data);
        let mut buf: FixedBuffers<1024> = FixedBuffers::new(10);
        let mut ring = io_uring::IoUring::new(4)?;
        let cargo_lock = CString::new("Cargo.lock").expect("CString");

        let time_start = Instant::now();
        let open_at = io_uring::opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), cargo_lock.as_ptr())
            .flags(libc::O_CLOEXEC | libc::O_RDONLY)
            .build()
            .user_data(uring_id.user_data());
        let uring_id = uring_id.increment();
        let mut statx: libc::statx = unsafe { std::mem::zeroed() };
        let statx_entry = io_uring::opcode::Statx::new(
            types::Fd(libc::AT_FDCWD),
            cargo_lock.as_ptr(),
            &mut statx as *mut libc::statx as *mut _,
        )
        .build()
        .user_data(uring_id.user_data());
        unsafe {
            ring.submission().push(&open_at)?;
            ring.submission().push(&statx_entry)?;
        };

        ring.submit_and_wait(2)?;
        let mut opened_file = ring.completion().next().expect("Number of read bytes");
        let mut stats = ring.completion().next().expect("Number of read bytes");
        println!("{opened_file:?} {stats:?}");
        if UringId(stats.user_data()).call_id() == 0 {
            std::mem::swap(&mut opened_file, &mut stats);
        }
        println!("{opened_file:?}");
        println!("{stats:?}, stats: {statx:?}",);
        let file_fd = unsafe { Fd::from_raw_fd(opened_file.result()) };
        let (entry, storage_id) = buf.read(&file_fd)?;
        unsafe { ring.submission().push(&entry) }?;

        ring.submit_and_wait(1)?;
        let read_bytes = ring.completion().next().expect("Number of read bytes");

        println!("{}", String::from_utf8_lossy(&buf[storage_id]));
        let time = Instant::now().duration_since(time_start);
        println!("Time: {time:?}");
        Ok(())
    }
}
