use core::fmt;
use std::{
    cell::UnsafeCell,
    io::{self, Write},
};

#[derive(Debug)]
pub struct Bump<const N: usize>(UnsafeCell<Inner<N>>);

pub struct Inner<const N: usize> {
    array: [u8; N],
    offset: usize,
}

impl<const N: usize> Write for Inner<N> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let alloc = self.alloc(buf.len());
        alloc.copy_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<const N: usize> Bump<N> {
    pub fn new() -> Self {
        Bump(UnsafeCell::new(Inner {
            array: [0; N],
            offset: 0,
        }))
    }

    pub fn write_fmt_to_ref(&self, args: fmt::Arguments) -> io::Result<&[u8]> {
        let inner = self.inner();
        let offset = inner.offset;
        inner.write_fmt(args)?;
        Ok(&inner.array[offset..inner.offset])
    }

    pub fn alloc(&self, size: usize) -> &mut [u8] {
        let inner = self.inner();
        let new_offset = inner.offset + size;
        // TODO handle out of bounds
        let slice = &mut inner.array[inner.offset..new_offset];
        inner.offset = new_offset;
        slice
    }

    pub fn reset(&mut self) {
        let inner = self.0.get_mut();
        inner.offset = 0;
    }

    pub fn inner<'a>(&self) -> &'a mut Inner<N> {
        unsafe { self.0.get().as_mut() }.expect("Bump null pointer")
    }
}

impl<const N: usize> Inner<N> {
    pub fn alloc(&mut self, size: usize) -> &mut [u8] {
        let new_offset = self.offset + size;
        // TODO handle out of bounds
        let slice = &mut self.array[self.offset..new_offset];
        self.offset = new_offset;
        slice
    }
}
