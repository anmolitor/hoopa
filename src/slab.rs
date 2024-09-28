use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
};

#[derive(Debug)]
pub struct Slab<K, T> {
    storage: Box<[T]>,
    next: u16,
    free_list: Vec<u16>,
    _key_type: PhantomData<K>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    StorageFull,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::StorageFull => f.write_str("Storage full!"),
        }
    }
}

impl std::error::Error for Error {}

pub trait Reset {
    fn reset(&mut self);
}

impl<K, T> Slab<K, T>
where
    T: Default + Reset,
    K: From<u16> + Into<u16> + Copy,
{
    pub fn new(capacity: u16) -> Self {
        if capacity == 0 {
            panic!("capacity should be at least one.");
        }
        let storage: Box<[T]> = std::iter::repeat_with(|| Default::default())
            .take(capacity as _)
            .collect();

        Self {
            storage,
            next: capacity,
            free_list: Default::default(),
            _key_type: PhantomData,
        }
    }

    pub fn reserve(&mut self) -> Result<(K, &mut T), Error> {
        let id = self.next()?;
        let value = &mut self.storage[id as usize];
        let key = K::from(id);
        Ok((key, value))
    }

    pub fn get_mut(&mut self, key: K) -> &mut T {
        let k = Into::<u16>::into(key);
        &mut self.storage[k as usize]
    }

    pub fn reset(&mut self, key: K) {
        let k = Into::<u16>::into(key);
        self.free_list.push(k);
        self.storage[k as usize].reset();
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
}
