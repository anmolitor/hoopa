use std::{fmt::Display, marker::PhantomData};

pub struct Slab<T> {
    storage: Vec<Option<T>>,
    next: u16,
    free_list: Vec<u16>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    StorageFull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SlabId<T>(u16, PhantomData<T>);

impl<T> From<u16> for SlabId<T> {
    fn from(value: u16) -> Self {
        Self(value, PhantomData)
    }
}

impl<T> From<SlabId<T>> for u16 {
    fn from(value: SlabId<T>) -> Self {
        value.0
    }
}

impl<T> From<SlabId<T>> for u32 {
    fn from(value: SlabId<T>) -> Self {
        value.0.into()
    }
}

impl<T> From<SlabId<T>> for u64 {
    fn from(value: SlabId<T>) -> Self {
        value.0.into()
    }
}

impl<T> From<SlabId<T>> for usize {
    fn from(value: SlabId<T>) -> Self {
        value.0.into()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::StorageFull => f.write_str("Storage full!"),
        }
    }
}

impl std::error::Error for Error {}

#[must_use]
pub struct Entry<'a, T> {
    key: SlabId<T>,
    slab: &'a mut Slab<T>,
}

impl<'a, T> Entry<'a, T> {
    pub fn remove(self) {
        self.slab.remove(self.key);
    }

    pub fn set(self, value: T) {
        self.slab.storage[self.key.0 as usize] = Some(value);
    }
}

impl<T> Slab<T> {
    pub fn new(capacity: u16) -> Self {
        if capacity == 0 {
            panic!("capacity should be at least one.");
        }
        let storage: Vec<Option<T>> = std::iter::repeat_with(|| None)
            .take(capacity as _)
            .collect();

        Self {
            storage,
            next: capacity,
            free_list: Default::default(),
        }
    }

    pub fn insert(&mut self, value: T) -> Result<SlabId<T>, Error> {
        let id = self.next()?;
        self.storage[id as usize] = Some(value);
        Ok(SlabId(id, PhantomData))
    }

    pub fn remove(&mut self, key: SlabId<T>) -> Option<T> {
        let value = self.storage.get_mut(key.0 as usize)?;
        self.free_list.push(key.0);
        value.take()
    }

    pub fn entry(&mut self, key: SlabId<T>) -> Option<(T, Entry<T>)> {
        let value = self.storage.get_mut(key.0 as usize)?;
        let value = value.take()?;
        Some((value, Entry { key, slab: self }))
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
