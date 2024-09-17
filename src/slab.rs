use std::{fmt::Display, marker::PhantomData};

#[derive(Debug)]
pub struct Slab<K, T> {
    storage: Vec<Option<T>>,
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

#[must_use]
pub struct Entry<'a, K, T>
where
    K: Into<u16> + Copy,
{
    key: K,
    slab: &'a mut Slab<K, T>,
    set: bool,
}

impl<'a, K, T> Entry<'a, K, T>
where
    K: Into<u16> + Copy,
{
    pub fn set(mut self, value: T) {
        self.slab.storage[Into::<u16>::into(self.key) as usize] = Some(value);
        self.set = true;
    }
}

impl<'a, K, T> Drop for Entry<'a, K, T>
where
    K: Into<u16> + Copy,
{
    fn drop(&mut self) {
        if self.set {
            return;
        }
        self.slab.remove(self.key);
    }
}

impl<K, T> Slab<K, T> {
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
            _key_type: PhantomData,
        }
    }

    pub fn insert(&mut self, value: T) -> Result<K, Error>
    where
        K: From<u16>,
    {
        let id = self.next()?;
        self.storage[id as usize] = Some(value);
        Ok(K::from(id))
    }

    pub fn reserve(&mut self) -> Result<(K, Entry<K, T>), Error>
    where
        K: From<u16> + Into<u16> + Copy,
    {
        let id = self.next()?;

        let key = K::from(id);
        Ok((
            key,
            Entry {
                key,
                slab: self,
                set: false,
            },
        ))
    }

    pub fn remove(&mut self, key: K) -> Option<T>
    where
        K: Into<u16> + Copy,
    {
        let key = Into::<u16>::into(key);
        let value = self.storage.get_mut(key as usize)?;
        self.free_list.push(key);
        value.take()
    }

    pub fn entry(&mut self, key: K) -> Option<(T, Entry<K, T>)>
    where
        K: Into<u16> + Copy,
    {
        let k = Into::<u16>::into(key);
        let value = self.storage.get_mut(k as usize)?;
        let value = value.take()?;
        Some((
            value,
            Entry {
                key,
                slab: self,
                set: false,
            },
        ))
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
