/// This is the ID we use to associate `completion`s with `submission`s.
/// It is a simple `u64`, but we use the bytes in the following way:
/// The upper 32 bits are used as the "main identifier" for a given http request.
/// The lower 32 bits are used to distinguish concurrent io_uring requests from each other.
///
/// This is one of the reasons why io_uring is good:
/// we can concurrently make Kernel calls. For example, for opening a file and getting metadata about the file,
/// we submit two entries to the submission queue. These two entries can then be completed in any order
/// by the Kernel and the results will be placed in the completion queue.
/// Since a completion entry just consists of i32 result (which depends on the call made)
/// and the same u64 we can set on the submission entry, the u64 is the only chance we get to
/// know which is which. In the example, the OpenAt entry will get the same upper 32 bits as the Statx entry,
/// but the latter will get a 1 for the lower 32 bits, while the former gets a 0.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct UringId(pub u64);

pub struct UringIdOutOfRange<T>(T);

impl UringId {
    pub fn increment(mut self) -> Self {
        self.0 += 1;
        self
    }

    pub fn user_data(self) -> u64 {
        self.0
    }

    pub fn call_id(self) -> u32 {
        self.0 as u32
    }

    pub fn user_id(self) -> u32 {
        (self.0 >> 32) as u32
    }
}

impl From<u32> for UringId {
    fn from(value: u32) -> Self {
        UringId((value as u64) << 32)
    }
}
