#[derive(Debug)]
pub enum OptZeroed<T> {
    Zeroed(T),
    Done(T),
}

impl<T> OptZeroed<T> {
    pub unsafe fn zeroed() -> Self {
        Self::Zeroed(std::mem::zeroed())
    }

    pub fn reset(&mut self) {
        std::mem::replace(self, Self::Zeroed(unsafe { std::mem::zeroed() }));
    }

    pub fn get_mut(&mut self) -> *mut T {
        match self {
            OptZeroed::Zeroed(t) => t,
            OptZeroed::Done(t) => t,
        }
    }

    pub fn get(&self) -> Option<&T> {
        match self {
            OptZeroed::Zeroed(_) => None,
            OptZeroed::Done(t) => Some(t),
        }
    }

    pub unsafe fn set_done(&mut self) -> &T {
        match self {
            OptZeroed::Zeroed(ref mut value) => {
                // Take the value out of the Zeroed variant
                let value_ptr: *mut T = value;

                // Replace the current variant with Done
                *self = OptZeroed::Done(std::ptr::read(value_ptr));

                if let OptZeroed::Done(ref mut value) = self {
                    value
                } else {
                    unreachable!()
                }
            }
            OptZeroed::Done(ref mut value) => value,
        }
    }
}
