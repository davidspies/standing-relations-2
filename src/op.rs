use crate::value_count::ValueCount;

pub trait Op<T> {
    fn foreach(&mut self, f: impl FnMut(T, ValueCount));
}
