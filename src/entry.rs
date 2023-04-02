use crate::value_count::ValueCount;

pub struct Entry<T> {
    pub(crate) value: T,
    pub(crate) value_count: ValueCount,
}

impl<T> Entry<T> {
    pub(crate) fn new(value: T, value_count: ValueCount) -> Self {
        Self { value, value_count }
    }
}
