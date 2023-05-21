use crate::{context::Level, value_count::ValueCount};

pub struct Entry<T> {
    pub(crate) value: T,
    pub(crate) level: Level,
    pub(crate) value_count: ValueCount,
}

impl<T> Entry<T> {
    pub(crate) fn new(value: T, level: Level, value_count: ValueCount) -> Self {
        Self {
            value,
            level,
            value_count,
        }
    }
}
