use crate::{context::Ids, value_count::ValueCount};

pub struct Entry<T> {
    pub(crate) value: T,
    pub(crate) ids: Ids,
    pub(crate) value_count: ValueCount,
}

impl<T> Entry<T> {
    pub(crate) fn new(value: T, ids: Ids, value_count: ValueCount) -> Self {
        Self {
            value,
            ids,
            value_count,
        }
    }

    pub(crate) fn f_on(self, f: impl FnOnce(T, Ids, ValueCount)) {
        f(self.value, self.ids, self.value_count);
    }
}
