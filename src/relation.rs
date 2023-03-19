use std::marker::PhantomData;

use crate::{op::Op, value_count::ValueCount};

pub struct Relation<T, C> {
    _phantom: PhantomData<T>,
    inner: C,
}

impl<T, C: Op<T>> Relation<T, C> {
    pub(crate) fn foreach(&mut self, f: impl FnMut(T, ValueCount)) {
        self.inner.foreach(f)
    }
}
