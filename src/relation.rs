use std::marker::PhantomData;

use crate::{
    op::{DynOp, Op},
    value_count::ValueCount,
};

pub struct Relation<T, C> {
    _phantom: PhantomData<T>,
    inner: C,
}

impl<T, C: Op<T>> Relation<T, C> {
    pub(crate) fn foreach(&mut self, f: impl FnMut(T, ValueCount)) {
        self.inner.foreach(f)
    }
}

impl<'a, T, C: Op<T> + 'a> Relation<T, C> {
    pub fn dynamic(self) -> Relation<T, Box<dyn DynOp<T> + 'a>> {
        Relation {
            _phantom: self._phantom,
            inner: Box::new(self.inner),
        }
    }
}
