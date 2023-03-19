use crate::{op::Op, value_count::ValueCount, Relation};

pub struct Negate<T, C> {
    sub_rel: Relation<T, C>,
}

impl<T, C> Negate<T, C> {
    pub(crate) fn new(sub_rel: Relation<T, C>) -> Self {
        Self { sub_rel }
    }
}

impl<T, C: Op<T>> Op<T> for Negate<T, C> {
    fn foreach(&mut self, mut f: impl FnMut(T, ValueCount)) {
        self.sub_rel.foreach(|value, count| f(value, -count))
    }
}
