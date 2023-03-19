use crate::{op::Op, relation::Relation, value_count::ValueCount};

pub struct FlatMap<S, F, C> {
    sub_rel: Relation<S, C>,
    f: F,
}

impl<S, F, C> FlatMap<S, F, C> {
    pub fn new(sub_rel: Relation<S, C>, f: F) -> Self {
        Self { sub_rel, f }
    }
}

impl<S, I, T, F, C> Op<T> for FlatMap<S, F, C>
where
    I: IntoIterator<Item = T>,
    F: FnMut(S) -> I,
    C: Op<S>,
{
    fn foreach(&mut self, mut f: impl FnMut(T, ValueCount)) {
        self.sub_rel.foreach(|x, count| {
            for y in (self.f)(x) {
                f(y, count)
            }
        })
    }
}
