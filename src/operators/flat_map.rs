use crate::{context::CommitId, op::Op, relation::RelationInner, value_count::ValueCount};

pub struct FlatMap<S, F, C> {
    sub_rel: RelationInner<S, C>,
    f: F,
}

impl<S, F, C> FlatMap<S, F, C> {
    pub(crate) fn new(sub_rel: RelationInner<S, C>, f: F) -> Self {
        Self { sub_rel, f }
    }
}

impl<S, I, T, F, C> Op<T> for FlatMap<S, F, C>
where
    I: IntoIterator<Item = T>,
    F: Fn(S) -> I,
    C: Op<S>,
{
    fn type_name(&self) -> &'static str {
        "flat_map"
    }
    fn foreach(&mut self, current_id: CommitId, mut f: impl FnMut(T, ValueCount)) {
        self.sub_rel.foreach(current_id, |x, count| {
            for y in (self.f)(x) {
                f(y, count)
            }
        })
    }
}
