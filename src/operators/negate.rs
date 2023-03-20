use crate::{context::CommitId, op::Op, relation::Relation, value_count::ValueCount};

pub struct Negate<T, C> {
    sub_rel: Relation<T, C>,
}

impl<T, C> Negate<T, C> {
    pub(crate) fn new(sub_rel: Relation<T, C>) -> Self {
        Self { sub_rel }
    }
}

impl<T, C: Op<T>> Op<T> for Negate<T, C> {
    fn foreach(&mut self, current_id: CommitId, mut f: impl FnMut(T, ValueCount)) {
        self.sub_rel
            .foreach(current_id, |value, count| f(value, -count))
    }
}
