use crate::{
    context::{CommitId, Level},
    op::Op,
    relation::RelationInner,
    value_count::ValueCount,
};

pub struct Negate<T, C> {
    sub_rel: RelationInner<T, C>,
}

impl<T, C> Negate<T, C> {
    pub(crate) fn new(sub_rel: RelationInner<T, C>) -> Self {
        Self { sub_rel }
    }
}

impl<T, C: Op<T>> Op<T> for Negate<T, C> {
    fn type_name(&self) -> &'static str {
        "negate"
    }
    fn foreach<F: FnMut(T, Level, ValueCount)>(&mut self, current_id: CommitId, mut f: F) {
        self.sub_rel
            .foreach(current_id, |value, lvl, count| f(value, lvl, -count))
    }
}
