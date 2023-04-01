use crate::{context::CommitId, op::Op, relation::RelationInner, value_count::ValueCount};

pub struct FlatMap<S, G, C> {
    sub_rel: RelationInner<S, C>,
    g: G,
}

impl<S, G, C> FlatMap<S, G, C> {
    pub(crate) fn new(sub_rel: RelationInner<S, C>, g: G) -> Self {
        Self { sub_rel, g }
    }
}

impl<S, I, T, G, C> Op<T> for FlatMap<S, G, C>
where
    I: IntoIterator<Item = T>,
    G: Fn(S) -> I,
    C: Op<S>,
{
    fn type_name(&self) -> &'static str {
        "flat_map"
    }
    fn foreach(&mut self, current_id: CommitId, mut f: impl FnMut(T, ValueCount)) {
        self.sub_rel.foreach(current_id, |x, count| {
            for y in (self.g)(x) {
                f(y, count)
            }
        })
    }
}
