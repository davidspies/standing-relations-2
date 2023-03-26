use std::hash::Hash;

use crate::{context::CommitId, e1map::E1Map, op::Op, relation::RelationInner, value_count::ValueCount};

pub struct Consolidate<T, C> {
    sub_rel: RelationInner<T, C>,
    collected_scratch: E1Map<T, ValueCount>,
}

impl<T, C> Consolidate<T, C> {
    pub(crate) fn new(sub_rel: RelationInner<T, C>) -> Self {
        Self {
            sub_rel,
            collected_scratch: E1Map::default(),
        }
    }
}

impl<T: Eq + Hash, C: Op<T>> Op<T> for Consolidate<T, C> {
    fn type_name(&self) -> &'static str {
        "consolidate"
    }
    fn foreach(&mut self, current_id: CommitId, mut f: impl FnMut(T, ValueCount)) {
        self.sub_rel.foreach(current_id, |value, count| {
            self.collected_scratch.add(value, count);
        });
        self.collected_scratch
            .drain()
            .for_each(|(value, count)| f(value, count));
    }
}
