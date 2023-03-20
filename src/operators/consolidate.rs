use std::hash::Hash;

use crate::{
    commit_id::CommitId, e1map::E1Map, op::Op, relation::Relation, value_count::ValueCount,
};

pub struct Consolidate<T, C> {
    sub_rel: Relation<T, C>,
    collected_scratch: E1Map<T, ValueCount>,
}

impl<T, C> Consolidate<T, C> {
    pub fn new(sub_rel: Relation<T, C>) -> Self {
        Self {
            sub_rel,
            collected_scratch: E1Map::default(),
        }
    }
}

impl<T: Eq + Hash, C: Op<T>> Op<T> for Consolidate<T, C> {
    fn foreach(&mut self, current_id: CommitId, mut f: impl FnMut(T, ValueCount)) {
        self.sub_rel.foreach(current_id, |value, count| {
            self.collected_scratch.add(value, count);
        });
        self.collected_scratch
            .drain()
            .for_each(|(value, count)| f(value, count));
    }
}
